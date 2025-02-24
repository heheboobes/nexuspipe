package dag

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
	"go.uber.org/zap"
)

type DAGCondition func(ctx context.Context, graph *Graph) (bool, error)

type DAGRun struct {
	ID         uuid.UUID
	Graph      *Graph
	StartedAt  time.Time
	FinishedAt *time.Time
	Result     *ExecutionResult
	Error      error
	Status     string
}

type DAGSchedule struct {
	ID        uuid.UUID
	Name      string
	Graph     *Graph
	CronExpr  string
	Condition DAGCondition
	Timeout   time.Duration
	Enabled   bool
	LastRun   *DAGRun
	NextRun   time.Time
	EntryID   cron.EntryID
}

type DAGRunEvent int

const (
	RunStarted DAGRunEvent = iota
	RunCompleted
	RunFailed
	RunSkipped
	RunTimedOut
)

type DAGEventListener func(event DAGRunEvent, schedule *DAGSchedule, run *DAGRun)

type DAGScheduler struct {
	logger     *zap.Logger
	executor   *DAGExecutor
	validator  *Validator
	cron       *cron.Cron
	schedules  map[uuid.UUID]*DAGSchedule
	mu         sync.RWMutex
	activeRuns map[uuid.UUID]context.CancelFunc
	listeners  []DAGEventListener
}

func NewDAGScheduler(logger *zap.Logger, executor *DAGExecutor) *DAGScheduler {
	c := cron.New(cron.WithSeconds())

	s := &DAGScheduler{
		logger:     logger.With(zap.String("component", "dag_scheduler")),
		executor:   executor,
		validator:  NewValidator(),
		cron:       c,
		schedules:  make(map[uuid.UUID]*DAGSchedule),
		activeRuns: make(map[uuid.UUID]context.CancelFunc),
		listeners:  make([]DAGEventListener, 0),
	}

	return s
}

func (s *DAGScheduler) Start() {
	s.cron.Start()
	s.logger.Info("DAG scheduler started")
}

func (s *DAGScheduler) Stop() {
	ctx := s.cron.Stop()
	<-ctx.Done()

	s.mu.Lock()
	for id, cancel := range s.activeRuns {
		cancel()
		delete(s.activeRuns, id)
	}
	s.mu.Unlock()

	s.logger.Info("DAG scheduler stopped")
}

func (s *DAGScheduler) AddSchedule(name, cronExpr string, graph *Graph) (*DAGSchedule, error) {
	result := s.validator.ValidateDAG(graph)
	if !result.Valid {
		return nil, fmt.Errorf("invalid DAG for schedule %q: %w", name, result)
	}

	schedule := &DAGSchedule{
		ID:       uuid.New(),
		Name:     name,
		Graph:    graph,
		CronExpr: cronExpr,
		Timeout:  30 * time.Minute,
		Enabled:  true,
	}

	entryID, err := s.cron.AddFunc(cronExpr, func() {
		s.executeSchedule(schedule)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to register cron expression %q: %w", cronExpr, err)
	}
	schedule.EntryID = entryID

	s.mu.Lock()
	s.schedules[schedule.ID] = schedule
	s.mu.Unlock()

	s.logger.Info("DAG schedule added",
		zap.String("name", name),
		zap.String("cron", cronExpr),
	)

	return schedule, nil
}

func (s *DAGScheduler) RemoveSchedule(id uuid.UUID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	schedule, ok := s.schedules[id]
	if !ok {
		return false
	}

	s.cron.Remove(schedule.EntryID)
	delete(s.schedules, id)

	s.logger.Info("DAG schedule removed",
		zap.String("name", schedule.Name),
	)
	return true
}

func (s *DAGScheduler) SetCondition(id uuid.UUID, condition DAGCondition) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	schedule, ok := s.schedules[id]
	if !ok {
		return fmt.Errorf("schedule %s not found", id)
	}
	schedule.Condition = condition
	return nil
}

func (s *DAGScheduler) SetTimeout(id uuid.UUID, timeout time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	schedule, ok := s.schedules[id]
	if !ok {
		return fmt.Errorf("schedule %s not found", id)
	}
	schedule.Timeout = timeout
	return nil
}

func (s *DAGScheduler) EnableSchedule(id uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	schedule, ok := s.schedules[id]
	if !ok {
		return fmt.Errorf("schedule %s not found", id)
	}
	schedule.Enabled = true
	return nil
}

func (s *DAGScheduler) DisableSchedule(id uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	schedule, ok := s.schedules[id]
	if !ok {
		return fmt.Errorf("schedule %s not found", id)
	}
	schedule.Enabled = false
	return nil
}

func (s *DAGScheduler) AddListener(listener DAGEventListener) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listeners = append(s.listeners, listener)
}

func (s *DAGScheduler) RunNow(id uuid.UUID) (*DAGRun, error) {
	s.mu.RLock()
	schedule, ok := s.schedules[id]
	s.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("schedule %s not found", id)
	}

	return s.executeSchedule(schedule), nil
}

func (s *DAGScheduler) GetSchedule(id uuid.UUID) *DAGSchedule {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.schedules[id]
}

func (s *DAGScheduler) ListSchedules() []*DAGSchedule {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*DAGSchedule, 0, len(s.schedules))
	for _, schedule := range s.schedules {
		result = append(result, schedule)
	}
	return result
}

func (s *DAGScheduler) executeSchedule(schedule *DAGSchedule) *DAGRun {
	s.mu.RLock()
	if !schedule.Enabled {
		s.mu.RUnlock()
		s.logger.Info("schedule is disabled, skipping", zap.String("name", schedule.Name))
		return nil
	}

	schedule.NextRun = s.cron.Entries()[schedule.EntryID].Next
	s.mu.RUnlock()

	ctx := context.Background()

	if schedule.Condition != nil {
		shouldRun, err := schedule.Condition(ctx, schedule.Graph)
		if err != nil {
			s.logger.Error("condition check failed",
				zap.String("schedule", schedule.Name),
				zap.Error(err),
			)
			run := &DAGRun{
				ID:        uuid.New(),
				Graph:     schedule.Graph,
				StartedAt: time.Now(),
				Status:    "skipped",
				Error:     fmt.Errorf("condition check failed: %w", err),
			}
			s.fireEvent(RunSkipped, schedule, run)
			return run
		}
		if !shouldRun {
			s.logger.Info("condition not met, skipping DAG run",
				zap.String("schedule", schedule.Name),
			)
			run := &DAGRun{
				ID:        uuid.New(),
				Graph:     schedule.Graph,
				StartedAt: time.Now(),
				Status:    "skipped",
			}
			s.fireEvent(RunSkipped, schedule, run)
			return run
		}
	}

	runCtx, cancel := context.WithTimeout(ctx, schedule.Timeout)

	s.mu.Lock()
	s.activeRuns[schedule.ID] = cancel
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.activeRuns, schedule.ID)
		s.mu.Unlock()
	}()

	run := &DAGRun{
		ID:        uuid.New(),
		Graph:     schedule.Graph,
		StartedAt: time.Now(),
		Status:    "running",
	}

	s.fireEvent(RunStarted, schedule, run)

	result, err := s.executor.Execute(runCtx, schedule.Graph)

	now := time.Now()
	run.FinishedAt = &now

	if err != nil {
		run.Status = "failed"
		run.Error = err
		s.logger.Error("DAG execution failed",
			zap.String("schedule", schedule.Name),
			zap.Error(err),
		)
		s.fireEvent(RunFailed, schedule, run)
		return run
	}

	if runCtx.Err() != nil {
		run.Status = "timed_out"
		run.Error = runCtx.Err()
		run.Result = result
		now := time.Now()
		run.FinishedAt = &now
		s.logger.Warn("DAG execution timed out",
			zap.String("schedule", schedule.Name),
			zap.Duration("timeout", schedule.Timeout),
		)
		s.fireEvent(RunTimedOut, schedule, run)
		return run
	}

	run.Result = result
	if result.Success {
		run.Status = "completed"
		s.fireEvent(RunCompleted, schedule, run)
	} else {
		run.Status = "failed"
		run.Error = fmt.Errorf("one or more nodes failed")
		s.fireEvent(RunFailed, schedule, run)
	}

	s.mu.Lock()
	schedule.LastRun = run
	schedule.NextRun = s.cron.Entries()[schedule.EntryID].Next
	s.mu.Unlock()

	s.logger.Info("DAG run completed",
		zap.String("schedule", schedule.Name),
		zap.String("status", run.Status),
		zap.Duration("duration", run.FinishedAt.Sub(run.StartedAt)),
	)

	return run
}

func (s *DAGScheduler) fireEvent(event DAGRunEvent, schedule *DAGSchedule, run *DAGRun) {
	s.mu.RLock()
	listeners := make([]DAGEventListener, len(s.listeners))
	copy(listeners, s.listeners)
	s.mu.RUnlock()

	for _, listener := range listeners {
		listener(event, schedule, run)
	}
}
