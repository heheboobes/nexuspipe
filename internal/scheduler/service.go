package scheduler

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"go.uber.org/zap"

	"nexuspipe/internal/logger"
	"nexuspipe/internal/models"
	"nexuspipe/internal/queue"
	"nexuspipe/internal/repository"
)

type SchedulerService struct {
	cron         *cron.Cron
	store        ScheduleStore
	publisher    *queue.Publisher
	recovery     *RecoveryEngine
	repo         *repository.ScheduleRepository
	activeJobs   map[string]cron.EntryID
	mu           sync.RWMutex
	ctx          context.Context
	cancel       context.CancelFunc
	pollTicker   *time.Ticker
	pollInterval time.Duration
	pollDone     chan struct{}
	logger       *zap.Logger
	pollEnabled  bool
	pollLimit    int
	startedAt    time.Time
}

type SchedulerOption func(*SchedulerService)

func WithPollingInterval(d time.Duration) SchedulerOption {
	return func(s *SchedulerService) {
		s.pollTicker = time.NewTicker(d)
		s.pollInterval = d
	}
}

func WithPollLimit(limit int) SchedulerOption {
	return func(s *SchedulerService) {
		if limit > 0 {
			s.pollLimit = limit
		}
	}
}

func NewSchedulerService(
	repo *repository.ScheduleRepository,
	publisher *queue.Publisher,
	recoveryCfg RecoveryConfig,
	opts ...SchedulerOption,
) *SchedulerService {
	ctx, cancel := context.WithCancel(context.Background())

	store := NewPostgresScheduleStore(repo)

	svc := &SchedulerService{
		cron:         cron.New(cron.WithSeconds(), cron.WithLogger(cron.PrintfLogger(log.Default()))),
		store:        store,
		publisher:    publisher,
		recovery:     NewRecoveryEngine(store, recoveryCfg),
		repo:         repo,
		activeJobs:   make(map[string]cron.EntryID),
		ctx:          ctx,
		cancel:       cancel,
		pollTicker:   time.NewTicker(15 * time.Second),
		pollInterval: 15 * time.Second,
		pollDone:     make(chan struct{}),
		logger:       logger.GetLogger(),
		pollLimit:    50,
	}

	for _, opt := range opts {
		opt(svc)
	}

	return svc
}

func (s *SchedulerService) Start() error {
	s.startedAt = time.Now()
	s.logger.Info("starting scheduler service")

	if err := s.recoverOnStartup(); err != nil {
		s.logger.Warn("recovery completed with errors", zap.Error(err))
	}

	if err := s.loadActiveJobs(); err != nil {
		return fmt.Errorf("load active jobs: %w", err)
	}

	s.cron.Start()
	s.logger.Info("cron engine started")

	go s.pollLoop()

	s.logger.Info("scheduler service started",
		zap.Int("active_jobs", len(s.activeJobs)),
		zap.Time("started_at", s.startedAt),
	)

	return nil
}

func (s *SchedulerService) Stop() error {
	s.logger.Info("stopping scheduler service")

	s.cancel()

	if s.pollTicker != nil {
		s.pollTicker.Stop()
	}
	<-s.pollDone

	ctx := s.cron.Stop()
	select {
	case <-ctx.Done():
		s.logger.Info("cron engine stopped")
	case <-time.After(10 * time.Second):
		s.logger.Warn("cron engine stop timed out")
	}

	s.mu.Lock()
	s.activeJobs = make(map[string]cron.EntryID)
	s.mu.Unlock()

	store := s.store.(*PostgresScheduleStore)
	store.ClearCache()

	s.logger.Info("scheduler service stopped",
		zap.Duration("uptime", time.Since(s.startedAt)),
	)
	return nil
}

func (s *SchedulerService) AddJob(schedule *models.Schedule) (cron.EntryID, error) {
	job, err := NewScheduledJob(schedule)
	if err != nil {
		return 0, fmt.Errorf("create scheduled job: %w", err)
	}

	entryID, err := s.cron.AddFunc(schedule.CronExpression, func() {
		s.executeJob(job)
	})
	if err != nil {
		return 0, fmt.Errorf("add cron func: %w", err)
	}

	s.mu.Lock()
	s.activeJobs[schedule.ID] = entryID
	s.mu.Unlock()

	s.logger.Info("job added to scheduler",
		zap.String("schedule_id", schedule.ID),
		zap.String("pipeline_id", schedule.PipelineID),
		zap.String("cron", schedule.CronExpression),
		zap.Int("entry_id", int(entryID)),
	)

	return entryID, nil
}

func (s *SchedulerService) RemoveJob(scheduleID string) error {
	s.mu.RLock()
	entryID, exists := s.activeJobs[scheduleID]
	s.mu.RUnlock()

	if !exists {
		return fmt.Errorf("schedule %s not found in active jobs", scheduleID)
	}

	s.cron.Remove(entryID)

	s.mu.Lock()
	delete(s.activeJobs, scheduleID)
	s.mu.Unlock()

	if store, ok := s.store.(*PostgresScheduleStore); ok {
		store.InvalidateCache(scheduleID)
	}

	s.logger.Info("job removed from scheduler",
		zap.String("schedule_id", scheduleID),
	)
	return nil
}

func (s *SchedulerService) UpdateJob(schedule *models.Schedule) error {
	if err := s.RemoveJob(schedule.ID); err != nil {
		s.logger.Warn("remove job during update failed", zap.String("schedule_id", schedule.ID), zap.Error(err))
	}
	_, err := s.AddJob(schedule)
	return err
}

func (s *SchedulerService) HasJob(scheduleID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, exists := s.activeJobs[scheduleID]
	return exists
}

func (s *SchedulerService) ActiveJobCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.activeJobs)
}

func (s *SchedulerService) executeJob(job *ScheduledJob) {
	if job.Disabled {
		s.logger.Debug("job is disabled, skipping execution",
			zap.String("schedule_id", job.ScheduleID),
		)
		return
	}

	exec := NewJobExecution(job)

	s.logger.Info("dispatching scheduled job",
		zap.String("execution_id", exec.ID),
		zap.String("schedule_id", exec.ScheduleID),
		zap.String("pipeline_id", exec.PipelineID),
		zap.String("job_type", string(exec.JobType)),
		zap.Time("scheduled_at", exec.ScheduledAt),
	)

	payload, err := exec.MarshalPayload()
	if err != nil {
		s.logger.Error("failed to marshal job execution", zap.Error(err))
		return
	}

	routingKey := exec.RoutingKey()
	if err := s.publisher.PublishWithRetry(s.ctx, routingKey, payload, 2); err != nil {
		s.logger.Error("failed to dispatch job to queue",
			zap.String("routing_key", routingKey),
			zap.Error(err),
		)
		return
	}

	nextRun, err := NextRunTime(job.CronExpression, nil)
	if err == nil {
		if err := s.store.UpdateNextRunTime(s.ctx, job.ScheduleID, nextRun); err != nil {
			s.logger.Warn("failed to update next run time",
				zap.String("schedule_id", job.ScheduleID),
				zap.Error(err),
			)
		}
	}

	s.logger.Info("job dispatched successfully",
		zap.String("execution_id", exec.ID),
		zap.String("routing_key", routingKey),
	)
}

func (s *SchedulerService) pollLoop() {
	defer close(s.pollDone)
	s.logger.Info("poll loop started", zap.Duration("interval", s.pollInterval))

	for {
		select {
		case <-s.ctx.Done():
			s.logger.Info("poll loop stopped")
			return
		case <-s.pollTicker.C:
			s.pollDueSchedules()
		}
	}
}

func (s *SchedulerService) pollDueSchedules() {
	due, err := s.store.GetDueSchedules(s.ctx, s.pollLimit)
	if err != nil {
		s.logger.Error("poll due schedules failed", zap.Error(err))
		return
	}

	for _, sch := range due {
		if s.HasJob(sch.ID) {
			continue
		}
		job, err := NewScheduledJob(sch)
		if err != nil {
			s.logger.Warn("create job from due schedule failed",
				zap.String("schedule_id", sch.ID),
				zap.Error(err),
			)
			continue
		}
		go s.executeJob(job)
	}
}

func (s *SchedulerService) recoverOnStartup() error {
	s.logger.Info("running startup recovery")

	results, err := s.recovery.RecoverMissedJobs(s.ctx)
	if err != nil {
		return fmt.Errorf("recover missed jobs: %w", err)
	}

	totalMissed := 0
	totalExecuted := 0
	totalSkipped := 0
	for _, res := range results {
		totalMissed += res.MissedRuns
		totalExecuted += res.ExecutedRuns
		totalSkipped += res.SkippedRuns
	}

	s.logger.Info("startup recovery complete",
		zap.Int("schedules_checked", len(results)),
		zap.Int("total_missed", totalMissed),
		zap.Int("total_executed", totalExecuted),
		zap.Int("total_skipped", totalSkipped),
	)

	return nil
}

func (s *SchedulerService) loadActiveJobs() error {
	schedules, err := s.store.GetActiveSchedules(s.ctx)
	if err != nil {
		return fmt.Errorf("get active schedules: %w", err)
	}

	loaded := 0
	errors := 0
	for _, sch := range schedules {
		if _, err := s.AddJob(sch); err != nil {
			s.logger.Error("failed to load schedule job",
				zap.String("schedule_id", sch.ID),
				zap.Error(err),
			)
			errors++
			continue
		}
		loaded++
	}

	s.logger.Info("active jobs loaded",
		zap.Int("loaded", loaded),
		zap.Int("failed", errors),
		zap.Int("total", len(schedules)),
	)

	return nil
}

func (s *SchedulerService) ReloadJobs() error {
	s.mu.Lock()
	for id, entryID := range s.activeJobs {
		s.cron.Remove(entryID)
		delete(s.activeJobs, id)
	}
	s.mu.Unlock()

	return s.loadActiveJobs()
}

func (s *SchedulerService) GetStore() ScheduleStore {
	return s.store
}

func (s *SchedulerService) GetRecoveryEngine() *RecoveryEngine {
	return s.recovery
}

func (s *SchedulerService) Uptime() time.Duration {
	return time.Since(s.startedAt)
}

func (s *SchedulerService) Stats() map[string]any {
	return map[string]any{
		"active_jobs":   s.ActiveJobCount(),
		"started_at":    s.startedAt.Format(time.RFC3339),
		"uptime":        s.Uptime().String(),
		"poll_limit":    s.pollLimit,
		"poll_interval": s.pollInterval.String(),
	}
}
