package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"

	"nexuspipe/internal/models"
	"nexuspipe/internal/queue"
	"nexuspipe/internal/repository"
)

var (
	pipelineExecutionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "nexuspipe",
		Subsystem: "pipeline",
		Name:      "executions_total",
		Help:      "Total number of pipeline executions.",
	}, []string{"pipeline_id", "status"})

	pipelineExecutionDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "nexuspipe",
		Subsystem: "pipeline",
		Name:      "execution_duration_seconds",
		Help:      "Duration of pipeline executions.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"pipeline_id"})

	pipelineStageDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "nexuspipe",
		Subsystem: "pipeline",
		Name:      "stage_duration_seconds",
		Help:      "Duration of individual pipeline stages.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"pipeline_id", "stage_name"})

	pipelineErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "nexuspipe",
		Subsystem: "pipeline",
		Name:      "errors_total",
		Help:      "Total number of pipeline execution errors.",
	}, []string{"pipeline_id", "stage_name", "error_type"})
)

type StageResult struct {
	StageName string
	Status    models.TaskStatus
	Output    interface{}
	Error     string
	Duration  time.Duration
}

type PipelineRunResult struct {
	RunID         uuid.UUID
	PipelineID    uuid.UUID
	Status        models.PipelineStatus
	StageResults  []StageResult
	TotalDuration time.Duration
	Error         string
}

type PipelineEngine struct {
	repo      *repository.PipelineRepository
	publisher *queue.Publisher
	logger    *zap.Logger
	executor  *PipelineExecutor
	eventBus  *EventBus
	active    map[string]context.CancelFunc
	mu        sync.RWMutex
}

func NewPipelineEngine(
	repo *repository.PipelineRepository,
	publisher *queue.Publisher,
	logger *zap.Logger,
	executor *PipelineExecutor,
	eventBus *EventBus,
) *PipelineEngine {
	return &PipelineEngine{
		repo:      repo,
		publisher: publisher,
		logger:    logger.With(zap.String("component", "pipeline_engine")),
		executor:  executor,
		eventBus:  eventBus,
		active:    make(map[string]context.CancelFunc),
	}
}

func (e *PipelineEngine) ExecutePipeline(ctx context.Context, pipeline *models.Pipeline, input interface{}) (*PipelineRunResult, error) {
	runID := uuid.New()
	start := time.Now()

	e.logger.Info("executing pipeline",
		zap.String("pipeline_id", pipeline.ID.String()),
		zap.String("pipeline_name", pipeline.Name),
		zap.String("run_id", runID.String()),
	)

	pipelineExecutionsTotal.WithLabelValues(pipeline.ID.String(), "running").Inc()
	e.eventBus.Publish(Event{Type: EventPipelineStarted, PipelineID: pipeline.ID, RunID: runID})

	stages, err := e.resolveStages(pipeline)
	if err != nil {
		e.handleFailure(runID, pipeline.ID, start, "resolve_stages", err)
		return nil, fmt.Errorf("resolve stages: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	e.mu.Lock()
	e.active[runID.String()] = cancel
	e.mu.Unlock()

	defer func() {
		e.mu.Lock()
		delete(e.active, runID.String())
		e.mu.Unlock()
	}()

	stageResults := make([]StageResult, 0, len(stages))
	var finalErr error

	for _, stage := range stages {
		select {
		case <-ctx.Done():
			pipelineExecutionsTotal.WithLabelValues(pipeline.ID.String(), "cancelled").Inc()
			e.eventBus.Publish(Event{Type: EventPipelineFailed, PipelineID: pipeline.ID, RunID: runID, Error: "cancelled"})
			return &PipelineRunResult{
				RunID: runID, PipelineID: pipeline.ID, Status: models.PipelineStatusFailed,
				StageResults: stageResults, TotalDuration: time.Since(start), Error: "cancelled",
			}, ctx.Err()
		default:
		}

		result := e.executor.ExecuteStage(ctx, pipeline, stage, input)
		stageResults = append(stageResults, result)

		pipelineStageDuration.WithLabelValues(pipeline.ID.String(), stage.Name).Observe(result.Duration.Seconds())

		if result.Status == models.TaskStatusFailed || result.Status == models.TaskStatusTimedOut {
			pipelineErrorsTotal.WithLabelValues(pipeline.ID.String(), stage.Name, "execution_error").Inc()
			finalErr = fmt.Errorf("stage %s failed: %s", stage.Name, result.Error)
			e.logger.Error("stage failed", zap.String("stage", stage.Name), zap.String("error", result.Error))

			if !stage.Optional {
				break
			}
		}
	}

	duration := time.Since(start)
	result := &PipelineRunResult{
		RunID:         runID,
		PipelineID:    pipeline.ID,
		StageResults:  stageResults,
		TotalDuration: duration,
	}

	if finalErr != nil {
		result.Status = models.PipelineStatusFailed
		result.Error = finalErr.Error()
		pipelineExecutionsTotal.WithLabelValues(pipeline.ID.String(), "failed").Inc()
		e.eventBus.Publish(Event{Type: EventPipelineFailed, PipelineID: pipeline.ID, RunID: runID, Error: finalErr.Error()})
	} else {
		result.Status = models.PipelineStatusActive
		pipelineExecutionsTotal.WithLabelValues(pipeline.ID.String(), "completed").Inc()
		e.eventBus.Publish(Event{Type: EventPipelineCompleted, PipelineID: pipeline.ID, RunID: runID})
	}

	pipelineExecutionDuration.WithLabelValues(pipeline.ID.String()).Observe(duration.Seconds())

	e.logger.Info("pipeline execution finished",
		zap.String("run_id", runID.String()),
		zap.String("status", string(result.Status)),
		zap.Duration("duration", duration),
	)

	return result, finalErr
}

func (e *PipelineEngine) Run(ctx context.Context) error {
	e.logger.Info("pipeline engine starting")
	<-ctx.Done()
	e.logger.Info("pipeline engine shutting down")
	e.shutdown()
	return ctx.Err()
}

func (e *PipelineEngine) CancelRun(runID string) {
	e.mu.RLock()
	cancel, ok := e.active[runID]
	e.mu.RUnlock()
	if ok {
		cancel()
		e.logger.Info("cancelled pipeline run", zap.String("run_id", runID))
	}
}

func (e *PipelineEngine) shutdown() {
	e.mu.Lock()
	defer e.mu.Unlock()
	for id, cancel := range e.active {
		cancel()
		delete(e.active, id)
	}
	e.logger.Info("all active pipeline runs cancelled")
}

func (e *PipelineEngine) handleFailure(runID uuid.UUID, pipelineID uuid.UUID, start time.Time, stage string, err error) {
	e.logger.Error("pipeline execution failed",
		zap.String("run_id", runID.String()),
		zap.String("pipeline_id", pipelineID.String()),
		zap.String("stage", stage),
		zap.Error(err),
	)
	pipelineExecutionsTotal.WithLabelValues(pipelineID.String(), "failed").Inc()
	pipelineErrorsTotal.WithLabelValues(pipelineID.String(), stage, "fatal").Inc()
	e.eventBus.Publish(Event{
		Type:       EventPipelineFailed,
		PipelineID: pipelineID,
		RunID:      runID,
		Error:      err.Error(),
	})
}

func (e *PipelineEngine) resolveStages(pipeline *models.Pipeline) ([]Stage, error) {
	if pipeline == nil {
		return nil, fmt.Errorf("pipeline is nil")
	}
	return GetStagesFromPipeline(pipeline), nil
}

func (e *PipelineEngine) ActiveRuns() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	ids := make([]string, 0, len(e.active))
	for id := range e.active {
		ids = append(ids, id)
	}
	return ids
}

func (e *PipelineEngine) IsRunning(runID string) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	_, ok := e.active[runID]
	return ok
}
