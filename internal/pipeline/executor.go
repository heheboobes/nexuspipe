package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"nexuspipe/internal/models"
)

var (
	executorStageDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "nexuspipe",
		Subsystem: "executor",
		Name:      "stage_duration_seconds",
		Help:      "Duration of stage execution.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"stage_type"})

	executorStageRetries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "nexuspipe",
		Subsystem: "executor",
		Name:      "stage_retries_total",
		Help:      "Total number of stage retries.",
	}, []string{"stage_type"})
)

type Stage struct {
	ID        uuid.UUID
	Name      string
	Type      models.TaskType
	Config    models.TaskConfig
	Sequence  int
	Optional  bool
	DependsOn []string
	Timeout   time.Duration
	MaxRetry  int
}

type StageHandler func(ctx context.Context, stage Stage, input interface{}) (interface{}, error)

type PipelineExecutor struct {
	logger   *zap.Logger
	handlers map[models.TaskType]StageHandler
	mu       sync.RWMutex
	timeout  time.Duration
}

func NewPipelineExecutor(logger *zap.Logger) *PipelineExecutor {
	e := &PipelineExecutor{
		logger:   logger.With(zap.String("component", "pipeline_executor")),
		handlers: make(map[models.TaskType]StageHandler),
		timeout:  300 * time.Second,
	}
	e.registerDefaultHandlers()
	return e
}

func (e *PipelineExecutor) registerDefaultHandlers() {
	e.RegisterHandler(models.TaskTypeHTTP, e.executeHTTPStage)
	e.RegisterHandler(models.TaskTypeScript, e.executeScriptStage)
	e.RegisterHandler(models.TaskTypeSQL, e.executeSQLStage)
	e.RegisterHandler(models.TaskTypeShell, e.executeShellStage)
	e.RegisterHandler(models.TaskTypeTransform, e.executeTransformStage)
	e.RegisterHandler(models.TaskTypeWebhook, e.executeWebhookStage)
	e.RegisterHandler(models.TaskTypeNotification, e.executeNotificationStage)
	e.RegisterHandler(models.TaskTypeCustom, e.executeCustomStage)
}

func (e *PipelineExecutor) RegisterHandler(t models.TaskType, handler StageHandler) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.handlers[t] = handler
}

func (e *PipelineExecutor) ExecuteStage(ctx context.Context, pipeline *models.Pipeline, stage Stage, input interface{}) StageResult {
	start := time.Now()
	e.logger.Info("executing stage",
		zap.String("pipeline", pipeline.Name),
		zap.String("stage", stage.Name),
		zap.String("type", string(stage.Type)),
		zap.Int("sequence", stage.Sequence),
	)

	stageTimeout := e.resolveTimeout(stage)
	ctx, cancel := context.WithTimeout(ctx, stageTimeout)
	defer cancel()

	handler, err := e.getHandler(stage.Type)
	if err != nil {
		return StageResult{
			StageName: stage.Name,
			Status:    models.TaskStatusFailed,
			Error:     err.Error(),
			Duration:  time.Since(start),
		}
	}

	var lastErr error
	maxRetries := e.resolveMaxRetries(stage)

	for attempt := 0; attempt <= maxRetries; attempt++ {
		select {
		case <-ctx.Done():
			result := StageResult{
				StageName: stage.Name,
				Status:    models.TaskStatusTimedOut,
				Error:     fmt.Sprintf("stage timed out after %v", stageTimeout),
				Duration:  time.Since(start),
			}
			if ctx.Err() == context.Canceled {
				result.Status = models.TaskStatusCancelled
				result.Error = "stage cancelled"
			}
			return result
		default:
		}

		if attempt > 0 {
			backoff := e.calculateBackoff(attempt)
			e.logger.Info("retrying stage",
				zap.String("stage", stage.Name),
				zap.Int("attempt", attempt+1),
				zap.Duration("backoff", backoff),
			)
			executorStageRetries.WithLabelValues(string(stage.Type)).Inc()

			select {
			case <-ctx.Done():
				return StageResult{
					StageName: stage.Name,
					Status:    models.TaskStatusCancelled,
					Error:     "cancelled during retry backoff",
					Duration:  time.Since(start),
				}
			case <-time.After(backoff):
			}
		}

		output, err := handler(ctx, stage, input)
		if err == nil {
			executorStageDuration.WithLabelValues(string(stage.Type)).Observe(time.Since(start).Seconds())
			return StageResult{
				StageName: stage.Name,
				Status:    models.TaskStatusCompleted,
				Output:    output,
				Duration:  time.Since(start),
			}
		}

		lastErr = err
		e.logger.Warn("stage execution attempt failed",
			zap.String("stage", stage.Name),
			zap.Int("attempt", attempt+1),
			zap.Error(err),
		)
	}

	executorStageDuration.WithLabelValues(string(stage.Type)).Observe(time.Since(start).Seconds())
	return StageResult{
		StageName: stage.Name,
		Status:    models.TaskStatusFailed,
		Error:     fmt.Sprintf("stage failed after %d retries: %v", maxRetries, lastErr),
		Duration:  time.Since(start),
	}
}

func (e *PipelineExecutor) ExecutePipelineStages(ctx context.Context, pipeline *models.Pipeline, stages []Stage, input interface{}) []StageResult {
	results := make([]StageResult, 0, len(stages))
	for _, stage := range stages {
		result := e.ExecuteStage(ctx, pipeline, stage, input)
		results = append(results, result)
		if result.Status == models.TaskStatusFailed && !stage.Optional {
			break
		}
	}
	return results
}

func (e *PipelineExecutor) getHandler(t models.TaskType) (StageHandler, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	handler, ok := e.handlers[t]
	if !ok {
		return nil, fmt.Errorf("no handler registered for stage type: %s", t)
	}
	return handler, nil
}

func (e *PipelineExecutor) resolveTimeout(stage Stage) time.Duration {
	if stage.Timeout > 0 {
		return stage.Timeout
	}
	return e.timeout
}

func (e *PipelineExecutor) resolveMaxRetries(stage Stage) int {
	if stage.MaxRetry > 0 {
		return stage.MaxRetry
	}
	return 2
}

func (e *PipelineExecutor) calculateBackoff(attempt int) time.Duration {
	backoff := time.Duration(attempt*attempt) * 500 * time.Millisecond
	if backoff > 30*time.Second {
		backoff = 30 * time.Second
	}
	return backoff
}

func (e *PipelineExecutor) executeHTTPStage(ctx context.Context, stage Stage, input interface{}) (interface{}, error) {
	return nil, fmt.Errorf("http stage not implemented")
}

func (e *PipelineExecutor) executeScriptStage(ctx context.Context, stage Stage, input interface{}) (interface{}, error) {
	return nil, fmt.Errorf("script stage not implemented")
}

func (e *PipelineExecutor) executeSQLStage(ctx context.Context, stage Stage, input interface{}) (interface{}, error) {
	return nil, fmt.Errorf("sql stage not implemented")
}

func (e *PipelineExecutor) executeShellStage(ctx context.Context, stage Stage, input interface{}) (interface{}, error) {
	return nil, fmt.Errorf("shell stage not implemented")
}

func (e *PipelineExecutor) executeTransformStage(ctx context.Context, stage Stage, input interface{}) (interface{}, error) {
	return nil, fmt.Errorf("transform stage not implemented")
}

func (e *PipelineExecutor) executeWebhookStage(ctx context.Context, stage Stage, input interface{}) (interface{}, error) {
	return nil, fmt.Errorf("webhook stage not implemented")
}

func (e *PipelineExecutor) executeNotificationStage(ctx context.Context, stage Stage, input interface{}) (interface{}, error) {
	return nil, fmt.Errorf("notification stage not implemented")
}

func (e *PipelineExecutor) executeCustomStage(ctx context.Context, stage Stage, input interface{}) (interface{}, error) {
	return nil, fmt.Errorf("custom stage not implemented")
}

func GetStagesFromPipeline(pipeline *models.Pipeline) []Stage {
	return []Stage{}
}
