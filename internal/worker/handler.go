package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"

	"github.com/heheboobes/nexuspipe/internal/models"
	"github.com/heheboobes/nexuspipe/internal/queue"
	"github.com/heheboobes/nexuspipe/internal/repository"
)

type HandlerConfig struct {
	TaskTimeout     time.Duration
	MaxRetries      int
	DLQName         string
	RetryExchange   string
	RetryRoutingKey string
	AckOnSuccess    bool
	NackOnFailure   bool
	RequeueOnNack   bool
}

func DefaultHandlerConfig() HandlerConfig {
	return HandlerConfig{
		TaskTimeout:     60 * time.Second,
		MaxRetries:      3,
		DLQName:         "tasks.dlq",
		RetryExchange:   "tasks.retry",
		RetryRoutingKey: "task.retry",
		AckOnSuccess:    true,
		NackOnFailure:   true,
		RequeueOnNack:   false,
	}
}

type MessageHandler struct {
	cfg       HandlerConfig
	processor TaskProcessor
	registry  *HandlerRegistry
	repo      *repository.TaskRepository
	rmq       *queue.RabbitMQ
	pool      *WorkerPool
	logger    *zap.Logger
}

func NewMessageHandler(
	cfg HandlerConfig,
	processor TaskProcessor,
	registry *HandlerRegistry,
	repo *repository.TaskRepository,
	rmq *queue.RabbitMQ,
	pool *WorkerPool,
	logger *zap.Logger,
) *MessageHandler {
	return &MessageHandler{
		cfg:       cfg,
		processor: processor,
		registry:  registry,
		repo:      repo,
		rmq:       rmq,
		pool:      pool,
		logger:    logger.With(zap.String("component", "message_handler")),
	}
}

func (h *MessageHandler) HandleDelivery(ctx context.Context, msg amqp091.Delivery) error {
	ctx, cancel := context.WithTimeout(ctx, h.cfg.TaskTimeout)
	defer cancel()

	env, info, err := queue.EnvelopeFromDelivery(&msg)
	if err != nil {
		h.logger.Error("failed to deserialize delivery",
			zap.Error(err),
			zap.Uint64("delivery_tag", msg.DeliveryTag),
		)
		return h.handleDeserializeError(ctx, msg, err)
	}

	h.logger.Debug("received delivery",
		zap.String("message_id", env.ID),
		zap.String("message_type", string(env.Type)),
		zap.String("routing_key", info.RoutingKey),
		zap.Uint64("delivery_tag", msg.DeliveryTag),
	)

	var task models.Task
	if err := json.Unmarshal(env.Payload, &task); err != nil {
		h.logger.Error("failed to unmarshal task payload",
			zap.String("message_id", env.ID),
			zap.Error(err),
		)
		return h.handleDeserializeError(ctx, msg, err)
	}

	select {
	case <-ctx.Done():
		h.logger.Warn("context cancelled before processing",
			zap.String("task_id", task.ID.String()),
			zap.String("message_id", env.ID),
		)
		return h.handleTimeout(ctx, msg, env)
	default:
	}

	result, err := h.processor.ProcessTask(ctx, &task)
	if err != nil {
		h.logger.Error("processor error",
			zap.String("task_id", task.ID.String()),
			zap.Error(err),
		)
		return h.handleProcessingError(ctx, msg, env, &task, result, err)
	}

	if result == nil {
		result = &TaskResult{
			TaskID:    task.ID.String(),
			Status:    models.TaskStatusFailed,
			Error:     "nil result from processor",
			ErrorType: ErrorTypeInternal,
		}
	}

	switch result.Status {
	case models.TaskStatusCompleted:
		return h.handleSuccess(ctx, msg, env, &task, result)
	case models.TaskStatusFailed:
		return h.handleProcessingError(ctx, msg, env, &task, result, nil)
	default:
		return h.handleSuccess(ctx, msg, env, &task, result)
	}
}

func (h *MessageHandler) handleSuccess(ctx context.Context, msg amqp091.Delivery, env *queue.Envelope, task *models.Task, result *TaskResult) error {
	if err := h.repo.UpdateStatus(ctx, task.ID.String(), models.TaskStatusCompleted, ""); err != nil {
		h.logger.Error("failed to update task status to completed",
			zap.String("task_id", task.ID.String()),
			zap.Error(err),
		)
	}

	if h.cfg.AckOnSuccess {
		if err := msg.Ack(false); err != nil {
			return fmt.Errorf("ack failed: %w", err)
		}
	}

	h.logger.Info("task completed successfully",
		zap.String("task_id", task.ID.String()),
		zap.Duration("duration", result.Duration),
	)

	return nil
}

func (h *MessageHandler) handleProcessingError(ctx context.Context, msg amqp091.Delivery, env *queue.Envelope, task *models.Task, result *TaskResult, procErr error) error {
	errMsg := ""
	if result != nil {
		errMsg = result.Error
	}
	if errMsg == "" && procErr != nil {
		errMsg = procErr.Error()
	}

	retryable := false
	if result != nil {
		retryable = result.Retryable
	}

	retryCount := env.RetryCount

	if retryable && retryCount < h.cfg.MaxRetries {
		return h.handleRetry(ctx, msg, env, task, errMsg, retryCount)
	}

	if err := h.repo.UpdateStatus(ctx, task.ID.String(), models.TaskStatusFailed, errMsg); err != nil {
		h.logger.Error("failed to update task status to failed",
			zap.String("task_id", task.ID.String()),
			zap.Error(err),
		)
	}

	return h.routeToDLQ(ctx, msg, env, errMsg)
}

func (h *MessageHandler) handleRetry(ctx context.Context, msg amqp091.Delivery, env *queue.Envelope, task *models.Task, errMsg string, retryCount int) error {
	env.RetryCount = retryCount + 1

	retryDelay := time.Duration(1<<uint(retryCount)) * time.Second
	if retryDelay > 30*time.Second {
		retryDelay = 30 * time.Second
	}

	env.Headers.Set("x-retry-count", retryCount+1)
	env.Headers.Set("x-retry-delay", retryDelay.String())
	env.Headers.Set("x-original-error", errMsg)
	env.Headers.Set("x-next-retry", time.Now().UTC().Add(retryDelay).Format(time.RFC3339))

	h.logger.Info("scheduling retry",
		zap.String("task_id", task.ID.String()),
		zap.Int("retry_count", retryCount+1),
		zap.Int("max_retries", h.cfg.MaxRetries),
		zap.Duration("retry_delay", retryDelay),
	)

	if err := h.repo.IncrementRetry(ctx, task.ID.String(), errMsg); err != nil {
		h.logger.Error("failed to increment retry in repository",
			zap.String("task_id", task.ID.String()),
			zap.Error(err),
		)
	}

	if h.cfg.NackOnFailure {
		if err := msg.Nack(false, h.cfg.RequeueOnNack); err != nil {
			return fmt.Errorf("nack for retry failed: %w", err)
		}
	}

	return nil
}

func (h *MessageHandler) routeToDLQ(ctx context.Context, msg amqp091.Delivery, env *queue.Envelope, errMsg string) error {
	h.logger.Warn("routing message to DLQ",
		zap.String("message_id", env.ID),
		zap.String("dlq", h.cfg.DLQName),
		zap.String("error", errMsg),
	)

	payload, err := env.Marshal()
	if err != nil {
		h.logger.Error("failed to marshal envelope for DLQ",
			zap.String("message_id", env.ID),
			zap.Error(err),
		)
		if h.cfg.NackOnFailure {
			return msg.Nack(false, false)
		}
		return fmt.Errorf("marshal for dlq: %w", err)
	}

	ch, err := h.rmq.AcquireChannel()
	if err != nil {
		h.logger.Error("failed to acquire channel for DLQ publish",
			zap.Error(err),
		)
		if h.cfg.NackOnFailure {
			return msg.Nack(false, false)
		}
		return fmt.Errorf("acquire channel for dlq: %w", err)
	}
	defer h.rmq.ReleaseChannel(ch)

	publishing := amqp091.Publishing{
		ContentType:  "application/json",
		Body:         payload,
		DeliveryMode: amqp091.Persistent,
		Timestamp:    time.Now().UTC(),
		Headers: amqp091.Table{
			"x-original-routing-key": msg.RoutingKey,
			"x-error":                errMsg,
			"x-failed-at":            time.Now().UTC().Format(time.RFC3339),
		},
	}

	if err := ch.PublishWithContext(ctx, "", h.cfg.DLQName, false, false, publishing); err != nil {
		h.logger.Error("failed to publish to DLQ",
			zap.String("dlq", h.cfg.DLQName),
			zap.Error(err),
		)
	}

	if h.cfg.NackOnFailure {
		if err := msg.Nack(false, false); err != nil {
			return fmt.Errorf("nack after dlq routing failed: %w", err)
		}
	}

	return nil
}

func (h *MessageHandler) handleDeserializeError(ctx context.Context, msg amqp091.Delivery, err error) error {
	h.logger.Error("deserialization error, routing to DLQ",
		zap.Uint64("delivery_tag", msg.DeliveryTag),
		zap.Error(err),
	)

	ch, chErr := h.rmq.AcquireChannel()
	if chErr == nil {
		defer h.rmq.ReleaseChannel(ch)
		_ = ch.PublishWithContext(ctx, "", h.cfg.DLQName, false, false, amqp091.Publishing{
			ContentType:  "application/octet-stream",
			Body:         msg.Body,
			DeliveryMode: amqp091.Persistent,
			Timestamp:    time.Now().UTC(),
			Headers: amqp091.Table{
				"x-error":                fmt.Sprintf("deserialize_error: %v", err),
				"x-failed-at":            time.Now().UTC().Format(time.RFC3339),
				"x-original-routing-key": msg.RoutingKey,
			},
		})
	}

	return msg.Nack(false, false)
}

func (h *MessageHandler) handleTimeout(ctx context.Context, msg amqp091.Delivery, env *queue.Envelope) error {
	h.logger.Warn("task timed out",
		zap.String("message_id", env.ID),
		zap.Duration("timeout", h.cfg.TaskTimeout),
	)

	ch, err := h.rmq.AcquireChannel()
	if err == nil {
		defer h.rmq.ReleaseChannel(ch)
		payload, _ := env.Marshal()
		_ = ch.PublishWithContext(ctx, "", h.cfg.DLQName, false, false, amqp091.Publishing{
			ContentType:  "application/json",
			Body:         payload,
			DeliveryMode: amqp091.Persistent,
			Timestamp:    time.Now().UTC(),
			Headers: amqp091.Table{
				"x-error":     "task_timeout",
				"x-timeout":   h.cfg.TaskTimeout.String(),
				"x-failed-at": time.Now().UTC().Format(time.RFC3339),
			},
		})
	}

	return msg.Nack(false, false)
}

var _ = uuid.New
