package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

type NotificationEvent struct {
	ID        uuid.UUID         `json:"id"`
	EventType string            `json:"event_type"`
	Source    string            `json:"source"`
	Payload   json.RawMessage   `json:"payload"`
	Timestamp time.Time         `json:"timestamp"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

type NotificationTask struct {
	ID         uuid.UUID         `json:"id"`
	Event      NotificationEvent `json:"event"`
	Webhooks   []*Webhook        `json:"webhooks"`
	CreatedAt  time.Time         `json:"created_at"`
	Retries    int               `json:"retries"`
	MaxRetries int               `json:"max_retries"`
}

type DeadLetterEntry struct {
	ID         uuid.UUID        `json:"id"`
	Task       NotificationTask `json:"task"`
	Reason     string           `json:"reason"`
	FailedAt   time.Time        `json:"failed_at"`
	RetryCount int              `json:"retry_count"`
}

type NotifierConfig struct {
	QueueSize       int           `json:"queue_size"`
	WorkerCount     int           `json:"worker_count"`
	BatchSize       int           `json:"batch_size"`
	BatchInterval   time.Duration `json:"batch_interval"`
	DeadLetterLimit int           `json:"dead_letter_limit"`
}

func DefaultNotifierConfig() NotifierConfig {
	return NotifierConfig{
		QueueSize:       1024,
		WorkerCount:     4,
		BatchSize:       10,
		BatchInterval:   500 * time.Millisecond,
		DeadLetterLimit: 1000,
	}
}

type EventNotifier struct {
	mu         sync.RWMutex
	config     NotifierConfig
	manager    *WebhookManager
	deliverer  *Deliverer
	history    *WebhookHistory
	logger     *zap.Logger
	taskCh     chan NotificationTask
	deadLetter []DeadLetterEntry
	stopCh     chan struct{}
	wg         sync.WaitGroup
	started    bool
	metrics    *NotifierMetrics
}

type NotifierMetrics struct {
	NotificationsSent     int64
	NotificationsFailed   int64
	NotificationsDropped  int64
	DeadLetterCount       int64
	BatchCount            int64
	AvgProcessingDuration time.Duration
	mu                    sync.Mutex
}

func NewEventNotifier(config NotifierConfig, manager *WebhookManager, deliverer *Deliverer, history *WebhookHistory, logger *zap.Logger) *EventNotifier {
	return &EventNotifier{
		config:     config,
		manager:    manager,
		deliverer:  deliverer,
		history:    history,
		logger:     logger.With(zap.String("component", "event_notifier")),
		taskCh:     make(chan NotificationTask, config.QueueSize),
		deadLetter: make([]DeadLetterEntry, 0, config.DeadLetterLimit),
		stopCh:     make(chan struct{}),
		metrics:    &NotifierMetrics{},
	}
}

func (n *EventNotifier) Start() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.started {
		return
	}

	n.started = true

	for i := 0; i < n.config.WorkerCount; i++ {
		n.wg.Add(1)
		go n.worker(i)
	}

	go n.batchProcessor()

	n.logger.Info("event notifier started",
		zap.Int("workers", n.config.WorkerCount),
		zap.Int("queue_size", n.config.QueueSize),
		zap.Int("batch_size", n.config.BatchSize),
	)
}

func (n *EventNotifier) Stop() {
	n.mu.Lock()
	if !n.started {
		n.mu.Unlock()
		return
	}
	n.started = false
	n.mu.Unlock()

	close(n.stopCh)
	n.wg.Wait()

	n.logger.Info("event notifier stopped",
		zap.Int64("sent", n.metrics.NotificationsSent),
		zap.Int64("failed", n.metrics.NotificationsFailed),
		zap.Int64("dead_letter", n.metrics.DeadLetterCount),
	)
}

func (n *EventNotifier) Notify(ctx context.Context, event NotificationEvent) error {
	webhooks := n.manager.MatchWebhooks(event.EventType)
	if len(webhooks) == 0 {
		return nil
	}

	task := NotificationTask{
		ID:         uuid.New(),
		Event:      event,
		Webhooks:   webhooks,
		CreatedAt:  time.Now().UTC(),
		MaxRetries: 3,
	}

	select {
	case n.taskCh <- task:
		n.metrics.mu.Lock()
		n.metrics.NotificationsSent++
		n.metrics.mu.Unlock()
		return nil
	default:
		n.metrics.mu.Lock()
		n.metrics.NotificationsDropped++
		n.metrics.mu.Unlock()
		n.logger.Warn("notification channel full, dropping event",
			zap.String("event_type", event.EventType),
			zap.String("event_id", event.ID.String()),
			zap.Int("webhooks", len(webhooks)),
		)
		return fmt.Errorf("notification queue full, dropping event %s", event.ID)
	}
}

func (n *EventNotifier) NotifyBatch(ctx context.Context, events []NotificationEvent) []error {
	errs := make([]error, 0, len(events))
	for _, event := range events {
		if err := n.Notify(ctx, event); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

func (n *EventNotifier) NotifyAsync(event NotificationEvent) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := n.Notify(ctx, event); err != nil {
			n.logger.Error("async notification failed",
				zap.String("event_type", event.EventType),
				zap.Error(err),
			)
		}
	}()
}

func (n *EventNotifier) worker(id int) {
	defer n.wg.Done()

	n.logger.Debug("worker started", zap.Int("worker_id", id))

	for {
		select {
		case <-n.stopCh:
			n.logger.Debug("worker stopped", zap.Int("worker_id", id))
			return
		case task := <-n.taskCh:
			n.processTask(task)
		}
	}
}

func (n *EventNotifier) processTask(task NotificationTask) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	for _, wh := range task.Webhooks {
		deliveryID := uuid.New()
		now := time.Now().UTC()

		delivery := &WebhookDelivery{
			ID:          deliveryID,
			WebhookID:   wh.ID,
			EventType:   task.Event.EventType,
			URL:         wh.URL,
			Status:      DeliveryStatusDelivering,
			RequestBody: task.Event.Payload,
			Attempt:     1,
			MaxRetries:  wh.RetryConfig.MaxRetries,
			CreatedAt:   now,
			UpdatedAt:   now,
		}

		n.history.RecordDelivery(delivery, wh.Name, wh.URL)

		result, err := n.deliverer.Deliver(ctx, wh.ID, wh.URL, task.Event.EventType, wh.Secret, wh.Headers, task.Event.Payload)

		if err != nil {
			n.metrics.mu.Lock()
			n.metrics.NotificationsFailed++
			n.metrics.mu.Unlock()

			status := DeliveryStatusFailed
			if result != nil && result.Attempt < wh.RetryConfig.MaxRetries {
				status = DeliveryStatusRetrying
			}

			errMsg := ""
			if err != nil {
				errMsg = err.Error()
			}

			durationMS := int64(0)
			if result != nil {
				durationMS = result.Duration.Milliseconds()
			}

			n.history.UpdateDeliveryStatus(deliveryID, status, result.StatusCode, result.ResponseBody, errMsg)
			n.history.UpdateDeliveryAttempt(deliveryID, result.Attempt, durationMS)

			n.logger.Warn("webhook delivery failed",
				zap.String("delivery_id", deliveryID.String()),
				zap.String("webhook_id", wh.ID.String()),
				zap.String("url", wh.URL),
				zap.String("event_type", task.Event.EventType),
				zap.Int("attempt", result.Attempt),
				zap.Error(err),
			)

			if task.Retries < task.MaxRetries {
				task.Retries++
				n.retryTask(task)
			} else {
				n.moveToDeadLetter(task, fmt.Sprintf("max retries exceeded: %v", err))
			}
		} else {
			durationMS := result.Duration.Milliseconds()
			n.history.UpdateDeliveryStatus(deliveryID, DeliveryStatusSuccess, result.StatusCode, result.ResponseBody, "")
			n.history.UpdateDeliveryAttempt(deliveryID, result.Attempt, durationMS)
		}
	}
}

func (n *EventNotifier) retryTask(task NotificationTask) {
	backoff := time.Duration(1<<task.Retries) * time.Second
	if backoff > 30*time.Second {
		backoff = 30 * time.Second
	}

	time.AfterFunc(backoff, func() {
		select {
		case n.taskCh <- task:
		default:
			n.logger.Warn("retry queue full, moving to dead letter",
				zap.String("task_id", task.ID.String()),
				zap.Int("retries", task.Retries),
			)
			n.moveToDeadLetter(task, "retry queue full")
		}
	})
}

func (n *EventNotifier) batchProcessor() {
	ticker := time.NewTicker(n.config.BatchInterval)
	defer ticker.Stop()

	var batch []NotificationTask

	for {
		select {
		case <-n.stopCh:
			if len(batch) > 0 {
				n.flushBatch(batch)
			}
			return
		case task := <-n.taskCh:
			batch = append(batch, task)
			if len(batch) >= n.config.BatchSize {
				n.flushBatch(batch)
				batch = nil
			}
		case <-ticker.C:
			if len(batch) > 0 {
				n.flushBatch(batch)
				batch = nil
			}
		}
	}
}

func (n *EventNotifier) flushBatch(batch []NotificationTask) {
	n.metrics.mu.Lock()
	n.metrics.BatchCount++
	n.metrics.mu.Unlock()

	n.logger.Debug("flushing batch",
		zap.Int("batch_size", len(batch)),
	)

	for _, task := range batch {
		n.processTask(task)
	}
}

func (n *EventNotifier) moveToDeadLetter(task NotificationTask, reason string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	entry := DeadLetterEntry{
		ID:         uuid.New(),
		Task:       task,
		Reason:     reason,
		FailedAt:   time.Now().UTC(),
		RetryCount: task.Retries,
	}

	if len(n.deadLetter) >= n.config.DeadLetterLimit {
		n.deadLetter = n.deadLetter[1:]
	}

	n.deadLetter = append(n.deadLetter, entry)

	n.metrics.mu.Lock()
	n.metrics.DeadLetterCount++
	n.metrics.mu.Unlock()

	n.logger.Warn("notification moved to dead letter",
		zap.String("task_id", task.ID.String()),
		zap.String("reason", reason),
		zap.Int("retries", task.Retries),
	)
}

func (n *EventNotifier) GetDeadLetterEntries() []DeadLetterEntry {
	n.mu.RLock()
	defer n.mu.RUnlock()

	entries := make([]DeadLetterEntry, len(n.deadLetter))
	copy(entries, n.deadLetter)
	return entries
}

func (n *EventNotifier) ReplayDeadLetter(entryID uuid.UUID) error {
	n.mu.Lock()

	var entry DeadLetterEntry
	index := -1
	for i, e := range n.deadLetter {
		if e.ID == entryID {
			entry = e
			index = i
			break
		}
	}

	if index == -1 {
		n.mu.Unlock()
		return fmt.Errorf("dead letter entry not found: %s", entryID)
	}

	n.deadLetter = append(n.deadLetter[:index], n.deadLetter[index+1:]...)
	n.mu.Unlock()

	entry.Task.Retries = 0
	select {
	case n.taskCh <- entry.Task:
		n.logger.Info("dead letter entry replayed",
			zap.String("entry_id", entryID.String()),
			zap.String("task_id", entry.Task.ID.String()),
		)
		return nil
	default:
		n.moveToDeadLetter(entry.Task, "replay queue full")
		return fmt.Errorf("notification queue full during replay")
	}
}

func (n *EventNotifier) ReplayAllDeadLetter() (int, error) {
	n.mu.Lock()
	entries := make([]DeadLetterEntry, len(n.deadLetter))
	copy(entries, n.deadLetter)
	n.deadLetter = n.deadLetter[:0]
	n.mu.Unlock()

	replayed := 0
	for _, entry := range entries {
		entry.Task.Retries = 0
		select {
		case n.taskCh <- entry.Task:
			replayed++
		default:
			n.moveToDeadLetter(entry.Task, "replay queue full")
		}
	}

	return replayed, nil
}

func (n *EventNotifier) ClearDeadLetter() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	count := len(n.deadLetter)
	n.deadLetter = make([]DeadLetterEntry, 0, n.config.DeadLetterLimit)
	return count
}

func (n *EventNotifier) QueueDepth() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.deadLetter)
}

func (n *EventNotifier) GetMetrics() NotifierMetrics {
	n.metrics.mu.Lock()
	defer n.metrics.mu.Unlock()
	return *n.metrics
}

func (n *EventNotifier) IsRunning() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.started
}
