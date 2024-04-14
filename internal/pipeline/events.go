package pipeline

import (
	"sync"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

type EventType string

const (
	EventPipelineStarted   EventType = "pipeline.started"
	EventPipelineCompleted EventType = "pipeline.completed"
	EventPipelineFailed    EventType = "pipeline.failed"
	EventPipelineRetrying  EventType = "pipeline.retrying"
	EventPipelineCancelled EventType = "pipeline.cancelled"
	EventPipelinePaused    EventType = "pipeline.paused"

	EventStageStarted   EventType = "stage.started"
	EventStageCompleted EventType = "stage.completed"
	EventStageFailed    EventType = "stage.failed"
	EventStageRetrying  EventType = "stage.retrying"
	EventStageSkipped   EventType = "stage.skipped"

	EventRunCreated  EventType = "run.created"
	EventRunFinished EventType = "run.finished"
)

type Event struct {
	Type       EventType
	PipelineID uuid.UUID
	RunID      uuid.UUID
	StageName  string
	Error      string
	Metadata   map[string]interface{}
}

type EventHandler func(Event)

type EventBus struct {
	mu         sync.RWMutex
	handlers   map[EventType][]EventHandler
	async      bool
	logger     *zap.Logger
	bufferSize int
	eventCh    chan Event
	stopCh     chan struct{}
	wg         sync.WaitGroup
}

func NewEventBus(logger *zap.Logger) *EventBus {
	return &EventBus{
		handlers:   make(map[EventType][]EventHandler),
		logger:     logger.With(zap.String("component", "event_bus")),
		bufferSize: 256,
	}
}

func NewAsyncEventBus(logger *zap.Logger, bufferSize int) *EventBus {
	if bufferSize <= 0 {
		bufferSize = 256
	}
	bus := &EventBus{
		handlers:   make(map[EventType][]EventHandler),
		async:      true,
		logger:     logger.With(zap.String("component", "event_bus")),
		bufferSize: bufferSize,
		eventCh:    make(chan Event, bufferSize),
		stopCh:     make(chan struct{}),
	}
	bus.startDispatcher()
	return bus
}

func (b *EventBus) Subscribe(eventType EventType, handler EventHandler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlers[eventType] = append(b.handlers[eventType], handler)
}

func (b *EventBus) Unsubscribe(eventType EventType, handler EventHandler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	handlers := b.handlers[eventType]
	for i, h := range handlers {
		if &h == &handler {
			b.handlers[eventType] = append(handlers[:i], handlers[i+1:]...)
			break
		}
	}
}

func (b *EventBus) SubscribeAll(handler EventHandler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for eventType := range b.handlers {
		b.handlers[eventType] = append(b.handlers[eventType], handler)
	}
}

func (b *EventBus) SubscribeMany(handler EventHandler, types ...EventType) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, t := range types {
		b.handlers[t] = append(b.handlers[t], handler)
	}
}

func (b *EventBus) Publish(event Event) {
	if b.async {
		select {
		case b.eventCh <- event:
		default:
			b.logger.Warn("event bus channel full, dropping event",
				zap.String("type", string(event.Type)),
				zap.String("pipeline_id", event.PipelineID.String()),
			)
		}
		return
	}

	b.dispatch(event)
}

func (b *EventBus) dispatch(event Event) {
	b.mu.RLock()
	handlers, exists := b.handlers[event.Type]
	b.mu.RUnlock()

	if !exists {
		return
	}

	for _, handler := range handlers {
		func() {
			defer func() {
				if r := recover(); r != nil {
					b.logger.Error("event handler panicked",
						zap.String("event_type", string(event.Type)),
						zap.Any("panic", r),
					)
				}
			}()
			handler(event)
		}()
	}
}

func (b *EventBus) startDispatcher() {
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		for {
			select {
			case event := <-b.eventCh:
				b.dispatch(event)
			case <-b.stopCh:
				b.drainRemaining()
				return
			}
		}
	}()
}

func (b *EventBus) drainRemaining() {
	for {
		select {
		case event := <-b.eventCh:
			b.dispatch(event)
		default:
			return
		}
	}
}

func (b *EventBus) Stop() {
	if b.async {
		close(b.stopCh)
		b.wg.Wait()
	}
}

func (b *EventBus) HasHandlers(eventType EventType) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	handlers, exists := b.handlers[eventType]
	return exists && len(handlers) > 0
}

func (b *EventBus) HandlerCount(eventType EventType) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.handlers[eventType])
}

func (b *EventBus) ClearHandlers() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlers = make(map[EventType][]EventHandler)
}

func (b *EventBus) ClearHandlersForType(eventType EventType) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.handlers, eventType)
}

func NewStartedEvent(pipelineID uuid.UUID, runID uuid.UUID) Event {
	return Event{Type: EventPipelineStarted, PipelineID: pipelineID, RunID: runID}
}

func NewCompletedEvent(pipelineID uuid.UUID, runID uuid.UUID) Event {
	return Event{Type: EventPipelineCompleted, PipelineID: pipelineID, RunID: runID}
}

func NewFailedEvent(pipelineID uuid.UUID, runID uuid.UUID, err string) Event {
	return Event{Type: EventPipelineFailed, PipelineID: pipelineID, RunID: runID, Error: err}
}

func NewRetryingEvent(pipelineID uuid.UUID, runID uuid.UUID, stageName string, err string) Event {
	return Event{Type: EventPipelineRetrying, PipelineID: pipelineID, RunID: runID, StageName: stageName, Error: err}
}

func NewStageStartedEvent(pipelineID uuid.UUID, runID uuid.UUID, stageName string) Event {
	return Event{Type: EventStageStarted, PipelineID: pipelineID, RunID: runID, StageName: stageName}
}

func NewStageCompletedEvent(pipelineID uuid.UUID, runID uuid.UUID, stageName string) Event {
	return Event{Type: EventStageCompleted, PipelineID: pipelineID, RunID: runID, StageName: stageName}
}

func NewStageFailedEvent(pipelineID uuid.UUID, runID uuid.UUID, stageName string, err string) Event {
	return Event{Type: EventStageFailed, PipelineID: pipelineID, RunID: runID, StageName: stageName, Error: err}
}
