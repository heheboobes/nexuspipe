package models

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type EventStatus string

const (
	EventStatusPending    EventStatus = "pending"
	EventStatusProcessing EventStatus = "processing"
	EventStatusCompleted  EventStatus = "completed"
	EventStatusFailed     EventStatus = "failed"
	EventStatusRetrying   EventStatus = "retrying"
	EventStatusCancelled  EventStatus = "cancelled"
	EventStatusDelayed    EventStatus = "delayed"
	EventStatusDeadLetter EventStatus = "dead_letter"
)

func (s EventStatus) IsValid() bool {
	switch s {
	case EventStatusPending, EventStatusProcessing, EventStatusCompleted,
		EventStatusFailed, EventStatusRetrying, EventStatusCancelled,
		EventStatusDelayed, EventStatusDeadLetter:
		return true
	}
	return false
}

func (s EventStatus) IsTerminal() bool {
	return s == EventStatusCompleted || s == EventStatusFailed ||
		s == EventStatusCancelled || s == EventStatusDeadLetter
}

type Event struct {
	ID            uuid.UUID       `json:"id" db:"id" yaml:"id"`
	PipelineID    *uuid.UUID      `json:"pipeline_id,omitempty" db:"pipeline_id" yaml:"pipeline_id,omitempty"`
	EventType     string          `json:"event_type" db:"event_type" yaml:"event_type"`
	Source        string          `json:"source" db:"source" yaml:"source"`
	Status        EventStatus     `json:"status" db:"status" yaml:"status"`
	Priority      int             `json:"priority" db:"priority" yaml:"priority"`
	Body          json.RawMessage `json:"body" db:"body" yaml:"body"`
	Headers       EventMetadata   `json:"headers" db:"headers" yaml:"headers"`
	RetryCount    int             `json:"retry_count" db:"retry_count" yaml:"retry_count"`
	MaxRetries    int             `json:"max_retries" db:"max_retries" yaml:"max_retries"`
	CorrelationID string          `json:"correlation_id,omitempty" db:"correlation_id" yaml:"correlation_id,omitempty"`
	CausationID   string          `json:"causation_id,omitempty" db:"causation_id" yaml:"causation_id,omitempty"`
	CreatedAt     time.Time       `json:"created_at" db:"created_at" yaml:"created_at"`
	UpdatedAt     time.Time       `json:"updated_at" db:"updated_at" yaml:"updated_at"`
	ProcessedAt   *time.Time      `json:"processed_at,omitempty" db:"processed_at" yaml:"processed_at,omitempty"`
	ScheduledAt   *time.Time      `json:"scheduled_at,omitempty" db:"scheduled_at" yaml:"scheduled_at,omitempty"`
	TTL           *int            `json:"ttl,omitempty" db:"ttl" yaml:"ttl,omitempty"`
}

type EventMetadata struct {
	ContentType    string            `json:"content_type,omitempty" yaml:"content_type,omitempty"`
	Encoding       string            `json:"encoding,omitempty" yaml:"encoding,omitempty"`
	SchemaVersion  string            `json:"schema_version,omitempty" yaml:"schema_version,omitempty"`
	TraceID        string            `json:"trace_id,omitempty" yaml:"trace_id,omitempty"`
	SpanID         string            `json:"span_id,omitempty" yaml:"span_id,omitempty"`
	UserAgent      string            `json:"user_agent,omitempty" yaml:"user_agent,omitempty"`
	IPAddress      string            `json:"ip_address,omitempty" yaml:"ip_address,omitempty"`
	IdempotencyKey string            `json:"idempotency_key,omitempty" yaml:"idempotency_key,omitempty"`
	Custom         map[string]string `json:"custom,omitempty" yaml:"custom,omitempty"`
}

type EventDeliveryStatus struct {
	EventID     uuid.UUID   `json:"event_id" db:"event_id"`
	Status      EventStatus `json:"status" db:"status"`
	ConsumerID  string      `json:"consumer_id" db:"consumer_id"`
	DeliveredAt time.Time   `json:"delivered_at" db:"delivered_at"`
	AckedAt     *time.Time  `json:"acked_at,omitempty" db:"acked_at"`
	Error       string      `json:"error,omitempty" db:"error"`
	Attempt     int         `json:"attempt" db:"attempt"`
}

type EventFilter struct {
	Status     []EventStatus `json:"status,omitempty"`
	EventTypes []string      `json:"event_types,omitempty"`
	Source     string        `json:"source,omitempty"`
	PipelineID *uuid.UUID    `json:"pipeline_id,omitempty"`
	From       *time.Time    `json:"from,omitempty"`
	To         *time.Time    `json:"to,omitempty"`
	Search     string        `json:"search,omitempty"`
	Limit      int           `json:"limit"`
	Offset     int           `json:"offset"`
	OrderBy    string        `json:"order_by"`
}

func NewEvent(eventType, source string, body json.RawMessage) *Event {
	now := time.Now().UTC()
	return &Event{
		ID:         uuid.New(),
		EventType:  eventType,
		Source:     source,
		Status:     EventStatusPending,
		Priority:   0,
		Body:       body,
		Headers:    EventMetadata{Custom: make(map[string]string)},
		MaxRetries: 3,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
}

func DefaultEventFilter() EventFilter {
	return EventFilter{
		Limit:   50,
		Offset:  0,
		OrderBy: "created_at DESC",
	}
}
