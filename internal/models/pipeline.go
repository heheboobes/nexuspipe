package models

import (
	"time"

	"github.com/google/uuid"
)

type PipelineStatus string

const (
	PipelineStatusActive   PipelineStatus = "active"
	PipelineStatusInactive PipelineStatus = "inactive"
	PipelineStatusPaused   PipelineStatus = "paused"
	PipelineStatusArchived PipelineStatus = "archived"
	PipelineStatusDraft    PipelineStatus = "draft"
	PipelineStatusFailed   PipelineStatus = "failed"
)

func (s PipelineStatus) IsValid() bool {
	switch s {
	case PipelineStatusActive, PipelineStatusInactive, PipelineStatusPaused,
		PipelineStatusArchived, PipelineStatusDraft, PipelineStatusFailed:
		return true
	}
	return false
}

func (s PipelineStatus) String() string {
	return string(s)
}

type Pipeline struct {
	ID          uuid.UUID         `json:"id" db:"id" yaml:"id"`
	Name        string            `json:"name" db:"name" yaml:"name"`
	Description string            `json:"description,omitempty" db:"description" yaml:"description,omitempty"`
	Status      PipelineStatus    `json:"status" db:"status" yaml:"status"`
	Version     int               `json:"version" db:"version" yaml:"version"`
	Config      PipelineConfig    `json:"config" db:"config" yaml:"config"`
	Tags        map[string]string `json:"tags,omitempty" db:"tags" yaml:"tags,omitempty"`
	CreatedBy   uuid.UUID         `json:"created_by" db:"created_by" yaml:"created_by"`
	CreatedAt   time.Time         `json:"created_at" db:"created_at" yaml:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at" db:"updated_at" yaml:"updated_at"`
	DeletedAt   *time.Time        `json:"deleted_at,omitempty" db:"deleted_at" yaml:"deleted_at,omitempty"`
}

type PipelineConfig struct {
	MaxRetries        int               `json:"max_retries" yaml:"max_retries"`
	TimeoutSeconds    int               `json:"timeout_seconds" yaml:"timeout_seconds"`
	Concurrency       int               `json:"concurrency" yaml:"concurrency"`
	Priority          int               `json:"priority" yaml:"priority"`
	QueueName         string            `json:"queue_name" yaml:"queue_name"`
	ExchangeName      string            `json:"exchange_name" yaml:"exchange_name"`
	RoutingKey        string            `json:"routing_key" yaml:"routing_key"`
	ErrorHandler      string            `json:"error_handler,omitempty" yaml:"error_handler,omitempty"`
	SuccessHandler    string            `json:"success_handler,omitempty" yaml:"success_handler,omitempty"`
	DLQEnabled        bool              `json:"dlq_enabled" yaml:"dlq_enabled"`
	DLQName           string            `json:"dlq_name,omitempty" yaml:"dlq_name,omitempty"`
	RetryBackoffMS    int               `json:"retry_backoff_ms" yaml:"retry_backoff_ms"`
	MaxBackoffMS      int               `json:"max_backoff_ms" yaml:"max_backoff_ms"`
	BackoffMultiplier float64           `json:"backoff_multiplier" yaml:"backoff_multiplier"`
	Environment       string            `json:"environment" yaml:"environment"`
	Metadata          map[string]string `json:"metadata,omitempty" yaml:"metadata,omitempty"`
}

type PipelineExecution struct {
	ID          uuid.UUID      `json:"id" db:"id"`
	PipelineID  uuid.UUID      `json:"pipeline_id" db:"pipeline_id"`
	Status      PipelineStatus `json:"status" db:"status"`
	Input       string         `json:"input" db:"input"`
	Output      string         `json:"output,omitempty" db:"output"`
	Error       string         `json:"error,omitempty" db:"error"`
	StartedAt   *time.Time     `json:"started_at,omitempty" db:"started_at"`
	CompletedAt *time.Time     `json:"completed_at,omitempty" db:"completed_at"`
	DurationMS  int64          `json:"duration_ms,omitempty" db:"duration_ms"`
	RetryCount  int            `json:"retry_count" db:"retry_count"`
	TriggeredBy string         `json:"triggered_by" db:"triggered_by"`
	CreatedAt   time.Time      `json:"created_at" db:"created_at"`
}

type PipelineVersion struct {
	ID         uuid.UUID      `json:"id" db:"id"`
	PipelineID uuid.UUID      `json:"pipeline_id" db:"pipeline_id"`
	Version    int            `json:"version" db:"version"`
	Config     PipelineConfig `json:"config" db:"config"`
	Changelog  string         `json:"changelog,omitempty" db:"changelog"`
	Published  bool           `json:"published" db:"published"`
	CreatedBy  uuid.UUID      `json:"created_by" db:"created_by"`
	CreatedAt  time.Time      `json:"created_at" db:"created_at"`
}

func NewPipeline(name string, createdBy uuid.UUID) *Pipeline {
	now := time.Now().UTC()
	return &Pipeline{
		ID:        uuid.New(),
		Name:      name,
		Status:    PipelineStatusDraft,
		Version:   1,
		Config:    DefaultPipelineConfig(),
		Tags:      make(map[string]string),
		CreatedBy: createdBy,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func DefaultPipelineConfig() PipelineConfig {
	return PipelineConfig{
		MaxRetries:        3,
		TimeoutSeconds:    300,
		Concurrency:       1,
		Priority:          0,
		QueueName:         "nexuspipe.pipelines.default",
		ExchangeName:      "nexuspipe.events",
		RoutingKey:        "pipeline.execute",
		DLQEnabled:        true,
		DLQName:           "nexuspipe.pipelines.dlq",
		RetryBackoffMS:    1000,
		MaxBackoffMS:      60000,
		BackoffMultiplier: 2.0,
		Environment:       "production",
		Metadata:          make(map[string]string),
	}
}

type PipelineFilter struct {
	Status  []PipelineStatus  `json:"status,omitempty"`
	Search  string            `json:"search,omitempty"`
	Tags    map[string]string `json:"tags,omitempty"`
	Limit   int               `json:"limit"`
	Offset  int               `json:"offset"`
	OrderBy string            `json:"order_by"`
}

func DefaultPipelineFilter() PipelineFilter {
	return PipelineFilter{
		Limit:   20,
		Offset:  0,
		OrderBy: "created_at DESC",
	}
}
