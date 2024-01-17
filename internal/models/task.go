package models

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type TaskType string

const (
	TaskTypeHTTP         TaskType = "http"
	TaskTypeGRPC         TaskType = "grpc"
	TaskTypeScript       TaskType = "script"
	TaskTypeSQL          TaskType = "sql"
	TaskTypeShell        TaskType = "shell"
	TaskTypeWebhook      TaskType = "webhook"
	TaskTypeTransform    TaskType = "transform"
	TaskTypeNotification TaskType = "notification"
	TaskTypeCustom       TaskType = "custom"
)

func (t TaskType) IsValid() bool {
	switch t {
	case TaskTypeHTTP, TaskTypeGRPC, TaskTypeScript, TaskTypeSQL,
		TaskTypeShell, TaskTypeWebhook, TaskTypeTransform,
		TaskTypeNotification, TaskTypeCustom:
		return true
	}
	return false
}

type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusScheduled TaskStatus = "scheduled"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusCompleted TaskStatus = "completed"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusCancelled TaskStatus = "cancelled"
	TaskStatusRetrying  TaskStatus = "retrying"
	TaskStatusPaused    TaskStatus = "paused"
	TaskStatusTimedOut  TaskStatus = "timed_out"
	TaskStatusSkipped   TaskStatus = "skipped"
)

func (s TaskStatus) IsValid() bool {
	switch s {
	case TaskStatusPending, TaskStatusScheduled, TaskStatusRunning,
		TaskStatusCompleted, TaskStatusFailed, TaskStatusCancelled,
		TaskStatusRetrying, TaskStatusPaused, TaskStatusTimedOut,
		TaskStatusSkipped:
		return true
	}
	return false
}

func (s TaskStatus) IsTerminal() bool {
	return s == TaskStatusCompleted || s == TaskStatusFailed ||
		s == TaskStatusCancelled || s == TaskStatusTimedOut ||
		s == TaskStatusSkipped
}

type Task struct {
	ID           uuid.UUID       `json:"id" db:"id" yaml:"id"`
	PipelineID   uuid.UUID       `json:"pipeline_id" db:"pipeline_id" yaml:"pipeline_id"`
	EventID      *uuid.UUID      `json:"event_id,omitempty" db:"event_id" yaml:"event_id,omitempty"`
	ParentTaskID *uuid.UUID      `json:"parent_task_id,omitempty" db:"parent_task_id" yaml:"parent_task_id,omitempty"`
	Name         string          `json:"name" db:"name" yaml:"name"`
	Type         TaskType        `json:"type" db:"type" yaml:"type"`
	Status       TaskStatus      `json:"status" db:"status" yaml:"status"`
	Priority     int             `json:"priority" db:"priority" yaml:"priority"`
	Sequence     int             `json:"sequence" db:"sequence" yaml:"sequence"`
	Input        json.RawMessage `json:"input,omitempty" db:"input" yaml:"input,omitempty"`
	Output       json.RawMessage `json:"output,omitempty" db:"output" yaml:"output,omitempty"`
	Error        string          `json:"error,omitempty" db:"error" yaml:"error,omitempty"`
	Config       TaskConfig      `json:"config" db:"config" yaml:"config"`
	RetryCount   int             `json:"retry_count" db:"retry_count" yaml:"retry_count"`
	MaxRetries   int             `json:"max_retries" db:"max_retries" yaml:"max_retries"`
	Timeout      int             `json:"timeout_seconds" db:"timeout_seconds" yaml:"timeout_seconds"`
	WorkerID     string          `json:"worker_id,omitempty" db:"worker_id" yaml:"worker_id,omitempty"`
	StartedAt    *time.Time      `json:"started_at,omitempty" db:"started_at" yaml:"started_at,omitempty"`
	CompletedAt  *time.Time      `json:"completed_at,omitempty" db:"completed_at" yaml:"completed_at,omitempty"`
	DurationMS   int64           `json:"duration_ms,omitempty" db:"duration_ms" yaml:"duration_ms,omitempty"`
	ScheduledAt  *time.Time      `json:"scheduled_at,omitempty" db:"scheduled_at" yaml:"scheduled_at,omitempty"`
	CreatedAt    time.Time       `json:"created_at" db:"created_at" yaml:"created_at"`
	UpdatedAt    time.Time       `json:"updated_at" db:"updated_at" yaml:"updated_at"`
}

type TaskConfig struct {
	Method          string            `json:"method,omitempty" yaml:"method,omitempty"`
	URL             string            `json:"url,omitempty" yaml:"url,omitempty"`
	Headers         map[string]string `json:"headers,omitempty" yaml:"headers,omitempty"`
	QueryParams     map[string]string `json:"query_params,omitempty" yaml:"query_params,omitempty"`
	Body            string            `json:"body,omitempty" yaml:"body,omitempty"`
	Script          string            `json:"script,omitempty" yaml:"script,omitempty"`
	Command         string            `json:"command,omitempty" yaml:"command,omitempty"`
	SQL             string            `json:"sql,omitempty" yaml:"sql,omitempty"`
	ServiceName     string            `json:"service_name,omitempty" yaml:"service_name,omitempty"`
	Endpoint        string            `json:"endpoint,omitempty" yaml:"endpoint,omitempty"`
	ProtoFile       string            `json:"proto_file,omitempty" yaml:"proto_file,omitempty"`
	Retryable       bool              `json:"retryable" yaml:"retryable"`
	RetryBackoffMS  int               `json:"retry_backoff_ms" yaml:"retry_backoff_ms"`
	Condition       string            `json:"condition,omitempty" yaml:"condition,omitempty"`
	Transform       string            `json:"transform,omitempty" yaml:"transform,omitempty"`
	OutputMapping   map[string]string `json:"output_mapping,omitempty" yaml:"output_mapping,omitempty"`
	NotifyOnFailure bool              `json:"notify_on_failure" yaml:"notify_on_failure"`
	NotifyChannel   string            `json:"notify_channel,omitempty" yaml:"notify_channel,omitempty"`
}

type TaskDependency struct {
	TaskID    uuid.UUID `json:"task_id" db:"task_id"`
	DependsOn uuid.UUID `json:"depends_on" db:"depends_on"`
	Mandatory bool      `json:"mandatory" db:"mandatory"`
}

type TaskFilter struct {
	PipelineID *uuid.UUID   `json:"pipeline_id,omitempty"`
	Status     []TaskStatus `json:"status,omitempty"`
	Types      []TaskType   `json:"types,omitempty"`
	WorkerID   string       `json:"worker_id,omitempty"`
	From       *time.Time   `json:"from,omitempty"`
	To         *time.Time   `json:"to,omitempty"`
	Search     string       `json:"search,omitempty"`
	Limit      int          `json:"limit"`
	Offset     int          `json:"offset"`
	OrderBy    string       `json:"order_by"`
}

func NewTask(pipelineID uuid.UUID, name string, taskType TaskType) *Task {
	now := time.Now().UTC()
	return &Task{
		ID:         uuid.New(),
		PipelineID: pipelineID,
		Name:       name,
		Type:       taskType,
		Status:     TaskStatusPending,
		Priority:   0,
		Sequence:   0,
		Config:     DefaultTaskConfig(),
		MaxRetries: 3,
		Timeout:    60,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
}

func DefaultTaskConfig() TaskConfig {
	return TaskConfig{
		Retryable:      true,
		RetryBackoffMS: 1000,
		Headers:        make(map[string]string),
		QueryParams:    make(map[string]string),
		OutputMapping:  make(map[string]string),
	}
}

func DefaultTaskFilter() TaskFilter {
	return TaskFilter{
		Limit:   50,
		Offset:  0,
		OrderBy: "created_at DESC",
	}
}
