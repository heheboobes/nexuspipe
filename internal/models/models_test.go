package models

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestNewPipeline(t *testing.T) {
	userID := uuid.New()
	p := NewPipeline("test-pipeline", userID)

	if p.ID == uuid.Nil {
		t.Error("expected non-nil pipeline ID")
	}
	if p.Name != "test-pipeline" {
		t.Errorf("expected name 'test-pipeline', got %q", p.Name)
	}
	if p.Status != PipelineStatusDraft {
		t.Errorf("expected status 'draft', got %q", p.Status)
	}
	if p.Version != 1 {
		t.Errorf("expected version 1, got %d", p.Version)
	}
	if p.CreatedBy != userID {
		t.Errorf("expected CreatedBy %v, got %v", userID, p.CreatedBy)
	}
	if p.CreatedAt.IsZero() {
		t.Error("expected non-zero CreatedAt")
	}
	if p.UpdatedAt.IsZero() {
		t.Error("expected non-zero UpdatedAt")
	}
	if p.CreatedAt != p.UpdatedAt {
		t.Error("expected CreatedAt to equal UpdatedAt on creation")
	}
}

func TestNewPipelineConfig(t *testing.T) {
	cfg := DefaultPipelineConfig()

	if cfg.MaxRetries != 3 {
		t.Errorf("expected MaxRetries 3, got %d", cfg.MaxRetries)
	}
	if cfg.TimeoutSeconds != 300 {
		t.Errorf("expected TimeoutSeconds 300, got %d", cfg.TimeoutSeconds)
	}
	if cfg.Concurrency != 1 {
		t.Errorf("expected Concurrency 1, got %d", cfg.Concurrency)
	}
	if cfg.Priority != 0 {
		t.Errorf("expected Priority 0, got %d", cfg.Priority)
	}
	if cfg.QueueName != "nexuspipe.pipelines.default" {
		t.Errorf("expected QueueName 'nexuspipe.pipelines.default', got %q", cfg.QueueName)
	}
	if !cfg.DLQEnabled {
		t.Error("expected DLQEnabled to be true")
	}
	if cfg.BackoffMultiplier != 2.0 {
		t.Errorf("expected BackoffMultiplier 2.0, got %f", cfg.BackoffMultiplier)
	}
	if cfg.Environment != "production" {
		t.Errorf("expected Environment 'production', got %q", cfg.Environment)
	}
}

func TestPipelineStatusValid(t *testing.T) {
	valid := []PipelineStatus{
		PipelineStatusActive,
		PipelineStatusInactive,
		PipelineStatusPaused,
		PipelineStatusArchived,
		PipelineStatusDraft,
		PipelineStatusFailed,
	}
	for _, s := range valid {
		if !s.IsValid() {
			t.Errorf("expected %q to be valid", s)
		}
	}
	if PipelineStatus("invalid").IsValid() {
		t.Error("expected 'invalid' status to be invalid")
	}
}

func TestNewEvent(t *testing.T) {
	body := json.RawMessage(`{"key": "value"}`)
	e := NewEvent("pipeline.run", "test", body)

	if e.ID == uuid.Nil {
		t.Error("expected non-nil event ID")
	}
	if e.EventType != "pipeline.run" {
		t.Errorf("expected EventType 'pipeline.run', got %q", e.EventType)
	}
	if e.Source != "test" {
		t.Errorf("expected Source 'test', got %q", e.Source)
	}
	if e.Status != EventStatusPending {
		t.Errorf("expected Status 'pending', got %q", e.Status)
	}
	if e.Priority != 0 {
		t.Errorf("expected Priority 0, got %d", e.Priority)
	}
	if e.MaxRetries != 3 {
		t.Errorf("expected MaxRetries 3, got %d", e.MaxRetries)
	}
	if e.CreatedAt.IsZero() {
		t.Error("expected non-zero CreatedAt")
	}
}

func TestEventStatusTransitions(t *testing.T) {
	tests := []struct {
		status   EventStatus
		terminal bool
		valid    bool
	}{
		{EventStatusPending, false, true},
		{EventStatusProcessing, false, true},
		{EventStatusCompleted, true, true},
		{EventStatusFailed, true, true},
		{EventStatusRetrying, false, true},
		{EventStatusCancelled, true, true},
		{EventStatusDelayed, false, true},
		{EventStatusDeadLetter, true, true},
		{"unknown", false, false},
	}

	for _, tc := range tests {
		if tc.status.IsValid() != tc.valid {
			t.Errorf("IsValid(%q) = %v, want %v", tc.status, tc.status.IsValid(), tc.valid)
		}
		if tc.status.IsTerminal() != tc.terminal {
			t.Errorf("IsTerminal(%q) = %v, want %v", tc.status, tc.status.IsTerminal(), tc.terminal)
		}
	}
}

func TestNewTask(t *testing.T) {
	pipelineID := uuid.New()
	task := NewTask(pipelineID, "http-request", TaskTypeHTTP)

	if task.ID == uuid.Nil {
		t.Error("expected non-nil task ID")
	}
	if task.PipelineID != pipelineID {
		t.Errorf("expected PipelineID %v, got %v", pipelineID, task.PipelineID)
	}
	if task.Name != "http-request" {
		t.Errorf("expected Name 'http-request', got %q", task.Name)
	}
	if task.Type != TaskTypeHTTP {
		t.Errorf("expected Type 'http', got %q", task.Type)
	}
	if task.Status != TaskStatusPending {
		t.Errorf("expected Status 'pending', got %q", task.Status)
	}
	if task.MaxRetries != 3 {
		t.Errorf("expected MaxRetries 3, got %d", task.MaxRetries)
	}
	if task.Timeout != 60 {
		t.Errorf("expected Timeout 60, got %d", task.Timeout)
	}
	if task.CreatedAt.IsZero() {
		t.Error("expected non-zero CreatedAt")
	}
	if task.UpdatedAt.IsZero() {
		t.Error("expected non-zero UpdatedAt")
	}
}

func TestTaskTypeValidation(t *testing.T) {
	validTypes := []TaskType{
		TaskTypeHTTP, TaskTypeGRPC, TaskTypeScript, TaskTypeSQL,
		TaskTypeShell, TaskTypeWebhook, TaskTypeTransform,
		TaskTypeNotification, TaskTypeCustom,
	}
	for _, tt := range validTypes {
		if !tt.IsValid() {
			t.Errorf("expected %q to be a valid task type", tt)
		}
	}
	if TaskType("invalid").IsValid() {
		t.Error("expected 'invalid' task type to be invalid")
	}
}

func TestTaskStatusTransitions(t *testing.T) {
	tests := []struct {
		status   TaskStatus
		terminal bool
		valid    bool
	}{
		{TaskStatusPending, false, true},
		{TaskStatusScheduled, false, true},
		{TaskStatusRunning, false, true},
		{TaskStatusCompleted, true, true},
		{TaskStatusFailed, true, true},
		{TaskStatusCancelled, true, true},
		{TaskStatusRetrying, false, true},
		{TaskStatusPaused, false, true},
		{TaskStatusTimedOut, true, true},
		{TaskStatusSkipped, true, true},
		{"bogus", false, false},
	}

	for _, tc := range tests {
		if tc.status.IsValid() != tc.valid {
			t.Errorf("IsValid(%q) = %v, want %v", tc.status, tc.status.IsValid(), tc.valid)
		}
		if tc.status.IsTerminal() != tc.terminal {
			t.Errorf("IsTerminal(%q) = %v, want %v", tc.status, tc.status.IsTerminal(), tc.terminal)
		}
	}
}

func TestDefaultPipelineFilter(t *testing.T) {
	f := DefaultPipelineFilter()

	if f.Limit != 20 {
		t.Errorf("expected Limit 20, got %d", f.Limit)
	}
	if f.Offset != 0 {
		t.Errorf("expected Offset 0, got %d", f.Offset)
	}
	if f.OrderBy != "created_at DESC" {
		t.Errorf("expected OrderBy 'created_at DESC', got %q", f.OrderBy)
	}
}

func TestDefaultEventFilter(t *testing.T) {
	f := DefaultEventFilter()

	if f.Limit != 50 {
		t.Errorf("expected Limit 50, got %d", f.Limit)
	}
	if f.Offset != 0 {
		t.Errorf("expected Offset 0, got %d", f.Offset)
	}
	if f.OrderBy != "created_at DESC" {
		t.Errorf("expected OrderBy 'created_at DESC', got %q", f.OrderBy)
	}
}

func TestDefaultTaskFilter(t *testing.T) {
	f := DefaultTaskFilter()

	if f.Limit != 50 {
		t.Errorf("expected Limit 50, got %d", f.Limit)
	}
	if f.Offset != 0 {
		t.Errorf("expected Offset 0, got %d", f.Offset)
	}
	if f.OrderBy != "created_at DESC" {
		t.Errorf("expected OrderBy 'created_at DESC', got %q", f.OrderBy)
	}
}

func TestNewPipelineWithTimestamps(t *testing.T) {
	before := time.Now().UTC().Add(-time.Second)
	p := NewPipeline("timed", uuid.New())
	after := time.Now().UTC().Add(time.Second)

	if p.CreatedAt.Before(before) || p.CreatedAt.After(after) {
		t.Error("CreatedAt outside expected time range")
	}
	if p.UpdatedAt.Before(before) || p.UpdatedAt.After(after) {
		t.Error("UpdatedAt outside expected time range")
	}
}

func TestDefaultTaskConfig(t *testing.T) {
	cfg := DefaultTaskConfig()

	if !cfg.Retryable {
		t.Error("expected Retryable to be true")
	}
	if cfg.RetryBackoffMS != 1000 {
		t.Errorf("expected RetryBackoffMS 1000, got %d", cfg.RetryBackoffMS)
	}
	if cfg.Headers == nil {
		t.Error("expected non-nil Headers map")
	}
	if cfg.QueryParams == nil {
		t.Error("expected non-nil QueryParams map")
	}
	if cfg.OutputMapping == nil {
		t.Error("expected non-nil OutputMapping map")
	}
}

func TestPipelineConfigDefaults(t *testing.T) {
	cfg := DefaultPipelineConfig()

	if cfg.Metadata == nil {
		t.Error("expected non-nil Metadata map")
	}
	if len(cfg.Metadata) != 0 {
		t.Errorf("expected empty Metadata, got %d entries", len(cfg.Metadata))
	}
}
