package scheduler

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"nexuspipe/internal/models"
)

type JobType string

const (
	JobTypePipeline JobType = "pipeline"
	JobTypeWebhook  JobType = "webhook"
	JobTypeCustom   JobType = "custom"
)

func (t JobType) IsValid() bool {
	switch t {
	case JobTypePipeline, JobTypeWebhook, JobTypeCustom:
		return true
	default:
		return false
	}
}

type JobPriority int

const (
	JobPriorityLow      JobPriority = 0
	JobPriorityNormal   JobPriority = 50
	JobPriorityHigh     JobPriority = 100
	JobPriorityCritical JobPriority = 200
)

type ScheduledJob struct {
	ID             string         `json:"id"`
	ScheduleID     string         `json:"schedule_id"`
	PipelineID     string         `json:"pipeline_id"`
	Type           JobType        `json:"type"`
	CronExpression string         `json:"cron_expression"`
	Timezone       string         `json:"timezone"`
	Priority       JobPriority    `json:"priority"`
	MaxConcurrent  int            `json:"max_concurrent"`
	Tags           []string       `json:"tags,omitempty"`
	Metadata       map[string]any `json:"metadata,omitempty"`
	NextRunAt      time.Time      `json:"next_run_at"`
	LastRunAt      *time.Time     `json:"last_run_at,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
	Disabled       bool           `json:"disabled"`
}

func NewScheduledJob(schedule *models.Schedule) (*ScheduledJob, error) {
	nextRun, err := schedule.NextRunTime()
	if err != nil {
		return nil, fmt.Errorf("compute next run for schedule %s: %w", schedule.ID, err)
	}

	var metadata map[string]any
	if len(schedule.Metadata) > 0 {
		if err := json.Unmarshal(schedule.Metadata, &metadata); err != nil {
			metadata = nil
		}
	}

	jobType := JobTypePipeline
	if metadata != nil {
		if t, ok := metadata["job_type"].(string); ok {
			proposed := JobType(t)
			if proposed.IsValid() {
				jobType = proposed
			}
		}
	}

	return &ScheduledJob{
		ID:             uuid.New().String(),
		ScheduleID:     schedule.ID,
		PipelineID:     schedule.PipelineID,
		Type:           jobType,
		CronExpression: schedule.CronExpression,
		Timezone:       schedule.Timezone,
		Priority:       JobPriority(schedule.Priority),
		MaxConcurrent:  schedule.MaxConcurrentRuns,
		Tags:           schedule.Tags,
		Metadata:       metadata,
		NextRunAt:      nextRun,
		LastRunAt:      schedule.LastRunTime,
		CreatedAt:      schedule.CreatedAt,
		Disabled:       !schedule.Enabled,
	}, nil
}

func (j *ScheduledJob) Key() string {
	return fmt.Sprintf("%s/%s", j.ScheduleID, j.PipelineID)
}

type JobExecution struct {
	ID            string          `json:"id"`
	ScheduleID    string          `json:"schedule_id"`
	PipelineID    string          `json:"pipeline_id"`
	JobType       JobType         `json:"job_type"`
	TriggeredAt   time.Time       `json:"triggered_at"`
	ScheduledAt   time.Time       `json:"scheduled_at"`
	Attempt       int             `json:"attempt"`
	MaxAttempts   int             `json:"max_attempts"`
	CorrelationID string          `json:"correlation_id"`
	Tags          []string        `json:"tags,omitempty"`
	Metadata      json.RawMessage `json:"metadata,omitempty"`
	Source        string          `json:"source"`
}

func NewJobExecution(job *ScheduledJob) *JobExecution {
	var metadata json.RawMessage
	if job.Metadata != nil {
		data, err := json.Marshal(job.Metadata)
		if err == nil {
			metadata = data
		}
	}

	return &JobExecution{
		ID:            uuid.New().String(),
		ScheduleID:    job.ScheduleID,
		PipelineID:    job.PipelineID,
		JobType:       job.Type,
		TriggeredAt:   time.Now().UTC(),
		ScheduledAt:   job.NextRunAt,
		Attempt:       1,
		MaxAttempts:   3,
		CorrelationID: uuid.New().String(),
		Tags:          job.Tags,
		Metadata:      metadata,
		Source:        "scheduler",
	}
}

func (e *JobExecution) MarshalPayload() ([]byte, error) {
	data, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("marshal job execution: %w", err)
	}
	return data, nil
}

func (e *JobExecution) RoutingKey() string {
	switch e.JobType {
	case JobTypePipeline:
		return fmt.Sprintf("pipeline.run.%s", e.PipelineID)
	case JobTypeWebhook:
		return fmt.Sprintf("webhook.trigger.%s", e.PipelineID)
	case JobTypeCustom:
		return fmt.Sprintf("custom.run.%s", e.PipelineID)
	default:
		return fmt.Sprintf("pipeline.run.%s", e.PipelineID)
	}
}

type JobResult struct {
	ExecutionID  string    `json:"execution_id"`
	ScheduleID   string    `json:"schedule_id"`
	PipelineID   string    `json:"pipeline_id"`
	Success      bool      `json:"success"`
	StartedAt    time.Time `json:"started_at"`
	CompletedAt  time.Time `json:"completed_at"`
	Duration     string    `json:"duration"`
	ErrorMessage string    `json:"error_message,omitempty"`
	Retryable    bool      `json:"retryable"`
}

func UnmarshalJobExecution(data []byte) (*JobExecution, error) {
	exec := &JobExecution{}
	if err := json.Unmarshal(data, exec); err != nil {
		return nil, fmt.Errorf("unmarshal job execution: %w", err)
	}
	return exec, nil
}
