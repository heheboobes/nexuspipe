package models

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

type ScheduleStatus string

const (
	ScheduleStatusActive  ScheduleStatus = "active"
	ScheduleStatusPaused  ScheduleStatus = "paused"
	ScheduleStatusFailed  ScheduleStatus = "failed"
	ScheduleStatusDeleted ScheduleStatus = "deleted"
)

func (s ScheduleStatus) IsValid() bool {
	switch s {
	case ScheduleStatusActive, ScheduleStatusPaused, ScheduleStatusFailed, ScheduleStatusDeleted:
		return true
	default:
		return false
	}
}

type Schedule struct {
	ID                string          `json:"id" db:"id"`
	PipelineID        string          `json:"pipeline_id" db:"pipeline_id"`
	CronExpression    string          `json:"cron_expression" db:"cron_expression"`
	Timezone          string          `json:"timezone" db:"timezone"`
	Status            ScheduleStatus  `json:"status" db:"status"`
	Enabled           bool            `json:"enabled" db:"enabled"`
	Priority          int             `json:"priority" db:"priority"`
	MaxConcurrentRuns int             `json:"max_concurrent_runs" db:"max_concurrent_runs"`
	Tags              []string        `json:"tags,omitempty" db:"tags"`
	Metadata          json.RawMessage `json:"metadata,omitempty" db:"metadata"`
	NextRunTimeValue  time.Time       `json:"next_run_time" db:"next_run_time"`
	LastRunTime       *time.Time      `json:"last_run_time,omitempty" db:"last_run_time"`
	CreatedBy         string          `json:"created_by" db:"created_by"`
	CreatedAt         time.Time       `json:"created_at" db:"created_at"`
	UpdatedAt         time.Time       `json:"updated_at" db:"updated_at"`
}

func (s *Schedule) NextRunTime() (time.Time, error) {
	if s.CronExpression == "" {
		return time.Time{}, fmt.Errorf("cron expression is empty")
	}

	loc, err := time.LoadLocation(s.Timezone)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid timezone %s: %w", s.Timezone, err)
	}

	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	schedule, err := parser.Parse(s.CronExpression)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid cron expression %q: %w", s.CronExpression, err)
	}

	now := time.Now().In(loc)
	next := schedule.Next(now)

	return next, nil
}

func (s *Schedule) Validate() error {
	if s.ID == "" {
		return fmt.Errorf("schedule id is required")
	}
	if s.PipelineID == "" {
		return fmt.Errorf("pipeline_id is required")
	}
	if s.CronExpression == "" {
		return fmt.Errorf("cron_expression is required")
	}
	if !s.Status.IsValid() {
		return fmt.Errorf("invalid schedule status: %q", s.Status)
	}
	if s.Priority < 0 {
		return fmt.Errorf("priority must be non-negative")
	}
	if s.MaxConcurrentRuns <= 0 {
		return fmt.Errorf("max_concurrent_runs must be positive")
	}

	loc, err := time.LoadLocation(s.Timezone)
	if err != nil {
		return fmt.Errorf("invalid timezone %q: %w", s.Timezone, err)
	}
	if loc == nil {
		return fmt.Errorf("resolved timezone is nil for %q", s.Timezone)
	}

	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	_, err = parser.Parse(s.CronExpression)
	if err != nil {
		return fmt.Errorf("invalid cron expression %q: %w", s.CronExpression, err)
	}

	return nil
}

func (s *Schedule) ShouldRunNow() bool {
	if !s.Enabled {
		return false
	}
	if s.Status != ScheduleStatusActive {
		return false
	}
	if s.NextRunTimeValue.IsZero() {
		return false
	}
	return time.Now().After(s.NextRunTimeValue) || time.Now().Equal(s.NextRunTimeValue)
}

func (s *Schedule) HumanReadableNextRun() string {
	if s.NextRunTimeValue.IsZero() {
		return "never"
	}
	return s.NextRunTimeValue.Format(time.RFC3339)
}

func ParseCronExpression(expr string) (cron.Schedule, error) {
	if expr == "" {
		return nil, fmt.Errorf("cron expression cannot be empty")
	}

	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	schedule, err := parser.Parse(expr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse cron expression %q: %w", expr, err)
	}

	return schedule, nil
}

func NextRunFromCron(expr string, loc *time.Location) (time.Time, error) {
	if loc == nil {
		loc = time.UTC
	}

	schedule, err := ParseCronExpression(expr)
	if err != nil {
		return time.Time{}, err
	}

	return schedule.Next(time.Now().In(loc)), nil
}

type ScheduleTrigger struct {
	ScheduleID  string    `json:"schedule_id"`
	PipelineID  string    `json:"pipeline_id"`
	TriggeredAt time.Time `json:"triggered_at"`
	SkipReason  string    `json:"skip_reason,omitempty"`
	Executed    bool      `json:"executed"`
}

type ScheduleStats struct {
	TotalSchedules  int                    `json:"total_schedules"`
	ActiveSchedules int                    `json:"active_schedules"`
	PausedSchedules int                    `json:"paused_schedules"`
	FailedSchedules int                    `json:"failed_schedules"`
	DueNow          int                    `json:"due_now"`
	ByStatus        map[ScheduleStatus]int `json:"by_status"`
}
