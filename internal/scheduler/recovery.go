package scheduler

import (
	"context"
	"fmt"
	"log"
	"time"

	"nexuspipe/internal/models"
)

type SkipPolicy int

const (
	SkipPolicyNone SkipPolicy = iota
	SkipPolicyExecuteLatest
	SkipPolicySkipAll
	SkipPolicyExecuteAll
)

func (p SkipPolicy) String() string {
	switch p {
	case SkipPolicyNone:
		return "none"
	case SkipPolicyExecuteLatest:
		return "execute_latest"
	case SkipPolicySkipAll:
		return "skip_all"
	case SkipPolicyExecuteAll:
		return "execute_all"
	default:
		return "unknown"
	}
}

type RecoveryResult struct {
	ScheduleID      string        `json:"schedule_id"`
	PipelineID      string        `json:"pipeline_id"`
	MissedRuns      int           `json:"missed_runs"`
	ExecutedRuns    int           `json:"executed_runs"`
	SkippedRuns     int           `json:"skipped_runs"`
	NextScheduledAt time.Time     `json:"next_scheduled_at"`
	Policy          SkipPolicy    `json:"policy"`
	Duration        time.Duration `json:"duration"`
	Error           string        `json:"error,omitempty"`
}

type RecoveryConfig struct {
	Enabled     bool          `json:"enabled"`
	Policy      SkipPolicy    `json:"policy"`
	MaxLookback time.Duration `json:"max_lookback"`
	BatchSize   int           `json:"batch_size"`
	Interval    time.Duration `json:"interval"`
	DryRun      bool          `json:"dry_run"`
}

func DefaultRecoveryConfig() RecoveryConfig {
	return RecoveryConfig{
		Enabled:     true,
		Policy:      SkipPolicyExecuteLatest,
		MaxLookback: 24 * time.Hour,
		BatchSize:   20,
		Interval:    5 * time.Minute,
	}
}

type RecoveryEngine struct {
	store  ScheduleStore
	config RecoveryConfig
}

func NewRecoveryEngine(store ScheduleStore, cfg RecoveryConfig) *RecoveryEngine {
	return &RecoveryEngine{
		store:  store,
		config: cfg,
	}
}

func (r *RecoveryEngine) RecoverMissedJobs(ctx context.Context) ([]RecoveryResult, error) {
	if !r.config.Enabled {
		log.Println("schedule recovery is disabled, skipping")
		return nil, nil
	}

	schedules, err := r.store.GetActiveSchedules(ctx)
	if err != nil {
		return nil, fmt.Errorf("get active schedules for recovery: %w", err)
	}

	now := time.Now().UTC()
	results := make([]RecoveryResult, 0, len(schedules))

	for _, sch := range schedules {
		result, err := r.recoverSingleSchedule(ctx, sch, now)
		if err != nil {
			log.Printf("recovery error for schedule %s: %v", sch.ID, err)
			result = RecoveryResult{
				ScheduleID: sch.ID,
				PipelineID: sch.PipelineID,
				Error:      err.Error(),
			}
		}
		results = append(results, result)
	}

	executed := 0
	skipped := 0
	for _, res := range results {
		executed += res.ExecutedRuns
		skipped += res.SkippedRuns
	}

	log.Printf("recovery complete: %d schedules checked, %d executed, %d skipped",
		len(schedules), executed, skipped)

	return results, nil
}

func (r *RecoveryEngine) recoverSingleSchedule(ctx context.Context, sch *models.Schedule, now time.Time) (RecoveryResult, error) {
	start := time.Now()
	result := RecoveryResult{
		ScheduleID: sch.ID,
		PipelineID: sch.PipelineID,
		Policy:     r.config.Policy,
	}

	if !sch.Enabled || sch.Status != models.ScheduleStatusActive {
		return result, nil
	}

	loc, err := time.LoadLocation(sch.Timezone)
	if err != nil {
		return result, fmt.Errorf("load timezone %s: %w", sch.Timezone, err)
	}

	lookbackStart := now.Add(-r.config.MaxLookback)
	lastRun := sch.LastRunTime

	since := lookbackStart
	if lastRun != nil && lastRun.After(lookbackStart) {
		since = *lastRun
	}

	if sch.NextRunTimeValue.After(now) {
		result.NextScheduledAt = sch.NextRunTimeValue
		return result, nil
	}

	missedCount, nextRun, err := CountMissedRuns(sch.CronExpression, since, now, loc)
	if err != nil {
		return result, fmt.Errorf("count missed runs: %w", err)
	}

	result.MissedRuns = missedCount

	if missedCount == 0 {
		return result, nil
	}

	switch r.config.Policy {
	case SkipPolicySkipAll:
		result.SkippedRuns = missedCount
		result.NextScheduledAt = nextRun
		if err := r.store.UpdateNextRunTime(ctx, sch.ID, nextRun); err != nil {
			return result, fmt.Errorf("update next run after skip: %w", err)
		}

	case SkipPolicyExecuteLatest:
		result.SkippedRuns = missedCount - 1
		result.ExecutedRuns = 1
		if err := r.executeRecoveryJob(ctx, sch); err != nil {
			return result, fmt.Errorf("execute latest recovery job: %w", err)
		}
		result.NextScheduledAt = nextRun
		if err := r.store.UpdateNextRunTime(ctx, sch.ID, nextRun); err != nil {
			return result, fmt.Errorf("update next run after execute latest: %w", err)
		}

	case SkipPolicyExecuteAll:
		executeCount := 0
		intermediate := since
		for i := 0; i < missedCount; i++ {
			intermediate = scheduleNext(sch.CronExpression, intermediate, loc)
			if r.config.DryRun {
				executeCount++
				continue
			}
			if err := r.executeRecoveryJob(ctx, sch); err != nil {
				log.Printf("recovery execute job %d for schedule %s failed: %v", i, sch.ID, err)
				continue
			}
			executeCount++
		}
		result.ExecutedRuns = executeCount
		result.NextScheduledAt = nextRun
		if !r.config.DryRun {
			if err := r.store.UpdateNextRunTime(ctx, sch.ID, nextRun); err != nil {
				return result, fmt.Errorf("update next run after execute all: %w", err)
			}
		}

	case SkipPolicyNone:
		result.ExecutedRuns = missedCount
		result.NextScheduledAt = nextRun

	default:
		result.SkippedRuns = missedCount
		result.NextScheduledAt = nextRun
	}

	result.Duration = time.Since(start)
	return result, nil
}

func (r *RecoveryEngine) executeRecoveryJob(ctx context.Context, sch *models.Schedule) error {
	if r.config.DryRun {
		return nil
	}
	job, err := NewScheduledJob(sch)
	if err != nil {
		return fmt.Errorf("create recovery job: %w", err)
	}
	exec := NewJobExecution(job)
	log.Printf("recovery dispatching job: schedule=%s pipeline=%s exec=%s",
		exec.ScheduleID, exec.PipelineID, exec.ID)
	return nil
}

func scheduleNext(expr string, from time.Time, loc *time.Location) time.Time {
	schedule, err := ParseCronExpression(expr)
	if err != nil {
		return from
	}
	return schedule.Next(from.In(loc))
}

func (r *RecoveryEngine) RecoverSpecificSchedule(ctx context.Context, scheduleID string) (*RecoveryResult, error) {
	sch, err := r.store.GetSchedule(ctx, scheduleID)
	if err != nil {
		return nil, fmt.Errorf("get schedule %s: %w", scheduleID, err)
	}
	result, err := r.recoverSingleSchedule(ctx, sch, time.Now().UTC())
	return &result, err
}

func (r *RecoveryEngine) DetectMissedSchedules(ctx context.Context) ([]string, error) {
	due, err := r.store.GetDueSchedules(ctx, r.config.BatchSize)
	if err != nil {
		return nil, fmt.Errorf("get due schedules for detection: %w", err)
	}
	ids := make([]string, 0, len(due))
	for _, sch := range due {
		if sch.LastRunTime != nil {
			expectedNext := scheduleNext(sch.CronExpression, *sch.LastRunTime, time.UTC)
			if expectedNext.Before(sch.NextRunTimeValue) {
				ids = append(ids, sch.ID)
			}
		}
	}
	return ids, nil
}
