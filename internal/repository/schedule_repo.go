package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"nexuspipe/internal/models"
)

type ScheduleFilter struct {
	PipelineID string
	Status     models.ScheduleStatus
	Enabled    *bool
	Limit      int
	Offset     int
}

type ScheduleRepository struct {
	pool *pgxpool.Pool
}

func NewScheduleRepository(pool *pgxpool.Pool) *ScheduleRepository {
	return &ScheduleRepository{pool: pool}
}

func (r *ScheduleRepository) Create(ctx context.Context, s *models.Schedule) error {
	query := `
		INSERT INTO schedule_config (
			id, pipeline_id, cron_expression, timezone, status,
			enabled, priority, max_concurrent_runs, tags, metadata,
			next_run_time, last_run_time, created_by, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
			$11, $12, $13, $14, $15
		) RETURNING created_at, updated_at`

	now := time.Now().UTC()
	if s.CreatedAt.IsZero() {
		s.CreatedAt = now
	}
	if s.UpdatedAt.IsZero() {
		s.UpdatedAt = now
	}
	if s.Priority == 0 {
		s.Priority = 100
	}
	if s.MaxConcurrentRuns <= 0 {
		s.MaxConcurrentRuns = 1
	}
	if s.Timezone == "" {
		s.Timezone = "UTC"
	}

	nextRun, err := s.NextRunTime()
	if err != nil {
		return fmt.Errorf("failed to compute next run time: %w", err)
	}

	err = r.pool.QueryRow(ctx, query,
		s.ID, s.PipelineID, s.CronExpression, s.Timezone, s.Status,
		s.Enabled, s.Priority, s.MaxConcurrentRuns, s.Tags, s.Metadata,
		nextRun, s.LastRunTime, s.CreatedBy, s.CreatedAt, s.UpdatedAt,
	).Scan(&s.CreatedAt, &s.UpdatedAt)

	if err != nil {
		return fmt.Errorf("failed to create schedule: %w", err)
	}

	s.NextRunTimeValue = nextRun
	return nil
}

func (r *ScheduleRepository) Get(ctx context.Context, id string) (*models.Schedule, error) {
	query := `
		SELECT id, pipeline_id, cron_expression, timezone, status,
		       enabled, priority, max_concurrent_runs, tags, metadata,
		       next_run_time, last_run_time, created_by, created_at, updated_at
		FROM schedule_config
		WHERE id = $1`

	s := &models.Schedule{}
	err := r.pool.QueryRow(ctx, query, id).Scan(
		&s.ID, &s.PipelineID, &s.CronExpression, &s.Timezone, &s.Status,
		&s.Enabled, &s.Priority, &s.MaxConcurrentRuns, &s.Tags, &s.Metadata,
		&s.NextRunTimeValue, &s.LastRunTime, &s.CreatedBy, &s.CreatedAt, &s.UpdatedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("schedule %s: %w", id, ErrNotFound)
		}
		return nil, fmt.Errorf("failed to get schedule %s: %w", id, err)
	}

	return s, nil
}

func (r *ScheduleRepository) Update(ctx context.Context, s *models.Schedule) error {
	query := `
		UPDATE schedule_config SET
			cron_expression = $2, timezone = $3, status = $4,
			enabled = $5, priority = $6, max_concurrent_runs = $7,
			tags = $8, metadata = $9, next_run_time = $10,
			updated_at = $11
		WHERE id = $1`

	s.UpdatedAt = time.Now().UTC()

	nextRun, err := s.NextRunTime()
	if err != nil {
		return fmt.Errorf("failed to compute next run time: %w", err)
	}

	tag, err := r.pool.Exec(ctx, query,
		s.ID, s.CronExpression, s.Timezone, s.Status,
		s.Enabled, s.Priority, s.MaxConcurrentRuns,
		s.Tags, s.Metadata, nextRun, s.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to update schedule %s: %w", s.ID, err)
	}

	if tag.RowsAffected() == 0 {
		return fmt.Errorf("schedule %s: %w", s.ID, ErrNotFound)
	}

	s.NextRunTimeValue = nextRun
	return nil
}

func (r *ScheduleRepository) Delete(ctx context.Context, id string) error {
	query := `DELETE FROM schedule_config WHERE id = $1`

	tag, err := r.pool.Exec(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed to delete schedule %s: %w", id, err)
	}

	if tag.RowsAffected() == 0 {
		return fmt.Errorf("schedule %s: %w", id, ErrNotFound)
	}

	return nil
}

func (r *ScheduleRepository) List(ctx context.Context, filter ScheduleFilter) ([]*models.Schedule, error) {
	if filter.Limit <= 0 {
		filter.Limit = 50
	}

	query := `
		SELECT id, pipeline_id, cron_expression, timezone, status,
		       enabled, priority, max_concurrent_runs, tags, metadata,
		       next_run_time, last_run_time, created_by, created_at, updated_at
		FROM schedule_config
		WHERE ($1::text IS NULL OR pipeline_id = $1)
		  AND ($2::text IS NULL OR status = $2::schedule_status)
		  AND ($3::bool IS NULL OR enabled = $3)
		ORDER BY priority ASC, next_run_time ASC
		LIMIT $4 OFFSET $5`

	var pipelineFilter *string
	if filter.PipelineID != "" {
		pipelineFilter = &filter.PipelineID
	}

	var statusFilter *string
	if filter.Status != "" {
		s := string(filter.Status)
		statusFilter = &s
	}

	rows, err := r.pool.Query(ctx, query, pipelineFilter, statusFilter, filter.Enabled, filter.Limit, filter.Offset)
	if err != nil {
		return nil, fmt.Errorf("failed to list schedules: %w", err)
	}
	defer rows.Close()

	var schedules []*models.Schedule
	for rows.Next() {
		s := &models.Schedule{}
		if err := rows.Scan(
			&s.ID, &s.PipelineID, &s.CronExpression, &s.Timezone, &s.Status,
			&s.Enabled, &s.Priority, &s.MaxConcurrentRuns, &s.Tags, &s.Metadata,
			&s.NextRunTimeValue, &s.LastRunTime, &s.CreatedBy, &s.CreatedAt, &s.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan schedule row: %w", err)
		}
		schedules = append(schedules, s)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating schedule rows: %w", err)
	}

	if schedules == nil {
		schedules = []*models.Schedule{}
	}

	return schedules, nil
}

func (r *ScheduleRepository) GetDueSchedules(ctx context.Context, limit int) ([]*models.Schedule, error) {
	query := `
		SELECT id, pipeline_id, cron_expression, timezone, status,
		       enabled, priority, max_concurrent_runs, tags, metadata,
		       next_run_time, last_run_time, created_by, created_at, updated_at
		FROM schedule_config
		WHERE enabled = true
		  AND status = $1
		  AND next_run_time <= NOW()
		ORDER BY priority ASC, next_run_time ASC
		LIMIT $2
		FOR UPDATE SKIP LOCKED`

	rows, err := r.pool.Query(ctx, query, models.ScheduleStatusActive, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get due schedules: %w", err)
	}
	defer rows.Close()

	var schedules []*models.Schedule
	for rows.Next() {
		s := &models.Schedule{}
		if err := rows.Scan(
			&s.ID, &s.PipelineID, &s.CronExpression, &s.Timezone, &s.Status,
			&s.Enabled, &s.Priority, &s.MaxConcurrentRuns, &s.Tags, &s.Metadata,
			&s.NextRunTimeValue, &s.LastRunTime, &s.CreatedBy, &s.CreatedAt, &s.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan due schedule: %w", err)
		}
		schedules = append(schedules, s)
	}

	return schedules, rows.Err()
}

func (r *ScheduleRepository) UpdateNextRunTime(ctx context.Context, id string, nextRun time.Time) error {
	query := `
		UPDATE schedule_config SET
			last_run_time = CASE WHEN next_run_time < $2 THEN next_run_time ELSE last_run_time END,
			next_run_time = $2,
			updated_at = $3
		WHERE id = $1`

	tag, err := r.pool.Exec(ctx, query, id, nextRun, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("failed to update next run time for schedule %s: %w", id, err)
	}

	if tag.RowsAffected() == 0 {
		return fmt.Errorf("schedule %s: %w", id, ErrNotFound)
	}

	return nil
}

func (r *ScheduleRepository) ToggleEnabled(ctx context.Context, id string, enabled bool) error {
	query := `
		UPDATE schedule_config SET
			enabled = $2, updated_at = $3
		WHERE id = $1`

	tag, err := r.pool.Exec(ctx, query, id, enabled, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("failed to toggle schedule %s: %w", id, err)
	}

	if tag.RowsAffected() == 0 {
		return fmt.Errorf("schedule %s: %w", id, ErrNotFound)
	}

	return nil
}
