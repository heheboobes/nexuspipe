package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"nexuspipe/internal/models"
)

var (
	ErrNotFound               = errors.New("not found")
	ErrConcurrentModification = errors.New("concurrent modification detected")
)

type TaskFilter struct {
	PipelineID string
	Status     models.TaskStatus
	Limit      int
	Offset     int
}

type TaskBatchResult struct {
	Total     int
	Succeeded int
	Failed    int
	Errors    []error
}

type TaskRepository struct {
	pool *pgxpool.Pool
}

func NewTaskRepository(pool *pgxpool.Pool) *TaskRepository {
	return &TaskRepository{pool: pool}
}

func (r *TaskRepository) Create(ctx context.Context, t *models.TaskExecution) error {
	query := `
		INSERT INTO task_executions (
			id, pipeline_id, run_id, status, input_data, output_data,
			error_message, retry_count, max_retries, scheduled_at,
			started_at, completed_at, worker_id, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
			$11, $12, $13, $14, $15
		) RETURNING created_at, updated_at`

	now := time.Now().UTC()
	if t.CreatedAt.IsZero() {
		t.CreatedAt = now
	}
	if t.UpdatedAt.IsZero() {
		t.UpdatedAt = now
	}
	if t.RetryCount == 0 {
		t.RetryCount = 0
	}
	if t.MaxRetries == 0 {
		t.MaxRetries = 3
	}
	if t.Status == "" {
		t.Status = models.TaskStatusPending
	}

	err := r.pool.QueryRow(ctx, query,
		t.ID, t.PipelineID, t.RunID, t.Status, t.InputData,
		t.OutputData, t.ErrorMessage, t.RetryCount, t.MaxRetries,
		t.ScheduledAt, t.StartedAt, t.CompletedAt, t.WorkerID,
		t.CreatedAt, t.UpdatedAt,
	).Scan(&t.CreatedAt, &t.UpdatedAt)

	if err != nil {
		return fmt.Errorf("failed to create task: %w", err)
	}

	return nil
}

func (r *TaskRepository) BatchCreate(ctx context.Context, tasks []*models.TaskExecution) (*TaskBatchResult, error) {
	result := &TaskBatchResult{Total: len(tasks)}

	for _, t := range tasks {
		if err := r.Create(ctx, t); err != nil {
			result.Failed++
			result.Errors = append(result.Errors, fmt.Errorf("task %s: %w", t.ID, err))
		} else {
			result.Succeeded++
		}
	}

	return result, nil
}

func (r *TaskRepository) GetByID(ctx context.Context, id string) (*models.TaskExecution, error) {
	query := `
		SELECT id, pipeline_id, run_id, status, input_data, output_data,
		       error_message, retry_count, max_retries, scheduled_at,
		       started_at, completed_at, worker_id, created_at, updated_at
		FROM task_executions
		WHERE id = $1`

	t := &models.TaskExecution{}
	err := r.pool.QueryRow(ctx, query, id).Scan(
		&t.ID, &t.PipelineID, &t.RunID, &t.Status, &t.InputData,
		&t.OutputData, &t.ErrorMessage, &t.RetryCount, &t.MaxRetries,
		&t.ScheduledAt, &t.StartedAt, &t.CompletedAt, &t.WorkerID,
		&t.CreatedAt, &t.UpdatedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("task %s: %w", id, ErrNotFound)
		}
		return nil, fmt.Errorf("failed to get task %s: %w", id, err)
	}

	return t, nil
}

func (r *TaskRepository) GetByPipeline(ctx context.Context, pipelineID string, filter TaskFilter) ([]*models.TaskExecution, error) {
	if filter.Limit <= 0 {
		filter.Limit = 50
	}

	query := `
		SELECT id, pipeline_id, run_id, status, input_data, output_data,
		       error_message, retry_count, max_retries, scheduled_at,
		       started_at, completed_at, worker_id, created_at, updated_at
		FROM task_executions
		WHERE pipeline_id = $1
		  AND ($2::text IS NULL OR status = $2::task_status)
		ORDER BY created_at DESC
		LIMIT $3 OFFSET $4`

	var statusFilter *string
	if filter.Status != "" {
		s := string(filter.Status)
		statusFilter = &s
	}

	rows, err := r.pool.Query(ctx, query, pipelineID, statusFilter, filter.Limit, filter.Offset)
	if err != nil {
		return nil, fmt.Errorf("failed to get tasks for pipeline %s: %w", pipelineID, err)
	}
	defer rows.Close()

	var tasks []*models.TaskExecution
	for rows.Next() {
		t := &models.TaskExecution{}
		if err := rows.Scan(
			&t.ID, &t.PipelineID, &t.RunID, &t.Status, &t.InputData,
			&t.OutputData, &t.ErrorMessage, &t.RetryCount, &t.MaxRetries,
			&t.ScheduledAt, &t.StartedAt, &t.CompletedAt, &t.WorkerID,
			&t.CreatedAt, &t.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan task row: %w", err)
		}
		tasks = append(tasks, t)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating task rows: %w", err)
	}

	if tasks == nil {
		tasks = []*models.TaskExecution{}
	}

	return tasks, nil
}

func (r *TaskRepository) UpdateStatus(ctx context.Context, id string, status models.TaskStatus, msg string) error {
	query := `
		UPDATE task_executions SET
			status = $2, error_message = $3, updated_at = $4
		WHERE id = $1`

	if status == models.TaskStatusRunning {
		query = `
			UPDATE task_executions SET
				status = $2, started_at = COALESCE(started_at, $4), updated_at = $4
			WHERE id = $1`
	}

	if status == models.TaskStatusCompleted || status == models.TaskStatusFailed {
		query = `
			UPDATE task_executions SET
				status = $2, error_message = $3,
				completed_at = $4, updated_at = $4
			WHERE id = $1`
	}

	now := time.Now().UTC()
	tag, err := r.pool.Exec(ctx, query, id, status, msg, now)
	if err != nil {
		return fmt.Errorf("failed to update task %s status: %w", id, err)
	}

	if tag.RowsAffected() == 0 {
		return fmt.Errorf("task %s: %w", id, ErrNotFound)
	}

	return nil
}

func (r *TaskRepository) IncrementRetry(ctx context.Context, id string, errMsg string) error {
	query := `
		UPDATE task_executions SET
			retry_count = retry_count + 1,
			status = CASE
				WHEN retry_count + 1 >= max_retries THEN $2
				ELSE $3
			END,
			error_message = $4,
			updated_at = $5
		WHERE id = $1
		RETURNING retry_count, status`

	var retryCount int
	var status models.TaskStatus
	err := r.pool.QueryRow(ctx, query, id,
		models.TaskStatusFailed,
		models.TaskStatusPending,
		errMsg,
		time.Now().UTC(),
	).Scan(&retryCount, &status)
	if err != nil {
		if err == pgx.ErrNoRows {
			return fmt.Errorf("task %s: %w", id, ErrNotFound)
		}
		return fmt.Errorf("failed to increment retry for task %s: %w", id, err)
	}

	return nil
}

func (r *TaskRepository) ListPending(ctx context.Context, limit int) ([]*models.TaskExecution, error) {
	query := `
		SELECT id, pipeline_id, run_id, status, input_data, output_data,
		       error_message, retry_count, max_retries, scheduled_at,
		       started_at, completed_at, worker_id, created_at, updated_at
		FROM task_executions
		WHERE status = $1
		  AND (scheduled_at IS NULL OR scheduled_at <= NOW())
		ORDER BY created_at ASC
		LIMIT $2
		FOR UPDATE SKIP LOCKED`

	rows, err := r.pool.Query(ctx, query, models.TaskStatusPending, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to list pending tasks: %w", err)
	}
	defer rows.Close()

	var tasks []*models.TaskExecution
	for rows.Next() {
		t := &models.TaskExecution{}
		if err := rows.Scan(
			&t.ID, &t.PipelineID, &t.RunID, &t.Status, &t.InputData,
			&t.OutputData, &t.ErrorMessage, &t.RetryCount, &t.MaxRetries,
			&t.ScheduledAt, &t.StartedAt, &t.CompletedAt, &t.WorkerID,
			&t.CreatedAt, &t.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan pending task: %w", err)
		}
		tasks = append(tasks, t)
	}

	return tasks, rows.Err()
}

func (r *TaskRepository) BulkUpdateStatus(ctx context.Context, ids []string, status models.TaskStatus, workerID string) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}

	query := `
		UPDATE task_executions SET
			status = $1, worker_id = $2, started_at = $3, updated_at = $3
		WHERE id = ANY($4)`

	tag, err := r.pool.Exec(ctx, query, status, workerID, time.Now().UTC(), ids)
	if err != nil {
		return 0, fmt.Errorf("failed to bulk update task status: %w", err)
	}

	return tag.RowsAffected(), nil
}
