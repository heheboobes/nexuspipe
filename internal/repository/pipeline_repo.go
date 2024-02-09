package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"nexuspipe/internal/models"
)

type PipelineFilter struct {
	Status    models.PipelineStatus
	CreatedBy string
	Limit     int
	Offset    int
}

type PipelineRepository struct {
	pool *pgxpool.Pool
}

func NewPipelineRepository(pool *pgxpool.Pool) *PipelineRepository {
	return &PipelineRepository{pool: pool}
}

func (r *PipelineRepository) Create(ctx context.Context, p *models.Pipeline) error {
	query := `
		INSERT INTO pipeline_config (
			id, name, description, status, config_json, version,
			created_by, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9
		) RETURNING created_at, updated_at`

	now := time.Now().UTC()
	if p.CreatedAt.IsZero() {
		p.CreatedAt = now
	}
	if p.UpdatedAt.IsZero() {
		p.UpdatedAt = now
	}
	if p.Version == 0 {
		p.Version = 1
	}

	err := r.pool.QueryRow(ctx, query,
		p.ID, p.Name, p.Description, p.Status, p.ConfigJSON,
		p.Version, p.CreatedBy, p.CreatedAt, p.UpdatedAt,
	).Scan(&p.CreatedAt, &p.UpdatedAt)

	if err != nil {
		return fmt.Errorf("failed to create pipeline: %w", err)
	}

	return nil
}

func (r *PipelineRepository) Get(ctx context.Context, id string) (*models.Pipeline, error) {
	query := `
		SELECT id, name, description, status, config_json, version,
		       created_by, created_at, updated_at
		FROM pipeline_config
		WHERE id = $1`

	p := &models.Pipeline{}
	err := r.pool.QueryRow(ctx, query, id).Scan(
		&p.ID, &p.Name, &p.Description, &p.Status, &p.ConfigJSON,
		&p.Version, &p.CreatedBy, &p.CreatedAt, &p.UpdatedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("pipeline %s: %w", id, ErrNotFound)
		}
		return nil, fmt.Errorf("failed to get pipeline %s: %w", id, err)
	}

	return p, nil
}

func (r *PipelineRepository) GetByID(ctx context.Context, id string) (*models.Pipeline, error) {
	return r.Get(ctx, id)
}

func (r *PipelineRepository) Update(ctx context.Context, p *models.Pipeline) error {
	query := `
		UPDATE pipeline_config SET
			name = $2, description = $3, status = $4,
			config_json = $5, version = version + 1, updated_at = $6
		WHERE id = $1 AND version = $7
		RETURNING version, updated_at`

	p.UpdatedAt = time.Now().UTC()

	result := r.pool.QueryRow(ctx, query,
		p.ID, p.Name, p.Description, p.Status, p.ConfigJSON,
		p.UpdatedAt, p.Version,
	)

	err := result.Scan(&p.Version, &p.UpdatedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return fmt.Errorf("pipeline %s: %w", p.ID, ErrConcurrentModification)
		}
		return fmt.Errorf("failed to update pipeline %s: %w", p.ID, err)
	}

	return nil
}

func (r *PipelineRepository) Delete(ctx context.Context, id string) error {
	query := `DELETE FROM pipeline_config WHERE id = $1`

	tag, err := r.pool.Exec(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed to delete pipeline %s: %w", id, err)
	}

	if tag.RowsAffected() == 0 {
		return fmt.Errorf("pipeline %s: %w", id, ErrNotFound)
	}

	return nil
}

func (r *PipelineRepository) SoftDelete(ctx context.Context, id string) error {
	query := `
		UPDATE pipeline_config SET
			status = $2, updated_at = $3
		WHERE id = $1`

	_, err := r.pool.Exec(ctx, query, id, models.PipelineStatusDeleted, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("failed to soft-delete pipeline %s: %w", id, err)
	}

	return nil
}

func (r *PipelineRepository) List(ctx context.Context, filter PipelineFilter) ([]*models.Pipeline, error) {
	if filter.Limit <= 0 {
		filter.Limit = 50
	}
	if filter.Limit > 500 {
		filter.Limit = 500
	}

	query := `
		SELECT id, name, description, status, config_json, version,
		       created_by, created_at, updated_at
		FROM pipeline_config
		WHERE ($1::text IS NULL OR status = $1::pipeline_status)
		  AND ($2::text IS NULL OR created_by = $2)
		ORDER BY created_at DESC
		LIMIT $3 OFFSET $4`

	var statusFilter *string
	if filter.Status != "" {
		s := string(filter.Status)
		statusFilter = &s
	}

	var createdByFilter *string
	if filter.CreatedBy != "" {
		createdByFilter = &filter.CreatedBy
	}

	rows, err := r.pool.Query(ctx, query, statusFilter, createdByFilter, filter.Limit, filter.Offset)
	if err != nil {
		return nil, fmt.Errorf("failed to list pipelines: %w", err)
	}
	defer rows.Close()

	var pipelines []*models.Pipeline
	for rows.Next() {
		p := &models.Pipeline{}
		if err := rows.Scan(
			&p.ID, &p.Name, &p.Description, &p.Status, &p.ConfigJSON,
			&p.Version, &p.CreatedBy, &p.CreatedAt, &p.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan pipeline row: %w", err)
		}
		pipelines = append(pipelines, p)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating pipeline rows: %w", err)
	}

	if pipelines == nil {
		pipelines = []*models.Pipeline{}
	}

	return pipelines, nil
}

func (r *PipelineRepository) Count(ctx context.Context, status models.PipelineStatus) (int, error) {
	query := `SELECT COUNT(*) FROM pipeline_config WHERE status = $1`

	var count int
	if err := r.pool.QueryRow(ctx, query, status).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count pipelines: %w", err)
	}

	return count, nil
}

func (r *PipelineRepository) UpdateStatus(ctx context.Context, id string, status models.PipelineStatus) error {
	query := `
		UPDATE pipeline_config SET
			status = $2, updated_at = $3
		WHERE id = $1`

	tag, err := r.pool.Exec(ctx, query, id, status, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("failed to update pipeline %s status: %w", id, err)
	}

	if tag.RowsAffected() == 0 {
		return fmt.Errorf("pipeline %s: %w", id, ErrNotFound)
	}

	return nil
}
