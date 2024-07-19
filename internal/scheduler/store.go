package scheduler

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"nexuspipe/internal/models"
	"nexuspipe/internal/repository"
)

type ScheduleStore interface {
	GetActiveSchedules(ctx context.Context) ([]*models.Schedule, error)
	GetDueSchedules(ctx context.Context, limit int) ([]*models.Schedule, error)
	GetSchedule(ctx context.Context, id string) (*models.Schedule, error)
	UpdateNextRunTime(ctx context.Context, id string, nextRun time.Time) error
	MarkScheduleFailed(ctx context.Context, id string, err error) error
}

type PostgresScheduleStore struct {
	repo  *repository.ScheduleRepository
	cache map[string]*cachedSchedule
	mu    sync.RWMutex
	ttl   time.Duration
}

type cachedSchedule struct {
	schedule *models.Schedule
	cachedAt time.Time
}

func NewPostgresScheduleStore(repo *repository.ScheduleRepository) *PostgresScheduleStore {
	return &PostgresScheduleStore{
		repo:  repo,
		cache: make(map[string]*cachedSchedule),
		ttl:   30 * time.Second,
	}
}

func (s *PostgresScheduleStore) GetActiveSchedules(ctx context.Context) ([]*models.Schedule, error) {
	schedules, err := s.repo.List(ctx, repository.ScheduleFilter{
		Status:  models.ScheduleStatusActive,
		Enabled: boolPtr(true),
		Limit:   1000,
	})
	if err != nil {
		return nil, fmt.Errorf("list active schedules: %w", err)
	}
	s.mu.Lock()
	for _, sch := range schedules {
		s.cache[sch.ID] = &cachedSchedule{
			schedule: sch,
			cachedAt: time.Now(),
		}
	}
	s.mu.Unlock()
	return schedules, nil
}

func (s *PostgresScheduleStore) GetDueSchedules(ctx context.Context, limit int) ([]*models.Schedule, error) {
	if limit <= 0 {
		limit = 50
	}
	schedules, err := s.repo.GetDueSchedules(ctx, limit)
	if err != nil {
		return nil, fmt.Errorf("get due schedules: %w", err)
	}
	return schedules, nil
}

func (s *PostgresScheduleStore) GetSchedule(ctx context.Context, id string) (*models.Schedule, error) {
	if cached := s.getFromCache(id); cached != nil {
		return cached, nil
	}
	sch, err := s.repo.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.cache[id] = &cachedSchedule{schedule: sch, cachedAt: time.Now()}
	s.mu.Unlock()
	return sch, nil
}

func (s *PostgresScheduleStore) UpdateNextRunTime(ctx context.Context, id string, nextRun time.Time) error {
	if err := s.repo.UpdateNextRunTime(ctx, id, nextRun); err != nil {
		return fmt.Errorf("update next run time: %w", err)
	}
	s.mu.Lock()
	if cached, ok := s.cache[id]; ok {
		cached.schedule.NextRunTimeValue = nextRun
		cached.cachedAt = time.Now()
	}
	s.mu.Unlock()
	return nil
}

func (s *PostgresScheduleStore) MarkScheduleFailed(ctx context.Context, id string, err error) error {
	sch, getErr := s.GetSchedule(ctx, id)
	if getErr != nil {
		return fmt.Errorf("get schedule for mark failed: %w", getErr)
	}
	sch.Status = models.ScheduleStatusFailed
	sch.Enabled = false
	if updateErr := s.repo.ToggleEnabled(ctx, id, false); updateErr != nil {
		return fmt.Errorf("disable failed schedule: %w", updateErr)
	}
	s.mu.Lock()
	delete(s.cache, id)
	s.mu.Unlock()
	log.Printf("schedule %s marked as failed: %v", id, err)
	return nil
}

func (s *PostgresScheduleStore) getFromCache(id string) *models.Schedule {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cached, ok := s.cache[id]
	if !ok {
		return nil
	}
	if time.Since(cached.cachedAt) > s.ttl {
		return nil
	}
	return cached.schedule
}

func (s *PostgresScheduleStore) InvalidateCache(id string) {
	s.mu.Lock()
	delete(s.cache, id)
	s.mu.Unlock()
}

func (s *PostgresScheduleStore) ClearCache() {
	s.mu.Lock()
	s.cache = make(map[string]*cachedSchedule)
	s.mu.Unlock()
}

func (s *PostgresScheduleStore) RefreshCache(ctx context.Context) error {
	schedules, err := s.repo.List(ctx, repository.ScheduleFilter{
		Status:  models.ScheduleStatusActive,
		Enabled: boolPtr(true),
		Limit:   1000,
	})
	if err != nil {
		return fmt.Errorf("refresh cache: %w", err)
	}
	s.mu.Lock()
	s.cache = make(map[string]*cachedSchedule, len(schedules))
	for _, sch := range schedules {
		s.cache[sch.ID] = &cachedSchedule{
			schedule: sch,
			cachedAt: time.Now(),
		}
	}
	s.mu.Unlock()
	return nil
}

func (s *PostgresScheduleStore) Stats(ctx context.Context) (*models.ScheduleStats, error) {
	schedules, err := s.repo.List(ctx, repository.ScheduleFilter{Limit: 10000})
	if err != nil {
		return nil, err
	}
	stats := &models.ScheduleStats{
		TotalSchedules: len(schedules),
		ByStatus:       make(map[models.ScheduleStatus]int),
	}
	for _, sch := range schedules {
		stats.ByStatus[sch.Status]++
		if sch.Status == models.ScheduleStatusActive && sch.Enabled {
			stats.ActiveSchedules++
		}
		if sch.Enabled && sch.ShouldRunNow() {
			stats.DueNow++
		}
	}
	return stats, nil
}

func boolPtr(b bool) *bool {
	return &b
}
