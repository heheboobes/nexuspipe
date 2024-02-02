package db

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/tracelog"
)

type PostgresConfig struct {
	Host              string
	Port              int
	User              string
	Password          string
	Database          string
	MaxConns          int32
	MinConns          int32
	MaxConnLifetime   time.Duration
	MaxConnIdleTime   time.Duration
	HealthCheckPeriod time.Duration
	LogLevel          tracelog.LogLevel
	ApplicationName   string
}

func DefaultPostgresConfig() PostgresConfig {
	return PostgresConfig{
		Host:              "localhost",
		Port:              5432,
		User:              "nexuspipe",
		Database:          "nexuspipe",
		MaxConns:          25,
		MinConns:          5,
		MaxConnLifetime:   30 * time.Minute,
		MaxConnIdleTime:   5 * time.Minute,
		HealthCheckPeriod: 30 * time.Second,
		LogLevel:          tracelog.LogLevelWarn,
		ApplicationName:   "nexuspipe",
	}
}

type PoolMetrics struct {
	mu              sync.RWMutex
	AcquireCount    int64
	AcquireDuration time.Duration
	AcquiredConns   int32
	IdleConns       int32
	MaxConns        int32
	TotalConns      int32
	lastHealthCheck time.Time
}

type PostgresPool struct {
	pool    *pgxpool.Pool
	config  PostgresConfig
	metrics *PoolMetrics
	logger  *slog.Logger
	done    chan struct{}
}

func NewPostgresPool(ctx context.Context, cfg PostgresConfig, logger *slog.Logger) (*PostgresPool, error) {
	connString := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?application_name=%s&sslmode=disable",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database, cfg.ApplicationName,
	)

	poolCfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse pool config: %w", err)
	}

	poolCfg.MaxConns = cfg.MaxConns
	poolCfg.MinConns = cfg.MinConns
	poolCfg.MaxConnLifetime = cfg.MaxConnLifetime
	poolCfg.MaxConnIdleTime = cfg.MaxConnIdleTime
	poolCfg.HealthCheckPeriod = cfg.HealthCheckPeriod

	tracer := &tracelog.TraceLog{
		Logger:   tracelog.NewLogger(logger, cfg.LogLevel),
		LogLevel: cfg.LogLevel,
	}
	poolCfg.ConnConfig.Tracer = tracer

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	logger.InfoContext(ctx, "postgres connection pool established",
		slog.String("host", cfg.Host),
		slog.Int("port", cfg.Port),
		slog.String("database", cfg.Database),
		slog.Int32("max_conns", cfg.MaxConns),
		slog.Int32("min_conns", cfg.MinConns),
	)

	p := &PostgresPool{
		pool:    pool,
		config:  cfg,
		metrics: &PoolMetrics{},
		logger:  logger,
		done:    make(chan struct{}),
	}

	go p.collectMetrics(ctx)

	return p, nil
}

func (p *PostgresPool) Pool() *pgxpool.Pool {
	return p.pool
}

func (p *PostgresPool) Acquire(ctx context.Context) (*pgxpool.Conn, error) {
	start := time.Now()
	conn, err := p.pool.Acquire(ctx)
	duration := time.Since(start)

	p.metrics.mu.Lock()
	p.metrics.AcquireCount++
	p.metrics.AcquireDuration += duration
	p.metrics.mu.Unlock()

	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	return conn, nil
}

func (p *PostgresPool) HealthCheck(ctx context.Context) error {
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("health check failed to acquire connection: %w", err)
	}
	defer conn.Release()

	var result int
	if err := conn.QueryRow(ctx, "SELECT 1").Scan(&result); err != nil {
		return fmt.Errorf("health check ping failed: %w", err)
	}

	p.metrics.mu.Lock()
	p.metrics.lastHealthCheck = time.Now()
	p.metrics.mu.Unlock()

	return nil
}

func (p *PostgresPool) Metrics() PoolMetrics {
	p.metrics.mu.RLock()
	defer p.metrics.mu.RUnlock()

	stats := p.pool.Stat()
	return PoolMetrics{
		AcquireCount:    p.metrics.AcquireCount,
		AcquireDuration: p.metrics.AcquireDuration,
		AcquiredConns:   stats.AcquiredConns(),
		IdleConns:       stats.IdleConns(),
		MaxConns:        stats.MaxConns(),
		TotalConns:      stats.TotalConns(),
		lastHealthCheck: p.metrics.lastHealthCheck,
	}
}

func (p *PostgresPool) collectMetrics(ctx context.Context) {
	ticker := time.NewTicker(p.config.HealthCheckPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			stats := p.pool.Stat()
			p.metrics.mu.Lock()
			p.metrics.AcquiredConns = stats.AcquiredConns()
			p.metrics.IdleConns = stats.IdleConns()
			p.metrics.TotalConns = stats.TotalConns()
			p.metrics.mu.Unlock()

			p.logger.DebugContext(ctx, "pool metrics",
				slog.Int32("total_conns", stats.TotalConns()),
				slog.Int32("acquired_conns", stats.AcquiredConns()),
				slog.Int32("idle_conns", stats.IdleConns()),
				slog.Int64("acquire_count", stats.AcquireCount()),
			)

		case <-p.done:
			return
		}
	}
}

func (p *PostgresPool) Ping(ctx context.Context) error {
	return p.pool.Ping(ctx)
}

func (p *PostgresPool) Close() {
	p.logger.Info("shutting down postgres connection pool")
	close(p.done)

	p.pool.Close()

	p.logger.Info("postgres connection pool closed")
}

func (p *PostgresPool) Shutdown(ctx context.Context) error {
	p.logger.Info("gracefully shutting down postgres pool")

	shutdownCh := make(chan struct{}, 1)
	go func() {
		p.Close()
		close(shutdownCh)
	}()

	select {
	case <-shutdownCh:
		return nil
	case <-ctx.Done():
		p.pool.Close()
		return ctx.Err()
	}
}

func (p *PostgresPool) WithTransaction(ctx context.Context, fn func(ctx context.Context, tx pgx.Tx) error) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				p.logger.ErrorContext(ctx, "transaction rollback failed",
					slog.Any("rollback_error", rbErr),
					slog.Any("original_error", err),
				)
			}
		}
	}()

	if err = fn(ctx, tx); err != nil {
		return err
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
