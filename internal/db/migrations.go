package db

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/pgxpool"
)

type MigrationConfig struct {
	SourceURL      string
	DatabaseURL    string
	MigrationsPath string
	WithForce      bool
	ForceVersion   int
}

func DefaultMigrationConfig() MigrationConfig {
	return MigrationConfig{
		MigrationsPath: "file://migrations",
	}
}

type MigrationResult struct {
	Version   uint
	Dirty     bool
	AppliedAt time.Time
	Duration  time.Duration
}

type MigrationRunner struct {
	config MigrationConfig
	logger *slog.Logger
}

func NewMigrationRunner(cfg MigrationConfig, logger *slog.Logger) *MigrationRunner {
	if cfg.SourceURL == "" {
		cfg.SourceURL = cfg.MigrationsPath
	}
	return &MigrationRunner{config: cfg, logger: logger}
}

func (r *MigrationRunner) RunMigrations(ctx context.Context) (*MigrationResult, error) {
	start := time.Now()
	r.logger.InfoContext(ctx, "starting database migrations",
		slog.String("source", r.config.SourceURL),
	)

	dbURL := r.config.DatabaseURL
	if dbURL == "" {
		dbURL = r.resolveDatabaseURL()
	}

	m, err := migrate.New(r.config.SourceURL, dbURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create migrator: %w", err)
	}
	defer m.Close()

	if r.config.WithForce {
		r.logger.WarnContext(ctx, "force-applying migration version",
			slog.Int("version", r.config.ForceVersion),
		)
		if err := m.Force(r.config.ForceVersion); err != nil {
			return nil, fmt.Errorf("failed to force migration version: %w", err)
		}
	}

	if err := m.Up(); err != nil {
		if errors.Is(err, migrate.ErrNoChange) {
			r.logger.InfoContext(ctx, "no migrations to apply")
			return r.currentVersion(ctx, m, start)
		}
		return nil, fmt.Errorf("failed to run migrations: %w", err)
	}

	result, err := r.currentVersion(ctx, m, start)
	if err != nil {
		return nil, err
	}

	r.logger.InfoContext(ctx, "migrations completed successfully",
		slog.Uint64("version", uint64(result.Version)),
		slog.Duration("duration", result.Duration),
	)

	return result, nil
}

func (r *MigrationRunner) RollbackLast(ctx context.Context) (*MigrationResult, error) {
	start := time.Now()
	r.logger.InfoContext(ctx, "rolling back last migration")

	dbURL := r.config.DatabaseURL
	if dbURL == "" {
		dbURL = r.resolveDatabaseURL()
	}

	m, err := migrate.New(r.config.SourceURL, dbURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create migrator: %w", err)
	}
	defer m.Close()

	if err := m.Steps(-1); err != nil {
		if errors.Is(err, migrate.ErrNoChange) {
			r.logger.InfoContext(ctx, "no migrations to roll back")
			return r.currentVersion(ctx, m, start)
		}
		return nil, fmt.Errorf("failed to roll back migration: %w", err)
	}

	result, err := r.currentVersion(ctx, m, start)
	if err != nil {
		return nil, err
	}

	r.logger.InfoContext(ctx, "rollback completed",
		slog.Uint64("version", uint64(result.Version)),
		slog.Duration("duration", result.Duration),
	)

	return result, nil
}

func (r *MigrationRunner) MigrateTo(ctx context.Context, targetVersion uint) (*MigrationResult, error) {
	start := time.Now()
	r.logger.InfoContext(ctx, "migrating to specific version",
		slog.Uint64("target", uint64(targetVersion)),
	)

	dbURL := r.config.DatabaseURL
	if dbURL == "" {
		dbURL = r.resolveDatabaseURL()
	}

	m, err := migrate.New(r.config.SourceURL, dbURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create migrator: %w", err)
	}
	defer m.Close()

	if err := m.Migrate(targetVersion); err != nil {
		if errors.Is(err, migrate.ErrNoChange) {
			r.logger.InfoContext(ctx, "already at target version")
			return r.currentVersion(ctx, m, start)
		}
		return nil, fmt.Errorf("failed to migrate to version %d: %w", targetVersion, err)
	}

	return r.currentVersion(ctx, m, start)
}

func (r *MigrationRunner) Version(ctx context.Context) (uint, bool, error) {
	dbURL := r.config.DatabaseURL
	if dbURL == "" {
		dbURL = r.resolveDatabaseURL()
	}

	m, err := migrate.New(r.config.SourceURL, dbURL)
	if err != nil {
		return 0, false, fmt.Errorf("failed to create migrator: %w", err)
	}
	defer m.Close()

	version, dirty, err := m.Version()
	if err != nil {
		return 0, false, fmt.Errorf("failed to get migration version: %w", err)
	}

	return version, dirty, nil
}

func (r *MigrationRunner) ValidateMigrationFiles(pool *pgxpool.Pool) error {
	dbURL := r.config.DatabaseURL
	if dbURL == "" {
		connConfig := pool.Config().ConnConfig
		dbURL = connConfig.ConnString()
	}

	m, err := migrate.New(r.config.SourceURL, dbURL)
	if err != nil {
		return fmt.Errorf("failed to create migrator for validation: %w", err)
	}
	defer m.Close()

	return nil
}

func (r *MigrationRunner) currentVersion(_ context.Context, m *migrate.Migrate, start time.Time) (*MigrationResult, error) {
	version, dirty, err := m.Version()
	if err != nil && !errors.Is(err, migrate.ErrNilVersion) {
		return nil, fmt.Errorf("failed to get current migration version: %w", err)
	}

	return &MigrationResult{
		Version:   version,
		Dirty:     dirty,
		AppliedAt: time.Now(),
		Duration:  time.Since(start),
	}, nil
}

func (r *MigrationRunner) resolveDatabaseURL() string {
	if envURL := os.Getenv("DATABASE_URL"); envURL != "" {
		return envURL
	}

	if envHost := os.Getenv("DB_HOST"); envHost != "" {
		user := os.Getenv("DB_USER")
		pass := os.Getenv("DB_PASSWORD")
		port := os.Getenv("DB_PORT")
		name := os.Getenv("DB_NAME")

		if user == "" {
			user = "nexuspipe"
		}
		if port == "" {
			port = "5432"
		}
		if name == "" {
			name = "nexuspipe"
		}

		return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, pass, envHost, port, name)
	}

	return "postgres://nexuspipe:nexuspipe@localhost:5432/nexuspipe?sslmode=disable"
}

func (r *MigrationRunner) ListMigrations() ([]string, error) {
	cleanPath := r.config.MigrationsPath
	if len(cleanPath) > 7 && cleanPath[:7] == "file://" {
		cleanPath = cleanPath[7:]
	}

	entries, err := os.ReadDir(cleanPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read migrations directory %s: %w", cleanPath, err)
	}

	var migrations []string
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".sql" {
			migrations = append(migrations, entry.Name())
		}
	}

	return migrations, nil
}
