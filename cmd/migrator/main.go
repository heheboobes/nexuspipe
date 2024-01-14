package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/golang-migrate/migrate/v4"
	migratepgx "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/spf13/cobra"

	"github.com/heheboobes/nexuspipe/internal/config"
	"github.com/heheboobes/nexuspipe/internal/logger"
)

var cfgFile string

func main() {
	rootCmd := &cobra.Command{
		Use:   "nexuspipe-migrator",
		Short: "NexusPipe database migration tool",
		Long: `Database migration tool for NexusPipe.

Supports up, down, rollback, and create operations for managing
PostgreSQL database schema migrations.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "config.yaml", "path to config file")

	rootCmd.AddCommand(&cobra.Command{
		Use:   "up",
		Short: "Run all pending migrations",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runMigration("up")
		},
	})

	rootCmd.AddCommand(&cobra.Command{
		Use:   "down",
		Short: "Rollback the last migration",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runMigration("down")
		},
	})

	rootCmd.AddCommand(&cobra.Command{
		Use:   "rollback",
		Short: "Rollback N migrations (default 1)",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runRollback()
		},
	})

	rootCmd.AddCommand(&cobra.Command{
		Use:   "reset",
		Short: "Rollback all migrations and re-apply",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runReset()
		},
	})

	rootCmd.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print current migration version",
		RunE: func(cmd *cobra.Command, args []string) error {
			return printVersion()
		},
	})

	rootCmd.AddCommand(&cobra.Command{
		Use:   "create",
		Short: "Create a new migration file",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return createMigration(args[0])
		},
	})

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("error: %v", err)
	}
}

func getMigrator() (*migrate.Migrate, error) {
	cfg, err := config.LoadConfig(cfgFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	_, err = logger.NewLogger(logger.Config{
		Level:  cfg.Log.Level,
		Format: cfg.Log.Format,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize logger: %w", err)
	}

	dsn := cfg.Database.DSN
	if dsn == "" {
		dsn = fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
			cfg.Database.User,
			cfg.Database.Password,
			cfg.Database.Host,
			cfg.Database.Port,
			cfg.Database.DBName,
			cfg.Database.SSLMode,
		)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	driver, err := migratepgx.WithInstance(db, &migratepgx.Config{
		MigrationsTable: "schema_migrations",
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create migration driver: %w", err)
	}

	migrationsPath := cfg.Database.MigrationsPath
	if !filepath.IsAbs(migrationsPath) {
		wd, _ := os.Getwd()
		migrationsPath = filepath.Join(wd, migrationsPath)
	}

	sourceURL := fmt.Sprintf("file://%s", filepath.ToSlash(migrationsPath))

	m, err := migrate.NewWithInstance("file", sourceURL, "pgx", driver)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create migrator: %w", err)
	}

	return m, nil
}

func runMigration(direction string) error {
	m, err := getMigrator()
	if err != nil {
		return err
	}
	defer m.Close()

	var runErr error
	switch direction {
	case "up":
		runErr = m.Up()
	case "down":
		runErr = m.Steps(-1)
	}

	if runErr != nil && runErr != migrate.ErrNoChange {
		return fmt.Errorf("migration failed: %w", runErr)
	}

	version, dirty, verErr := m.Version()
	if verErr != nil && verErr != migrate.ErrNilVersion {
		return fmt.Errorf("failed to get version: %w", verErr)
	}

	if runErr == migrate.ErrNoChange {
		fmt.Println("No pending migrations.")
	} else {
		fmt.Printf("Migration %s completed successfully.\n", direction)
	}

	fmt.Printf("Current version: %d (dirty: %v)\n", version, dirty)
	return nil
}

func runRollback() error {
	m, err := getMigrator()
	if err != nil {
		return err
	}
	defer m.Close()

	steps := 1
	if err := m.Steps(-steps); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("rollback failed: %w", err)
	}

	version, dirty, _ := m.Version()
	fmt.Printf("Rolled back %d migration(s). Current version: %d (dirty: %v)\n", steps, version, dirty)
	return nil
}

func runReset() error {
	m, err := getMigrator()
	if err != nil {
		return err
	}
	defer m.Close()

	if err := m.Down(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("reset down failed: %w", err)
	}

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("reset up failed: %w", err)
	}

	version, dirty, _ := m.Version()
	fmt.Printf("Migration reset complete. Current version: %d (dirty: %v)\n", version, dirty)
	return nil
}

func printVersion() error {
	m, err := getMigrator()
	if err != nil {
		return err
	}
	defer m.Close()

	version, dirty, err := m.Version()
	if err == migrate.ErrNilVersion {
		fmt.Println("No migrations have been applied yet.")
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to get version: %w", err)
	}

	fmt.Printf("Current migration version: %d (dirty: %v)\n", version, dirty)
	return nil
}

func createMigration(name string) error {
	cfg, err := config.LoadConfig(cfgFile)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	migrationsPath := cfg.Database.MigrationsPath
	if !filepath.IsAbs(migrationsPath) {
		wd, _ := os.Getwd()
		migrationsPath = filepath.Join(wd, migrationsPath)
	}

	if err := os.MkdirAll(migrationsPath, 0755); err != nil {
		return fmt.Errorf("failed to create migrations directory: %w", err)
	}

	upPath := filepath.Join(migrationsPath, fmt.Sprintf("*.up.sql"))
	_ = upPath

	ts := time.Now().Unix()
	upFile := filepath.Join(migrationsPath, fmt.Sprintf("%d_%s.up.sql", ts, name))
	downFile := filepath.Join(migrationsPath, fmt.Sprintf("%d_%s.down.sql", ts, name))

	upContent := fmt.Sprintf("-- Migration: %s\n-- Up\n\n", name)
	downContent := fmt.Sprintf("-- Migration: %s\n-- Down\n\n", name)

	if err := os.WriteFile(upFile, []byte(upContent), 0644); err != nil {
		return fmt.Errorf("failed to create up file: %w", err)
	}
	if err := os.WriteFile(downFile, []byte(downContent), 0644); err != nil {
		return fmt.Errorf("failed to create down file: %w", err)
	}

	fmt.Printf("Created migration:\n  %s\n  %s\n", upFile, downFile)
	return nil
}
