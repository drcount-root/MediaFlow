package main

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"mediaflow/apps/api/internal/config"
	"mediaflow/apps/api/internal/database"
)

func main() {
	cfg := config.Load()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	migrationsDir := getenv("MIGRATIONS_DIR", "../../infrastructure/migrations")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := database.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("database connection failed", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	files, err := migrationFiles(migrationsDir)
	if err != nil {
		logger.Error("read migrations failed", "error", err)
		os.Exit(1)
	}

	if err := ensureMigrationTable(ctx, db); err != nil {
		logger.Error("ensure migration table failed", "error", err)
		os.Exit(1)
	}

	applied := 0
	for _, file := range files {
		version := filepath.Base(file)
		alreadyApplied, err := isApplied(ctx, db, version)
		if err != nil {
			logger.Error("check migration failed", "migration", version, "error", err)
			os.Exit(1)
		}
		if alreadyApplied {
			logger.Info("migration already applied", "migration", version)
			continue
		}

		if err := applyMigration(ctx, db, version, file); err != nil {
			logger.Error("apply migration failed", "migration", version, "error", err)
			os.Exit(1)
		}

		applied++
		logger.Info("migration applied", "migration", version)
	}

	logger.Info("migrations complete", "applied", applied)
}

func migrationFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var files []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		files = append(files, filepath.Join(dir, entry.Name()))
	}

	sort.Strings(files)
	return files, nil
}

func ensureMigrationTable(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`)
	return err
}

func isApplied(ctx context.Context, db *sql.DB, version string) (bool, error) {
	var exists bool
	err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM schema_migrations WHERE version = $1
		)
	`, version).Scan(&exists)
	return exists, err
}

func applyMigration(ctx context.Context, db *sql.DB, version, path string) error {
	contents, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, string(contents)); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, version); err != nil {
		return err
	}

	return tx.Commit()
}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
