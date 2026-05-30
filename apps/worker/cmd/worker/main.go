package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"mediaflow/apps/worker/internal/config"
	"mediaflow/apps/worker/internal/database"
	"mediaflow/apps/worker/internal/storage"
	"mediaflow/apps/worker/internal/worker"
)

func main() {
	cfg := config.Load()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	startupCtx, startupCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer startupCancel()

	db, err := database.Open(startupCtx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("database connection failed", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	objectStorage, err := storage.NewMinIOStorage(
		cfg.MinIOEndpoint,
		cfg.MinIOAccessKey,
		cfg.MinIOSecretKey,
		cfg.MinIOUseSSL,
		cfg.MinIORawBucket,
		cfg.MinIOProcessedBucket,
		cfg.MinIOThumbnailBucket,
	)
	if err != nil {
		logger.Error("minio client setup failed", "error", err)
		os.Exit(1)
	}

	w, err := worker.New(cfg, logger, db, objectStorage)
	if err != nil {
		logger.Error("worker setup failed", "error", err)
		os.Exit(1)
	}
	defer w.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("worker starting", "env", cfg.AppEnv, "concurrency", cfg.WorkerConcurrency)
	if err := w.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("worker stopped with error", "error", err)
		os.Exit(1)
	}
	logger.Info("worker stopped")
}
