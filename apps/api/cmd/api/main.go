package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"mediaflow/apps/api/internal/config"
	"mediaflow/apps/api/internal/database"
	httpapi "mediaflow/apps/api/internal/http"
	"mediaflow/apps/api/internal/queue"
	"mediaflow/apps/api/internal/storage"
	"mediaflow/apps/api/internal/videos"
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

	publisher, err := queue.NewRabbitPublisher(cfg.RabbitMQURL)
	if err != nil {
		logger.Error("rabbitmq publisher setup failed", "error", err)
		os.Exit(1)
	}
	defer publisher.Close()

	repo := database.NewPostgresRepository(db)
	videoService := videos.NewService(repo, objectStorage, publisher, cfg.MinIORawBucket, cfg.MaxUploadBytes)

	router := httpapi.NewRouterWithVideos(cfg, videoService)
	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		logger.Info("api server starting", "addr", cfg.HTTPAddr, "env", cfg.AppEnv)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("api server failed", "error", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		logger.Error("api server shutdown failed", "error", err)
		os.Exit(1)
	}

	logger.Info("api server stopped")
}
