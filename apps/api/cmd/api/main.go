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
	"mediaflow/apps/api/internal/outbox"
	"mediaflow/apps/api/internal/queue"
	"mediaflow/apps/api/internal/storage"
	"mediaflow/apps/api/internal/uploads"
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
	videoService := videos.NewService(repo, objectStorage, cfg.MinIORawBucket, cfg.MaxUploadBytes)
	uploadService := uploads.NewService(repo, objectStorage, cfg.MinIORawBucket, cfg.MaxUploadBytes, cfg.UploadSessionTTL, cfg.UploadPartURLTTL)

	// The outbox relay is what actually publishes transcode jobs; the request
	// path only writes the outbox row. Run it for the lifetime of the process.
	relay := outbox.NewRelay(db, publisher, logger, cfg.OutboxPollInterval, cfg.OutboxBatchSize)
	relayCtx, relayCancel := context.WithCancel(context.Background())
	relayDone := make(chan struct{})
	go func() {
		relay.Run(relayCtx)
		close(relayDone)
	}()

	// The sweeper expires abandoned upload sessions and aborts their orphaned
	// multipart uploads so staged parts don't accumulate in object storage.
	sweeper := uploads.NewSweeper(repo, objectStorage, logger, cfg.UploadSweepInterval, cfg.UploadSweepBatchSize)
	sweepCtx, sweepCancel := context.WithCancel(context.Background())
	sweepDone := make(chan struct{})
	go func() {
		sweeper.Run(sweepCtx)
		close(sweepDone)
	}()

	router := httpapi.NewRouterWithServices(cfg, videoService, uploadService)
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

	relayCancel()
	<-relayDone

	sweepCancel()
	<-sweepDone

	logger.Info("api server stopped")
}
