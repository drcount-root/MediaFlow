package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"mediaflow/apps/worker/internal/config"
	"mediaflow/apps/worker/internal/database"
	"mediaflow/apps/worker/internal/job"
	"mediaflow/apps/worker/internal/processor"
	"mediaflow/apps/worker/internal/storage"
)

const transcodeQueue = "video.transcode"

type Worker struct {
	cfg       config.Config
	logger    *slog.Logger
	repo      *database.Repository
	storage   *storage.MinIOStorage
	processor processor.FFmpegProcessor
	conn      *amqp.Connection
	channel   *amqp.Channel
}

func New(cfg config.Config, logger *slog.Logger, db *sql.DB, objectStorage *storage.MinIOStorage) (*Worker, error) {
	conn, err := amqp.Dial(cfg.RabbitMQURL)
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	if err := channel.ExchangeDeclare("mediaflow.video", amqp.ExchangeDirect, true, false, false, false, nil); err != nil {
		channel.Close()
		conn.Close()
		return nil, err
	}
	if _, err := channel.QueueDeclare(transcodeQueue, true, false, false, false, nil); err != nil {
		channel.Close()
		conn.Close()
		return nil, err
	}
	if err := channel.QueueBind(transcodeQueue, transcodeQueue, "mediaflow.video", false, nil); err != nil {
		channel.Close()
		conn.Close()
		return nil, err
	}
	if err := channel.Qos(cfg.WorkerConcurrency, 0, false); err != nil {
		channel.Close()
		conn.Close()
		return nil, err
	}

	return &Worker{
		cfg:     cfg,
		logger:  logger,
		repo:    database.NewRepository(db),
		storage: objectStorage,
		processor: processor.FFmpegProcessor{
			FFmpegPath:  cfg.FFmpegPath,
			FFprobePath: cfg.FFprobePath,
		},
		conn:    conn,
		channel: channel,
	}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	deliveries, err := w.channel.Consume(transcodeQueue, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	w.logger.Info("worker consuming", "queue", transcodeQueue)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case delivery, ok := <-deliveries:
			if !ok {
				return nil
			}
			w.handleDelivery(ctx, delivery)
		}
	}
}

func (w *Worker) Close() error {
	if w.channel != nil {
		_ = w.channel.Close()
	}
	if w.conn != nil {
		return w.conn.Close()
	}
	return nil
}

func (w *Worker) handleDelivery(ctx context.Context, delivery amqp.Delivery) {
	var payload job.TranscodeJob
	if err := json.Unmarshal(delivery.Body, &payload); err != nil {
		w.logger.Error("invalid job payload", "error", err)
		_ = delivery.Nack(false, false)
		return
	}

	if err := w.Process(ctx, payload); err != nil {
		w.logger.Error("job failed", "jobId", payload.JobID, "videoId", payload.VideoID, "error", err)
		_ = w.repo.FailJob(context.Background(), payload.JobID, payload.VideoID, err)
		_ = delivery.Nack(false, false)
		return
	}

	_ = delivery.Ack(false)
}

func (w *Worker) Process(ctx context.Context, payload job.TranscodeJob) error {
	claimed, err := w.repo.ClaimJob(ctx, payload.JobID, payload.VideoID, w.cfg.WorkerID, w.cfg.JobLeaseDuration)
	if err != nil {
		return err
	}
	if !claimed {
		w.logger.Info("job skipped", "jobId", payload.JobID, "videoId", payload.VideoID)
		return nil
	}

	// Keep the lease alive while FFmpeg runs so the reaper does not reclaim a job
	// that is making progress. The heartbeat stops when processing returns.
	hbCtx, stopHeartbeat := context.WithCancel(ctx)
	defer stopHeartbeat()
	go w.heartbeat(hbCtx, payload.JobID)

	workDir := filepath.Join(w.cfg.WorkDir, payload.JobID)
	inputPath := filepath.Join(workDir, "input.mp4")
	thumbnailPath := filepath.Join(workDir, "thumbnail.jpg")
	hlsDir := filepath.Join(workDir, "hls")
	defer os.RemoveAll(workDir)

	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return err
	}

	w.logger.Info("downloading raw video", "videoId", payload.VideoID, "objectKey", payload.RawObjectKey)
	if err := w.storage.DownloadRaw(ctx, payload.RawObjectKey, inputPath); err != nil {
		return err
	}

	probe, err := w.processor.Probe(ctx, inputPath)
	if err != nil {
		return err
	}
	if err := w.repo.SaveProbe(ctx, payload.VideoID, probe); err != nil {
		return err
	}

	if err := w.processor.GenerateThumbnail(ctx, inputPath, thumbnailPath); err != nil {
		return err
	}

	variants, err := w.processor.GenerateHLS(ctx, inputPath, hlsDir, probe)
	if err != nil {
		return err
	}

	baseKey := "processed-videos/" + payload.VideoID
	if err := w.uploadHLS(ctx, baseKey, hlsDir, variants); err != nil {
		return err
	}

	thumbnailKey := "thumbnails/" + payload.VideoID + "/default.jpg"
	if err := w.storage.UploadThumbnail(ctx, thumbnailKey, thumbnailPath); err != nil {
		return err
	}

	for idx := range variants {
		variants[idx].PlaylistKey = baseKey + "/" + variants[idx].Quality + "/index.m3u8"
	}

	return w.repo.CompleteJob(ctx, payload.JobID, payload.VideoID, baseKey+"/master.m3u8", thumbnailKey, variants)
}

func (w *Worker) heartbeat(ctx context.Context, jobID string) {
	ticker := time.NewTicker(w.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.repo.Heartbeat(ctx, jobID, w.cfg.WorkerID, w.cfg.JobLeaseDuration); err != nil && ctx.Err() == nil {
				w.logger.Warn("lease heartbeat failed", "jobId", jobID, "error", err)
			}
		}
	}
}

func (w *Worker) uploadHLS(ctx context.Context, baseKey, hlsDir string, variants []job.Variant) error {
	err := filepath.WalkDir(hlsDir, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}

		relative, err := filepath.Rel(hlsDir, path)
		if err != nil {
			return err
		}
		objectKey := baseKey + "/" + filepath.ToSlash(relative)
		return w.storage.UploadProcessedFile(ctx, objectKey, path, processor.ContentType(path))
	})
	if err != nil {
		return err
	}

	for _, variant := range variants {
		if !strings.HasSuffix(variant.PlaylistKey, "index.m3u8") {
			return fmt.Errorf("variant %s missing playlist", variant.Quality)
		}
	}

	return nil
}
