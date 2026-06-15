package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"mediaflow/apps/worker/internal/config"
	"mediaflow/apps/worker/internal/database"
	"mediaflow/apps/worker/internal/job"
	"mediaflow/apps/worker/internal/processor"
	"mediaflow/apps/worker/internal/storage"
)

const (
	transcodeQueue = "video.transcode"
	retryQueue     = "video.transcode.retry"
	dlqQueue       = "video.transcode.dlq"
)

type Worker struct {
	cfg        config.Config
	logger     *slog.Logger
	repo       *database.Repository
	storage    *storage.MinIOStorage
	processor  processor.FFmpegProcessor
	conn       *amqp.Connection
	channel    *amqp.Channel
	pubChannel *amqp.Channel
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

	// Retry queue: no consumer. A message published here waits out its per-message
	// TTL, then the broker dead-letters it back to the main transcode queue.
	if err := declareRetryAndDLQ(channel); err != nil {
		channel.Close()
		conn.Close()
		return nil, err
	}

	if err := channel.Qos(cfg.WorkerConcurrency, 0, false); err != nil {
		channel.Close()
		conn.Close()
		return nil, err
	}

	// Dedicated channel for republishing to the retry/DLQ queues, with publisher
	// confirms so a retry is durably enqueued before we mark the job re-claimable.
	// Kept separate from the consume channel.
	pubChannel, err := conn.Channel()
	if err != nil {
		channel.Close()
		conn.Close()
		return nil, err
	}
	if err := pubChannel.Confirm(false); err != nil {
		pubChannel.Close()
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
		conn:       conn,
		channel:    channel,
		pubChannel: pubChannel,
	}, nil
}

// declareRetryAndDLQ declares the retry queue (which dead-letters back to the
// main transcode queue once a message's TTL expires) and the terminal DLQ, plus
// their bindings on the shared mediaflow.video exchange.
func declareRetryAndDLQ(channel *amqp.Channel) error {
	if _, err := channel.QueueDeclare(retryQueue, true, false, false, false, amqp.Table{
		"x-dead-letter-exchange":    "mediaflow.video",
		"x-dead-letter-routing-key": transcodeQueue,
	}); err != nil {
		return err
	}
	if err := channel.QueueBind(retryQueue, job.RetryRoutingKey, "mediaflow.video", false, nil); err != nil {
		return err
	}
	if _, err := channel.QueueDeclare(dlqQueue, true, false, false, false, nil); err != nil {
		return err
	}
	return channel.QueueBind(dlqQueue, job.DLQRoutingKey, "mediaflow.video", false, nil)
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
	if w.pubChannel != nil {
		_ = w.pubChannel.Close()
	}
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
		// A message we cannot even parse is poison: park it in the DLQ so it never
		// loops, and ack the original so the consumer keeps moving.
		w.logger.Error("invalid job payload, dead-lettering", "error", err)
		if pubErr := w.publishDLQ(ctx, delivery.Body, "invalid job payload: "+err.Error()); pubErr != nil {
			w.logger.Error("dead-letter publish failed", "error", pubErr)
			_ = delivery.Nack(false, true) // keep it; try again rather than drop
			return
		}
		_ = delivery.Ack(false)
		return
	}

	procErr := w.Process(ctx, payload)
	if procErr == nil {
		_ = delivery.Ack(false)
		return
	}

	w.handleFailure(ctx, delivery, payload, procErr)
}

// handleFailure decides what to do with a job that failed: schedule a backed-off
// retry (transient, below max attempts) or dead-letter it and mark it failed
// (permanent error, or attempts exhausted). Use a background context for the DB
// and broker writes so a cancelled ctx (shutdown) still records the outcome.
func (w *Worker) handleFailure(ctx context.Context, delivery amqp.Delivery, payload job.TranscodeJob, procErr error) {
	bg := context.Background()
	attempts, err := w.repo.JobAttempts(bg, payload.JobID)
	if err != nil {
		// Can't read state to decide — requeue the delivery and let the reaper or a
		// later attempt sort it out rather than guessing.
		w.logger.Error("could not read attempts after failure", "jobId", payload.JobID, "error", err)
		_ = delivery.Nack(false, true)
		return
	}

	if retry, delay := classifyFailure(procErr, attempts, w.cfg.JobMaxAttempts, w.cfg.RetryBaseDelay); retry {
		w.logger.Warn("scheduling retry", "jobId", payload.JobID, "videoId", payload.VideoID,
			"attempts", attempts, "delay", delay.String(), "error", procErr)
		// Publish-first: the retry message is durably enqueued (with a confirm)
		// before we release the DB claim. If anything below fails, the job is still
		// `processing` with a lease, so the reaper remains the backstop.
		if err := w.publishRetry(bg, delivery.Body, delay); err != nil {
			w.logger.Error("retry publish failed", "jobId", payload.JobID, "error", err)
			_ = delivery.Nack(false, true)
			return
		}
		if err := w.repo.MarkQueuedForRetry(bg, payload.JobID, payload.VideoID, procErr, attempts, delay); err != nil {
			w.logger.Error("mark-for-retry failed", "jobId", payload.JobID, "error", err)
		}
		_ = delivery.Ack(false)
		return
	}

	// Terminal: permanent error or out of attempts.
	reason := terminalReason(procErr, attempts, w.cfg.JobMaxAttempts)
	w.logger.Error("job failed permanently", "jobId", payload.JobID, "videoId", payload.VideoID,
		"attempts", attempts, "reason", reason)
	if err := w.repo.FailJob(bg, payload.JobID, payload.VideoID, procErr); err != nil {
		w.logger.Error("fail-job failed", "jobId", payload.JobID, "error", err)
	}
	if err := w.publishDLQ(bg, delivery.Body, reason); err != nil {
		w.logger.Error("dead-letter publish failed", "jobId", payload.JobID, "error", err)
	}
	_ = delivery.Ack(false)
}

// classifyFailure decides whether a failed job should be retried. Permanent
// errors and exhausted attempts are never retried; otherwise the backoff is
// RetryBaseDelay * 2^attempts (attempts is the number that just failed).
func classifyFailure(err error, attempts, maxAttempts int, base time.Duration) (retry bool, delay time.Duration) {
	if job.IsPermanent(err) || attempts >= maxAttempts {
		return false, 0
	}
	if attempts < 1 {
		attempts = 1
	}
	return true, base * time.Duration(1<<uint(attempts))
}

func terminalReason(err error, attempts, maxAttempts int) string {
	if job.IsPermanent(err) {
		return "permanent failure: " + err.Error()
	}
	return fmt.Sprintf("attempts exhausted (%d/%d): %s", attempts, maxAttempts, err.Error())
}

// publishRetry republishes the original message body to the retry queue with a
// per-message TTL, blocking until the broker confirms it.
func (w *Worker) publishRetry(ctx context.Context, body []byte, delay time.Duration) error {
	return w.confirmedPublish(ctx, job.RetryRoutingKey, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "application/json",
		Timestamp:    time.Now().UTC(),
		Expiration:   strconv.FormatInt(delay.Milliseconds(), 10),
		Body:         body,
	})
}

// publishDLQ parks a poison or exhausted message in the DLQ, recording the
// reason in a header for whoever inspects the queue later.
func (w *Worker) publishDLQ(ctx context.Context, body []byte, reason string) error {
	return w.confirmedPublish(ctx, job.DLQRoutingKey, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "application/json",
		Timestamp:    time.Now().UTC(),
		Headers:      amqp.Table{"x-failure-reason": reason},
		Body:         body,
	})
}

func (w *Worker) confirmedPublish(ctx context.Context, routingKey string, msg amqp.Publishing) error {
	confirm, err := w.pubChannel.PublishWithDeferredConfirmWithContext(ctx, "mediaflow.video", routingKey, false, false, msg)
	if err != nil {
		return err
	}
	acked, err := confirm.WaitContext(ctx)
	if err != nil {
		return err
	}
	if !acked {
		return fmt.Errorf("publish to %s not confirmed", routingKey)
	}
	return nil
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
