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
	renditionQueue = "video.rendition"
	finalizeQueue  = "video.finalize"
	retryQueue     = "video.transcode.retry"
	dlqQueue       = "video.transcode.dlq"
)

const (
	tagPlan      = "mediaflow-worker-plan"
	tagRendition = "mediaflow-worker-rendition"
	tagFinalize  = "mediaflow-worker-finalize"
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

	if err := declareTopology(channel); err != nil {
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

// declareTopology declares the fan-out queues (plan/rendition/finalize), the
// transcode retry queue (which dead-letters back to the main transcode queue
// once a message's TTL expires), the terminal DLQ, and their bindings on the
// shared mediaflow.video exchange.
func declareTopology(channel *amqp.Channel) error {
	if err := channel.ExchangeDeclare("mediaflow.video", amqp.ExchangeDirect, true, false, false, false, nil); err != nil {
		return err
	}

	for _, q := range []struct{ name, key string }{
		{transcodeQueue, transcodeQueue},
		{renditionQueue, job.RenditionRoutingKey},
		{finalizeQueue, job.FinalizeRoutingKey},
	} {
		if _, err := channel.QueueDeclare(q.name, true, false, false, false, nil); err != nil {
			return err
		}
		if err := channel.QueueBind(q.name, q.key, "mediaflow.video", false, nil); err != nil {
			return err
		}
	}

	// Retry queue: no consumer. A message published here waits out its per-message
	// TTL, then the broker dead-letters it back to the main transcode queue.
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
	planCh, err := w.channel.Consume(transcodeQueue, tagPlan, false, false, false, false, nil)
	if err != nil {
		return err
	}
	renditionCh, err := w.channel.Consume(renditionQueue, tagRendition, false, false, false, false, nil)
	if err != nil {
		return err
	}
	finalizeCh, err := w.channel.Consume(finalizeQueue, tagFinalize, false, false, false, false, nil)
	if err != nil {
		return err
	}

	// jobCtx governs in-flight processing and is independent of ctx: a shutdown
	// signal lets the current job finish. The watcher below cancels jobCtx only if
	// the grace period elapses (a hung/long job) — then the reaper recovers it.
	jobCtx, cancelJobs := context.WithCancel(context.Background())
	defer cancelJobs()

	stopped := make(chan struct{})
	go w.watchShutdown(ctx, cancelJobs, stopped)

	w.logger.Info("worker consuming", "queues", strings.Join([]string{transcodeQueue, renditionQueue, finalizeQueue}, ","))
	shutdownCh := ctx.Done()
	for {
		select {
		case <-shutdownCh:
			// Observe the shutdown signal once. The watcher cancels the AMQP
			// consumers, which close the delivery channels after any prefetched
			// message is handled; stop selecting here so we don't busy-loop.
			shutdownCh = nil
		case delivery, ok := <-planCh:
			if !ok {
				planCh = nil
				break
			}
			w.handleDelivery(jobCtx, delivery, w.handlePlan)
		case delivery, ok := <-renditionCh:
			if !ok {
				renditionCh = nil
				break
			}
			w.handleDelivery(jobCtx, delivery, w.handleRendition)
		case delivery, ok := <-finalizeCh:
			if !ok {
				finalizeCh = nil
				break
			}
			w.handleDelivery(jobCtx, delivery, w.handleFinalize)
		}

		if planCh == nil && renditionCh == nil && finalizeCh == nil {
			close(stopped)
			return nil
		}
	}
}

// watchShutdown waits for a shutdown signal, stops the worker pulling new work,
// and gives the in-flight job a grace period before aborting it.
func (w *Worker) watchShutdown(ctx context.Context, abortJob context.CancelFunc, stopped <-chan struct{}) {
	select {
	case <-ctx.Done():
	case <-stopped:
		return
	}

	w.logger.Info("shutdown signalled; draining in-flight job", "grace", w.cfg.ShutdownGrace.String())
	for _, tag := range []string{tagPlan, tagRendition, tagFinalize} {
		if err := w.channel.Cancel(tag, false); err != nil {
			w.logger.Warn("consumer cancel failed", "tag", tag, "error", err)
		}
	}

	select {
	case <-time.After(w.cfg.ShutdownGrace):
		w.logger.Warn("shutdown grace exceeded; aborting in-flight job (reaper will recover it)")
		abortJob()
	case <-stopped:
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

// handleDelivery parses a message and runs the stage handler. A body we cannot
// even parse is poison: it is parked in the DLQ so it never loops.
func (w *Worker) handleDelivery(ctx context.Context, delivery amqp.Delivery, handler func(context.Context, amqp.Delivery) error) {
	if err := handler(ctx, delivery); err != nil {
		w.logger.Error("delivery handling failed", "error", err)
	}
}

// handlePlan runs the plan stage and applies the M5 retry/DLQ policy on failure
// (the plan job has a retry queue; renditions and finalize recover via the
// reaper — see handleChildFailure).
func (w *Worker) handlePlan(ctx context.Context, delivery amqp.Delivery) error {
	var payload job.TranscodeJob
	if err := json.Unmarshal(delivery.Body, &payload); err != nil {
		return w.deadLetterPoison(ctx, delivery, err)
	}

	if procErr := w.ProcessPlan(ctx, payload); procErr != nil {
		w.handleFailure(ctx, delivery, payload, procErr)
		return nil
	}
	return delivery.Ack(false)
}

func (w *Worker) handleRendition(ctx context.Context, delivery amqp.Delivery) error {
	var payload job.RenditionJob
	if err := json.Unmarshal(delivery.Body, &payload); err != nil {
		return w.deadLetterPoison(ctx, delivery, err)
	}

	if procErr := w.ProcessRendition(ctx, payload); procErr != nil {
		return w.handleChildFailure(delivery, payload.JobID, payload.VideoID, "rendition", procErr)
	}
	return delivery.Ack(false)
}

func (w *Worker) handleFinalize(ctx context.Context, delivery amqp.Delivery) error {
	var payload job.FinalizeJob
	if err := json.Unmarshal(delivery.Body, &payload); err != nil {
		return w.deadLetterPoison(ctx, delivery, err)
	}

	if procErr := w.ProcessFinalize(ctx, payload); procErr != nil {
		return w.handleChildFailure(delivery, payload.JobID, payload.VideoID, "finalize", procErr)
	}
	return delivery.Ack(false)
}

// deadLetterPoison parks an unparseable message in the DLQ and acks the original
// so the consumer keeps moving.
func (w *Worker) deadLetterPoison(ctx context.Context, delivery amqp.Delivery, cause error) error {
	w.logger.Error("invalid job payload, dead-lettering", "error", cause)
	if pubErr := w.publishDLQ(ctx, delivery.Body, "invalid job payload: "+cause.Error()); pubErr != nil {
		w.logger.Error("dead-letter publish failed", "error", pubErr)
		return delivery.Nack(false, true) // keep it; try again rather than drop
	}
	return delivery.Ack(false)
}

// handleChildFailure handles a rendition or finalize error. Unlike the plan
// stage, child stages do not have their own retry queue (M7 slice A): the job
// row is left `processing` with its lease, and the reaper requeues it (below max
// attempts) or fails the video (at max). Dropping the delivery (Nack, no
// requeue) avoids a hot loop; the reaper drives recovery on the lease timeout.
func (w *Worker) handleChildFailure(delivery amqp.Delivery, jobID, videoID, stage string, procErr error) error {
	w.logger.Error("child stage failed; leaving for reaper",
		"stage", stage, "jobId", jobID, "videoId", videoID, "error", procErr)
	return delivery.Nack(false, false)
}

// handleFailure decides what to do with a plan job that failed: schedule a
// backed-off retry (transient, below max attempts) or dead-letter it and mark it
// failed (permanent error, or attempts exhausted). Use a background context for
// the DB and broker writes so a cancelled ctx (shutdown) still records the
// outcome.
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

// ProcessPlan claims the plan job, probes the source, makes the thumbnail, and
// fans out one rendition job per target quality (via the outbox). It does not
// transcode — that is the rendition stage's job.
func (w *Worker) ProcessPlan(ctx context.Context, payload job.TranscodeJob) error {
	claimed, err := w.repo.ClaimJob(ctx, payload.JobID, payload.VideoID, w.cfg.WorkerID, w.cfg.JobLeaseDuration)
	if err != nil {
		return err
	}
	if !claimed {
		w.logger.Info("plan job skipped", "jobId", payload.JobID, "videoId", payload.VideoID)
		return nil
	}

	hbCtx, stopHeartbeat := context.WithCancel(ctx)
	defer stopHeartbeat()
	go w.heartbeat(hbCtx, payload.JobID)

	workDir := filepath.Join(w.cfg.WorkDir, payload.JobID)
	inputPath := filepath.Join(workDir, "input.mp4")
	thumbnailPath := filepath.Join(workDir, "thumbnail.jpg")
	defer os.RemoveAll(workDir)

	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return err
	}

	w.logger.Info("planner downloading raw video", "videoId", payload.VideoID, "objectKey", payload.RawObjectKey)
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
	thumbnailKey := "thumbnails/" + payload.VideoID + "/default.jpg"
	if err := w.storage.UploadThumbnail(ctx, thumbnailKey, thumbnailPath); err != nil {
		return err
	}
	if err := w.repo.SaveThumbnail(ctx, payload.VideoID, thumbnailKey); err != nil {
		return err
	}

	specs := processor.PlanRenditions(probe.Height)
	w.logger.Info("planner fanning out renditions", "videoId", payload.VideoID, "count", len(specs))
	if err := w.repo.FanOutRenditions(ctx, payload.JobID, payload.VideoID, payload.RawBucket, payload.RawObjectKey, specs); err != nil {
		if database.IsPlanClaimLost(err) {
			w.logger.Info("plan claim lost before fan-out; another worker owns it", "jobId", payload.JobID)
			return nil
		}
		return err
	}
	return nil
}

// ProcessRendition transcodes exactly one quality and records the variant,
// atomically decrementing the plan's pending counter. The reduce step (and the
// finalize hand-off when this is the last rendition) happens in CompleteRendition.
func (w *Worker) ProcessRendition(ctx context.Context, payload job.RenditionJob) error {
	claimed, err := w.repo.ClaimChildJob(ctx, payload.JobID, payload.VideoID, w.cfg.WorkerID, w.cfg.JobLeaseDuration)
	if err != nil {
		return err
	}
	if !claimed {
		w.logger.Info("rendition job skipped", "jobId", payload.JobID, "videoId", payload.VideoID, "quality", payload.Spec.Quality)
		return nil
	}

	hbCtx, stopHeartbeat := context.WithCancel(ctx)
	defer stopHeartbeat()
	go w.heartbeat(hbCtx, payload.JobID)

	workDir := filepath.Join(w.cfg.WorkDir, payload.JobID)
	inputPath := filepath.Join(workDir, "input.mp4")
	outDir := filepath.Join(workDir, "out")
	defer os.RemoveAll(workDir)

	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return err
	}

	w.logger.Info("rendition downloading raw video", "videoId", payload.VideoID, "quality", payload.Spec.Quality)
	if err := w.storage.DownloadRaw(ctx, payload.RawObjectKey, inputPath); err != nil {
		return err
	}

	// Re-probe locally for the audio flag so a rendition is self-contained from
	// just the raw key + spec (the reaper can rebuild its message without probe data).
	probe, err := w.processor.Probe(ctx, inputPath)
	if err != nil {
		return err
	}

	variant, err := w.processor.GenerateRendition(ctx, inputPath, outDir, payload.Spec, probe.HasAudio)
	if err != nil {
		return err
	}

	baseKey := "processed-videos/" + payload.VideoID + "/" + payload.Spec.Quality
	if err := w.uploadDir(ctx, baseKey, outDir); err != nil {
		return err
	}
	variant.PlaylistKey = baseKey + "/index.m3u8"

	last, finalizeJobID, err := w.repo.CompleteRendition(ctx, payload.JobID, payload.ParentJobID, payload.VideoID, variant)
	if err != nil {
		return err
	}
	if last {
		w.logger.Info("last rendition done; finalize enqueued", "videoId", payload.VideoID, "finalizeJobId", finalizeJobID)
	}
	return nil
}

// ProcessFinalize assembles master.m3u8 from the recorded variants, uploads it,
// and marks the video ready.
func (w *Worker) ProcessFinalize(ctx context.Context, payload job.FinalizeJob) error {
	claimed, err := w.repo.ClaimChildJob(ctx, payload.JobID, payload.VideoID, w.cfg.WorkerID, w.cfg.JobLeaseDuration)
	if err != nil {
		return err
	}
	if !claimed {
		w.logger.Info("finalize job skipped", "jobId", payload.JobID, "videoId", payload.VideoID)
		return nil
	}

	hbCtx, stopHeartbeat := context.WithCancel(ctx)
	defer stopHeartbeat()
	go w.heartbeat(hbCtx, payload.JobID)

	variants, err := w.repo.ListVariants(ctx, payload.VideoID)
	if err != nil {
		return err
	}
	if len(variants) == 0 {
		return fmt.Errorf("no variants to finalize for video %s", payload.VideoID)
	}

	masterKey := "processed-videos/" + payload.VideoID + "/master.m3u8"
	master := processor.BuildMasterPlaylist(variants)
	if err := w.storage.UploadProcessedBytes(ctx, masterKey, master, processor.ContentType(masterKey)); err != nil {
		return err
	}

	return w.repo.CompleteFinalize(ctx, payload.JobID, payload.VideoID, masterKey)
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

// uploadDir uploads every file under dir to the processed bucket, keyed by
// baseKey + the file's path relative to dir.
func (w *Worker) uploadDir(ctx context.Context, baseKey, dir string) error {
	return filepath.WalkDir(dir, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		objectKey := baseKey + "/" + filepath.ToSlash(relative)
		return w.storage.UploadProcessedFile(ctx, objectKey, path, processor.ContentType(path))
	})
}
