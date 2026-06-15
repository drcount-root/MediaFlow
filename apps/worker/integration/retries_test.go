//go:build integration

package integration

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	amqp "github.com/rabbitmq/amqp091-go"
	"mediaflow/apps/worker/internal/config"
	"mediaflow/apps/worker/internal/database"
	"mediaflow/apps/worker/internal/job"
	"mediaflow/apps/worker/internal/storage"
	"mediaflow/apps/worker/internal/worker"
)

// TestPermanentFailureDeadLettersAndFailsVideo feeds the worker a raw object that
// is not a real video. ffprobe rejects it (a permanent error), so the worker must
// not retry: it marks the video failed and parks the message in the DLQ.
func TestPermanentFailureDeadLettersAndFailsVideo(t *testing.T) {
	requireFFmpeg(t)

	ctx := context.Background()
	db := openDB(t)
	truncateAll(t, db)

	videoID := uuid.NewString()
	jobID := uuid.NewString()
	rawKey := "raw-videos/" + videoID + "/original.mp4"

	// A text blob masquerading as an MP4 — ffprobe cannot read it.
	client, err := minioClient()
	if err != nil {
		t.Fatalf("minio client: %v", err)
	}
	bogus := "this is definitely not a video"
	if _, err := client.PutObject(ctx, rawBucket, rawKey,
		strings.NewReader(bogus), int64(len(bogus)), minio.PutObjectOptions{ContentType: "video/mp4"}); err != nil {
		t.Fatalf("upload bogus raw: %v", err)
	}

	seedQueuedVideo(t, db, videoID, jobID, rawKey)
	w := newTestWorker(t, db)
	purgeQueues(t)

	publishJob(t, job.TranscodeJob{JobID: jobID, VideoID: videoID, RawBucket: rawBucket, RawObjectKey: rawKey, RequestedAt: time.Now().UTC()})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = w.Run(runCtx) }()

	status := waitForTerminalStatus(t, db, videoID, 60*time.Second)
	if status != "failed" {
		t.Fatalf("expected video failed, got %q", status)
	}

	// Attempts must be 1 — a permanent failure is never retried.
	var attempts int
	if err := db.QueryRowContext(ctx, `SELECT attempts FROM video_jobs WHERE id = $1`, jobID).Scan(&attempts); err != nil {
		t.Fatalf("read attempts: %v", err)
	}
	if attempts != 1 {
		t.Fatalf("permanent failure must not retry; expected attempts=1, got %d", attempts)
	}

	msg := getMessage(t, job.DLQRoutingKey, 10*time.Second)
	reason, _ := msg.Headers["x-failure-reason"].(string)
	if reason == "" || !strings.Contains(reason, "permanent failure") {
		t.Fatalf("expected DLQ message tagged as permanent, got reason=%q", reason)
	}
}

// TestPoisonMessageDeadLettered publishes a body the worker cannot even parse. It
// must go straight to the DLQ and be acked, and the consumer must keep working
// (proven by a follow-up valid message that still gets processed).
func TestPoisonMessageDeadLettered(t *testing.T) {
	requireFFmpeg(t)

	ctx := context.Background()
	db := openDB(t)
	truncateAll(t, db)

	w := newTestWorker(t, db)
	purgeQueues(t)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = w.Run(runCtx) }()

	publishRaw(t, job.TranscodeRoutingKey, []byte("{not valid json"))

	msg := getMessage(t, job.DLQRoutingKey, 10*time.Second)
	if reason, _ := msg.Headers["x-failure-reason"].(string); !strings.Contains(reason, "invalid job payload") {
		t.Fatalf("expected DLQ message tagged invalid payload, got reason=%q", reason)
	}

	// The consumer is not wedged: a normal job seeded afterwards still completes.
	videoID := uuid.NewString()
	jobID := uuid.NewString()
	rawKey := "raw-videos/" + videoID + "/original.mp4"
	client, err := minioClient()
	if err != nil {
		t.Fatalf("minio client: %v", err)
	}
	if _, err := client.FPutObject(ctx, rawBucket, rawKey, generateFixtureMP4(t), minio.PutObjectOptions{ContentType: "video/mp4"}); err != nil {
		t.Fatalf("upload fixture: %v", err)
	}
	seedQueuedVideo(t, db, videoID, jobID, rawKey)
	publishJob(t, job.TranscodeJob{JobID: jobID, VideoID: videoID, RawBucket: rawBucket, RawObjectKey: rawKey, RequestedAt: time.Now().UTC()})

	if status := waitForTerminalStatus(t, db, videoID, 120*time.Second); status != "ready" {
		t.Fatalf("consumer wedged after poison message; follow-up video status=%q", status)
	}
}

// TestRetryQueueDeadLettersBackToMain proves the retry topology: a message landed
// in the retry queue with a short TTL is dead-lettered back to the main transcode
// queue once the TTL expires.
func TestRetryQueueDeadLettersBackToMain(t *testing.T) {
	db := openDB(t)
	newTestWorker(t, db) // declares the retry/DLQ topology
	purgeQueues(t)

	publishRawWithTTL(t, job.RetryRoutingKey, []byte(`{"jobId":"ttl-probe"}`), "800")

	// Nothing consumes the retry queue; after the TTL the broker routes it back.
	msg := getMessage(t, job.TranscodeRoutingKey, 10*time.Second)
	if string(msg.Body) != `{"jobId":"ttl-probe"}` {
		t.Fatalf("unexpected body dead-lettered back to main: %s", msg.Body)
	}
}

func TestMarkQueuedForRetryReleasesClaim(t *testing.T) {
	db := openDB(t)
	truncateAll(t, db)
	ctx := context.Background()

	videoID := uuid.NewString()
	jobID := uuid.NewString()
	// A job mid-flight: claimed, attempt 1, lease in the future.
	seedProcessing(t, db, videoID, jobID, 1, "worker-x", time.Now().Add(time.Minute))

	repo := database.NewRepository(db)
	if err := repo.MarkQueuedForRetry(ctx, jobID, videoID, errors.New("temporary glitch"), 1, 60*time.Second); err != nil {
		t.Fatalf("MarkQueuedForRetry: %v", err)
	}

	var (
		jobStatus   string
		attempts    int
		claimedBy   sql.NullString
		lease       sql.NullTime
		videoStatus string
	)
	if err := db.QueryRowContext(ctx, `
		SELECT j.status, j.attempts, j.claimed_by, j.lease_expires_at, v.status
		FROM video_jobs j JOIN videos v ON v.id = j.video_id WHERE j.id = $1
	`, jobID).Scan(&jobStatus, &attempts, &claimedBy, &lease, &videoStatus); err != nil {
		t.Fatalf("read job/video: %v", err)
	}
	if jobStatus != "queued" {
		t.Fatalf("expected job re-queued for retry, got %q", jobStatus)
	}
	if attempts != 1 {
		t.Fatalf("retry must not change attempts (the re-claim does), got %d", attempts)
	}
	if claimedBy.Valid || lease.Valid {
		t.Fatalf("expected claim/lease cleared, got claimed_by=%v lease=%v", claimedBy, lease)
	}
	if videoStatus != "processing" {
		t.Fatalf("video should stay processing during retry, got %q", videoStatus)
	}

	var events int
	if err := db.QueryRowContext(ctx, `
		SELECT count(*) FROM video_events WHERE video_id = $1 AND event_type = 'video.job.retry_scheduled'
	`, videoID).Scan(&events); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if events != 1 {
		t.Fatalf("expected 1 retry_scheduled event, got %d", events)
	}
}

// --- helpers ---

func newTestWorker(t *testing.T, db *sql.DB) *worker.Worker {
	t.Helper()
	cfg := config.Config{
		DatabaseURL:          infra.databaseURL,
		RabbitMQURL:          infra.rabbitURL,
		MinIOEndpoint:        infra.minioEndpoint,
		MinIOAccessKey:       infra.minioAccessKey,
		MinIOSecretKey:       infra.minioSecretKey,
		MinIORawBucket:       rawBucket,
		MinIOProcessedBucket: processedBucket,
		MinIOThumbnailBucket: thumbnailBucket,
		WorkerConcurrency:    1,
		WorkDir:              t.TempDir(),
		FFmpegPath:           "ffmpeg",
		FFprobePath:          "ffprobe",
		WorkerID:             "test-worker",
		JobLeaseDuration:     2 * time.Minute,
		JobMaxAttempts:       3,
		HeartbeatInterval:    30 * time.Second,
		ReaperInterval:       30 * time.Second,
		RetryBaseDelay:       30 * time.Second,
	}
	objStore, err := storage.NewMinIOStorage(
		cfg.MinIOEndpoint, cfg.MinIOAccessKey, cfg.MinIOSecretKey, cfg.MinIOUseSSL,
		cfg.MinIORawBucket, cfg.MinIOProcessedBucket, cfg.MinIOThumbnailBucket,
	)
	if err != nil {
		t.Fatalf("new minio storage: %v", err)
	}
	w, err := worker.New(cfg, slog.New(slog.NewTextHandler(os.Stderr, nil)), db, objStore)
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w
}

func purgeQueues(t *testing.T) {
	t.Helper()
	conn, ch := dialChannel(t)
	defer conn.Close()
	defer ch.Close()
	for _, q := range []string{job.TranscodeRoutingKey, job.RetryRoutingKey, job.DLQRoutingKey} {
		if _, err := ch.QueuePurge(q, false); err != nil {
			t.Fatalf("purge %s: %v", q, err)
		}
	}
}

func dialChannel(t *testing.T) (*amqp.Connection, *amqp.Channel) {
	t.Helper()
	conn, err := amqp.Dial(infra.rabbitURL)
	if err != nil {
		t.Fatalf("amqp dial: %v", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		t.Fatalf("amqp channel: %v", err)
	}
	return conn, ch
}

func publishRaw(t *testing.T, routingKey string, body []byte) {
	t.Helper()
	conn, ch := dialChannel(t)
	defer conn.Close()
	defer ch.Close()
	if err := ch.PublishWithContext(context.Background(), "mediaflow.video", routingKey, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent, ContentType: "application/json", Body: body,
	}); err != nil {
		t.Fatalf("publish raw: %v", err)
	}
}

func publishRawWithTTL(t *testing.T, routingKey string, body []byte, ttlMillis string) {
	t.Helper()
	conn, ch := dialChannel(t)
	defer conn.Close()
	defer ch.Close()
	if err := ch.PublishWithContext(context.Background(), "mediaflow.video", routingKey, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent, ContentType: "application/json", Expiration: ttlMillis, Body: body,
	}); err != nil {
		t.Fatalf("publish raw with ttl: %v", err)
	}
}

func getMessage(t *testing.T, queue string, timeout time.Duration) amqp.Delivery {
	t.Helper()
	conn, ch := dialChannel(t)
	defer conn.Close()
	defer ch.Close()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		msg, ok, err := ch.Get(queue, true)
		if err != nil {
			t.Fatalf("get from %s: %v", queue, err)
		}
		if ok {
			return msg
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("no message arrived on %s within %s", queue, timeout)
	return amqp.Delivery{}
}
