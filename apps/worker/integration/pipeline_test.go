//go:build integration

package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	amqp "github.com/rabbitmq/amqp091-go"
	"mediaflow/apps/worker/internal/config"
	"mediaflow/apps/worker/internal/job"
	"mediaflow/apps/worker/internal/storage"
	"mediaflow/apps/worker/internal/worker"
)

// TestPipelineProcessesUploadToReady drives the full upload→store→queue→process
// flow against real dependencies: a generated MP4 is stored in MinIO, a queued
// video is seeded, a transcode job is published, and the worker consumes it and
// produces HLS output, leaving the video `ready` with variants in storage.
func TestPipelineProcessesUploadToReady(t *testing.T) {
	requireFFmpeg(t)

	ctx := context.Background()
	db := openDB(t)
	truncateAll(t, db)

	videoID := uuid.NewString()
	jobID := uuid.NewString()
	rawKey := "raw-videos/" + videoID + "/original.mp4"

	// 1. Generate a short fixture clip and upload it as the raw object.
	fixture := generateFixtureMP4(t)
	client, err := minioClient()
	if err != nil {
		t.Fatalf("minio client: %v", err)
	}
	if _, err := client.FPutObject(ctx, rawBucket, rawKey, fixture, minio.PutObjectOptions{ContentType: "video/mp4"}); err != nil {
		t.Fatalf("upload raw fixture: %v", err)
	}

	// 2. Seed the queued video + job rows the API would have written.
	seedQueuedVideo(t, db, videoID, jobID, rawKey)

	// 3. Build the worker (declares the exchange/queue) and publish the job.
	cfg := config.Config{
		DatabaseURL:          infra.databaseURL,
		RabbitMQURL:          infra.rabbitURL,
		MinIOEndpoint:        infra.minioEndpoint,
		MinIOAccessKey:       infra.minioAccessKey,
		MinIOSecretKey:       infra.minioSecretKey,
		MinIOUseSSL:          false,
		MinIORawBucket:       rawBucket,
		MinIOProcessedBucket: processedBucket,
		MinIOThumbnailBucket: thumbnailBucket,
		WorkerConcurrency:    1,
		WorkDir:              t.TempDir(),
		FFmpegPath:           "ffmpeg",
		FFprobePath:          "ffprobe",
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

	publishJob(t, job.TranscodeJob{
		JobID:        jobID,
		VideoID:      videoID,
		RawBucket:    rawBucket,
		RawObjectKey: rawKey,
		RequestedAt:  time.Now().UTC(),
	})

	// 4. Run the worker until the video reaches a terminal state.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = w.Run(runCtx) }()

	status := waitForTerminalStatus(t, db, videoID, 120*time.Second)
	cancel()
	if status != "ready" {
		t.Fatalf("expected video status ready, got %q (error_message=%s)", status, videoErrorMessage(t, db, videoID))
	}

	// 5. Assert the produced artifacts.
	var hlsMasterKey, thumbnailKey string
	if err := db.QueryRowContext(ctx,
		`SELECT hls_master_key, thumbnail_key FROM videos WHERE id = $1`, videoID).
		Scan(&hlsMasterKey, &thumbnailKey); err != nil {
		t.Fatalf("read ready video: %v", err)
	}

	var variantCount int
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM video_variants WHERE video_id = $1`, videoID).Scan(&variantCount); err != nil {
		t.Fatalf("count variants: %v", err)
	}
	if variantCount == 0 {
		t.Fatal("expected at least one variant row")
	}

	if _, err := client.StatObject(ctx, processedBucket, hlsMasterKey, minio.StatObjectOptions{}); err != nil {
		t.Fatalf("master playlist %q missing from processed bucket: %v", hlsMasterKey, err)
	}
	if _, err := client.StatObject(ctx, thumbnailBucket, thumbnailKey, minio.StatObjectOptions{}); err != nil {
		t.Fatalf("thumbnail %q missing from thumbnail bucket: %v", thumbnailKey, err)
	}

	var jobStatus string
	if err := db.QueryRowContext(ctx,
		`SELECT status FROM video_jobs WHERE id = $1`, jobID).Scan(&jobStatus); err != nil {
		t.Fatalf("read job status: %v", err)
	}
	if jobStatus != "completed" {
		t.Fatalf("expected job completed, got %q", jobStatus)
	}
}

func requireFFmpeg(t *testing.T) {
	t.Helper()
	for _, bin := range []string{"ffmpeg", "ffprobe"} {
		if _, err := exec.LookPath(bin); err != nil {
			t.Skipf("%s not found on PATH: %v", bin, err)
		}
	}
}

// generateFixtureMP4 renders a tiny 360p clip with tone audio. Media files are
// never committed; they are produced on demand in a temp dir.
func generateFixtureMP4(t *testing.T) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "fixture.mp4")
	cmd := exec.Command("ffmpeg",
		"-y",
		"-f", "lavfi", "-i", "testsrc=duration=2:size=640x360:rate=15",
		"-f", "lavfi", "-i", "sine=frequency=1000:duration=2",
		"-c:v", "libx264", "-pix_fmt", "yuv420p",
		"-c:a", "aac",
		"-shortest",
		out,
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generate fixture mp4: %v\n%s", err, string(output))
	}
	return out
}

// seedQueuedVideo writes the queued video + job rows that the API's
// CreateQueuedVideo would have committed before publishing the job.
func seedQueuedVideo(t *testing.T, db *sql.DB, videoID, jobID, rawKey string) {
	t.Helper()
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO videos (id, title, status, raw_object_key, original_filename, content_type, size_bytes)
		VALUES ($1, $2, 'queued', $3, 'fixture.mp4', 'video/mp4', 0)
	`, videoID, "Pipeline Fixture", rawKey); err != nil {
		t.Fatalf("seed video: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO video_jobs (id, video_id, job_type, status)
		VALUES ($1, $2, 'transcode', 'queued')
	`, jobID, videoID); err != nil {
		t.Fatalf("seed job: %v", err)
	}
}

func publishJob(t *testing.T, payload job.TranscodeJob) {
	t.Helper()

	conn, err := amqp.Dial(infra.rabbitURL)
	if err != nil {
		t.Fatalf("amqp dial: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("amqp channel: %v", err)
	}
	defer ch.Close()

	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal job: %v", err)
	}
	if err := ch.PublishWithContext(context.Background(), "mediaflow.video", "video.transcode", false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "application/json",
		Body:         body,
	}); err != nil {
		t.Fatalf("publish job: %v", err)
	}
}

// waitForTerminalStatus polls the video row until it leaves the non-terminal
// states or the timeout elapses.
func waitForTerminalStatus(t *testing.T, db *sql.DB, videoID string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var status string
		if err := db.QueryRowContext(context.Background(),
			`SELECT status FROM videos WHERE id = $1`, videoID).Scan(&status); err != nil {
			t.Fatalf("poll status: %v", err)
		}
		if status == "ready" || status == "failed" {
			return status
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("video %s did not reach a terminal status within %s", videoID, timeout)
	return ""
}

func videoErrorMessage(t *testing.T, db *sql.DB, videoID string) string {
	t.Helper()
	var msg *string
	if err := db.QueryRowContext(context.Background(),
		`SELECT error_message FROM videos WHERE id = $1`, videoID).Scan(&msg); err != nil {
		return "(unavailable: " + err.Error() + ")"
	}
	if msg == nil {
		return ""
	}
	return *msg
}
