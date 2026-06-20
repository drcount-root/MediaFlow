//go:build integration

package integration

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"mediaflow/apps/worker/internal/config"
	"mediaflow/apps/worker/internal/job"
	"mediaflow/apps/worker/internal/storage"
	"mediaflow/apps/worker/internal/worker"
)

// TestFanOutProducesAllRenditions is the core M7 proof: a 720p source is planned
// into three rendition jobs, each transcodes one quality and decrements the
// pending counter, and the worker that drives the counter to zero triggers
// finalize, which assembles a master playlist referencing all three renditions
// and marks the video ready.
func TestFanOutProducesAllRenditions(t *testing.T) {
	requireFFmpeg(t)

	ctx := context.Background()
	db := openDB(t)
	truncateAll(t, db)

	videoID := uuid.NewString()
	planJobID := uuid.NewString()
	rawKey := "raw-videos/" + videoID + "/original.mp4"

	client, err := minioClient()
	if err != nil {
		t.Fatalf("minio client: %v", err)
	}
	// 720p source -> full 720p/480p/360p ladder.
	if _, err := client.FPutObject(ctx, rawBucket, rawKey, generateClip(t, 3), minio.PutObjectOptions{ContentType: "video/mp4"}); err != nil {
		t.Fatalf("upload fixture: %v", err)
	}
	seedQueuedVideo(t, db, videoID, planJobID, rawKey)

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
		WorkerConcurrency:    2,
		WorkDir:              t.TempDir(),
		FFmpegPath:           "ffmpeg",
		FFprobePath:          "ffprobe",
		WorkerID:             "fanout-worker",
		JobLeaseDuration:     2 * time.Minute,
		JobMaxAttempts:       3,
		HeartbeatInterval:    30 * time.Second,
		ReaperInterval:       30 * time.Second,
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

	purgeQueues(t)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	startTestRelay(t, runCtx, db)
	publishJob(t, job.TranscodeJob{
		JobID:        planJobID,
		VideoID:      videoID,
		RawBucket:    rawBucket,
		RawObjectKey: rawKey,
		RequestedAt:  time.Now().UTC(),
	})
	go func() { _ = w.Run(runCtx) }()

	if status := waitForTerminalStatus(t, db, videoID, 120*time.Second); status != "ready" {
		t.Fatalf("expected ready, got %q (error=%s)", status, videoErrorMessage(t, db, videoID))
	}

	// Job hierarchy: the plan job is completed with a zeroed counter; three
	// rendition jobs and one finalize job, all completed, hang off it.
	var planStatus string
	var pending int
	if err := db.QueryRowContext(ctx,
		`SELECT status, COALESCE(pending_renditions, -1) FROM video_jobs WHERE id = $1`, planJobID).
		Scan(&planStatus, &pending); err != nil {
		t.Fatalf("read plan job: %v", err)
	}
	if planStatus != "completed" || pending != 0 {
		t.Fatalf("plan job: status=%q pending=%d (want completed/0)", planStatus, pending)
	}

	assertJobCount(t, db, videoID, "rendition", "completed", 3)
	assertJobCount(t, db, videoID, "finalize", "completed", 1)

	// Three variants recorded, each pointing at its own playlist.
	rows, err := db.QueryContext(ctx,
		`SELECT quality, playlist_key FROM video_variants WHERE video_id = $1 ORDER BY bitrate DESC`, videoID)
	if err != nil {
		t.Fatalf("query variants: %v", err)
	}
	defer rows.Close()
	var qualities []string
	for rows.Next() {
		var quality, playlistKey string
		if err := rows.Scan(&quality, &playlistKey); err != nil {
			t.Fatalf("scan variant: %v", err)
		}
		wantKey := "processed-videos/" + videoID + "/" + quality + "/index.m3u8"
		if playlistKey != wantKey {
			t.Fatalf("variant %s playlist_key=%q, want %q", quality, playlistKey, wantKey)
		}
		qualities = append(qualities, quality)
	}
	if strings.Join(qualities, ",") != "720p,480p,360p" {
		t.Fatalf("variants (bitrate desc) = %v, want [720p 480p 360p]", qualities)
	}

	// Master playlist lists all three renditions, highest bitrate first.
	master := getObject(t, client, processedBucket, "processed-videos/"+videoID+"/master.m3u8")
	for _, q := range []string{"720p/index.m3u8", "480p/index.m3u8", "360p/index.m3u8"} {
		if !strings.Contains(master, q) {
			t.Fatalf("master playlist missing %q:\n%s", q, master)
		}
	}
	if strings.Count(master, "#EXT-X-STREAM-INF") != 3 {
		t.Fatalf("expected 3 streams in master, got:\n%s", master)
	}
	if strings.Index(master, "720p/index.m3u8") > strings.Index(master, "360p/index.m3u8") {
		t.Fatalf("master should list 720p before 360p:\n%s", master)
	}
}

func assertJobCount(t *testing.T, db *sql.DB, videoID, jobType, status string, want int) {
	t.Helper()
	var n int
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM video_jobs WHERE video_id = $1 AND job_type = $2 AND status = $3`,
		videoID, jobType, status).Scan(&n); err != nil {
		t.Fatalf("count %s jobs: %v", jobType, err)
	}
	if n != want {
		t.Fatalf("expected %d %s jobs in status %q, got %d", want, jobType, status, n)
	}
}

func getObject(t *testing.T, client *minio.Client, bucket, key string) string {
	t.Helper()
	obj, err := client.GetObject(context.Background(), bucket, key, minio.GetObjectOptions{})
	if err != nil {
		t.Fatalf("get object %s: %v", key, err)
	}
	defer obj.Close()
	data, err := io.ReadAll(obj)
	if err != nil {
		t.Fatalf("read object %s: %v", key, err)
	}
	return string(data)
}
