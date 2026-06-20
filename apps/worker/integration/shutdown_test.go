//go:build integration

package integration

import (
	"context"
	"database/sql"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"mediaflow/apps/worker/internal/job"
)

// TestGracefulShutdownFinishesInFlightRendition proves that a shutdown signal
// while a rendition is transcoding lets that rendition finish and commit its
// work (job → completed, variant recorded) rather than abandoning it — the clean
// SIGTERM path, distinct from kill -9 which the reaper handles. With M7 fan-out a
// single worker won't drive the whole video to `ready` during shutdown (the
// downstream consumers are cancelled), so the guarantee is asserted at the
// in-flight stage: no committed work is lost.
func TestGracefulShutdownFinishesInFlightRendition(t *testing.T) {
	requireFFmpeg(t)

	ctx := context.Background()
	db := openDB(t)
	truncateAll(t, db)

	videoID := uuid.NewString()
	jobID := uuid.NewString()
	rawKey := "raw-videos/" + videoID + "/original.mp4"

	client, err := minioClient()
	if err != nil {
		t.Fatalf("minio client: %v", err)
	}
	// A 720p clip fans out to three renditions; at concurrency 1 they run one at a
	// time, giving a reliable window to catch one mid-transcode.
	if _, err := client.FPutObject(ctx, rawBucket, rawKey, generateClip(t, 6), minio.PutObjectOptions{ContentType: "video/mp4"}); err != nil {
		t.Fatalf("upload fixture: %v", err)
	}
	seedQueuedVideo(t, db, videoID, jobID, rawKey)

	w := newTestWorker(t, db)
	purgeQueues(t)

	runCtx, cancel := context.WithCancel(ctx)
	runDone := make(chan struct{})
	startTestRelay(t, runCtx, db)
	publishJob(t, job.TranscodeJob{JobID: jobID, VideoID: videoID, RawBucket: rawBucket, RawObjectKey: rawKey, RequestedAt: time.Now().UTC()})
	go func() { _ = w.Run(runCtx); close(runDone) }()

	// Once a rendition is actually transcoding, signal shutdown mid-flight.
	waitForRenditionStatus(t, db, videoID, "processing", 60*time.Second)
	cancel()

	// The in-flight rendition must finish and commit: at least one rendition job
	// reaches `completed` with its variant recorded, rather than being abandoned.
	if !waitForCompletedRendition(t, db, videoID, 90*time.Second) {
		t.Fatalf("graceful shutdown should finish the in-flight rendition; none completed (error=%s)", videoErrorMessage(t, db, videoID))
	}
	var variants int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM video_variants WHERE video_id = $1`, videoID).Scan(&variants); err != nil {
		t.Fatalf("count variants: %v", err)
	}
	if variants < 1 {
		t.Fatal("a completed rendition must have recorded its variant")
	}

	// And the worker exits cleanly after draining.
	select {
	case <-runDone:
	case <-time.After(30 * time.Second):
		t.Fatal("worker did not exit after shutdown drained")
	}
}

// waitForRenditionStatus blocks until at least one rendition job for the video
// has the given status.
func waitForRenditionStatus(t *testing.T, db *sql.DB, videoID, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var n int
		if err := db.QueryRowContext(context.Background(),
			`SELECT count(*) FROM video_jobs WHERE video_id = $1 AND job_type = 'rendition' AND status = $2`, videoID, want).Scan(&n); err != nil {
			t.Fatalf("poll rendition status: %v", err)
		}
		if n > 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("no rendition for video %s reached status %q within %s", videoID, want, timeout)
}

func waitForCompletedRendition(t *testing.T, db *sql.DB, videoID string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var n int
		if err := db.QueryRowContext(context.Background(),
			`SELECT count(*) FROM video_jobs WHERE video_id = $1 AND job_type = 'rendition' AND status = 'completed'`, videoID).Scan(&n); err != nil {
			t.Fatalf("poll completed renditions: %v", err)
		}
		if n > 0 {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// generateClip renders a test clip of the given duration (seconds) at 720p so it
// fans out to the full rendition ladder.
func generateClip(t *testing.T, seconds int) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "clip.mp4")
	dur := strconv.Itoa(seconds)
	cmd := exec.Command("ffmpeg",
		"-y",
		"-f", "lavfi", "-i", "testsrc=duration="+dur+":size=1280x720:rate=30",
		"-f", "lavfi", "-i", "sine=frequency=1000:duration="+dur,
		"-c:v", "libx264", "-pix_fmt", "yuv420p",
		"-c:a", "aac",
		"-shortest",
		out,
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generate clip: %v\n%s", err, string(output))
	}
	return out
}
