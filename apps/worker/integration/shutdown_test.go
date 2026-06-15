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

// TestGracefulShutdownFinishesInFlightJob proves that a shutdown signal while a
// job is transcoding lets that job finish (reaches `ready`) rather than aborting
// it — the clean SIGTERM path, distinct from kill -9 which the reaper handles.
func TestGracefulShutdownFinishesInFlightJob(t *testing.T) {
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
	// A longer clip so the transcode is still running when we signal shutdown.
	if _, err := client.FPutObject(ctx, rawBucket, rawKey, generateClip(t, 8), minio.PutObjectOptions{ContentType: "video/mp4"}); err != nil {
		t.Fatalf("upload fixture: %v", err)
	}
	seedQueuedVideo(t, db, videoID, jobID, rawKey)

	w := newTestWorker(t, db)
	purgeQueues(t)
	publishJob(t, job.TranscodeJob{JobID: jobID, VideoID: videoID, RawBucket: rawBucket, RawObjectKey: rawKey, RequestedAt: time.Now().UTC()})

	runCtx, cancel := context.WithCancel(ctx)
	runDone := make(chan struct{})
	go func() { _ = w.Run(runCtx); close(runDone) }()

	// Once the job is actually processing, signal shutdown mid-flight.
	waitForVideoStatus(t, db, videoID, "processing", 30*time.Second)
	cancel()

	// The in-flight job must still complete.
	if status := waitForTerminalStatus(t, db, videoID, 90*time.Second); status != "ready" {
		t.Fatalf("graceful shutdown should finish the in-flight job; got %q (error=%s)", status, videoErrorMessage(t, db, videoID))
	}

	// And the worker exits cleanly after draining.
	select {
	case <-runDone:
	case <-time.After(30 * time.Second):
		t.Fatal("worker did not exit after shutdown drained")
	}
}

func waitForVideoStatus(t *testing.T, db *sql.DB, videoID, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var status string
		if err := db.QueryRowContext(context.Background(),
			`SELECT status FROM videos WHERE id = $1`, videoID).Scan(&status); err != nil {
			t.Fatalf("poll status: %v", err)
		}
		if status == want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("video %s never reached status %q within %s", videoID, want, timeout)
}

// generateClip renders a test clip of the given duration (seconds) at 360p.
func generateClip(t *testing.T, seconds int) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "clip.mp4")
	dur := strconv.Itoa(seconds)
	cmd := exec.Command("ffmpeg",
		"-y",
		"-f", "lavfi", "-i", "testsrc=duration="+dur+":size=640x360:rate=30",
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
