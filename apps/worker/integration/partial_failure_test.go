//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"mediaflow/apps/worker/internal/config"
	"mediaflow/apps/worker/internal/database"
	"mediaflow/apps/worker/internal/job"
)

// TestRenditionTerminalFailureCleansUp proves immediate partial-failure cleanup:
// when one rendition is doomed, FailVideoFromRendition fails the whole video,
// cancels sibling jobs still in flight, leaves completed siblings alone, and notes
// the discarded completed renditions in an event. The video converges to `failed`
// rather than hanging — a doomed rendition never decrements the counter, so finalize
// would otherwise never fire.
func TestRenditionTerminalFailureCleansUp(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	truncateAll(t, db)

	videoID := uuid.NewString()
	planJobID := uuid.NewString()
	seedProcessingVideo(t, db, videoID)
	seedPlanCompleted(t, db, planJobID, videoID, 3)
	done := seedRendition(t, db, videoID, planJobID, "720p", "completed")
	doomed := seedRendition(t, db, videoID, planJobID, "480p", "processing")
	queued := seedRendition(t, db, videoID, planJobID, "360p", "queued")
	insertVariant(t, db, videoID, "720p", 2800000) // 720p already produced its variant

	repo := database.NewRepository(db)
	if err := repo.FailVideoFromRendition(ctx, doomed, videoID, planJobID, errors.New("ffmpeg blew up")); err != nil {
		t.Fatalf("FailVideoFromRendition: %v", err)
	}

	// Video failed, with the cause surfaced.
	var status, errMsg string
	if err := db.QueryRowContext(ctx, `SELECT status, COALESCE(error_message, '') FROM videos WHERE id = $1`, videoID).Scan(&status, &errMsg); err != nil {
		t.Fatalf("read video: %v", err)
	}
	if status != "failed" {
		t.Fatalf("video status = %q, want failed", status)
	}
	if !strings.Contains(errMsg, "ffmpeg blew up") {
		t.Fatalf("video error_message = %q, want the failure cause", errMsg)
	}

	// Doomed failed; the still-queued sibling cancelled; the completed sibling left intact.
	assertJobStatus(t, db, doomed, "failed")
	assertJobStatus(t, db, queued, "failed")
	assertJobStatus(t, db, done, "completed")

	var cancelErr string
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(last_error, '') FROM video_jobs WHERE id = $1`, queued).Scan(&cancelErr); err != nil {
		t.Fatalf("read cancelled sibling: %v", err)
	}
	if !strings.Contains(cancelErr, "cancelled") {
		t.Fatalf("cancelled sibling last_error = %q, want a cancellation note", cancelErr)
	}

	// The failure event names the failed quality and lists the discarded completed one.
	var meta []byte
	if err := db.QueryRowContext(ctx, `
		SELECT metadata_json FROM video_events
		WHERE video_id = $1 AND event_type = 'video.processing.failed'
		ORDER BY created_at DESC LIMIT 1
	`, videoID).Scan(&meta); err != nil {
		t.Fatalf("read failure event: %v", err)
	}
	var parsed struct {
		FailedQuality       string   `json:"failedQuality"`
		CompletedRenditions []string `json:"completedRenditions"`
	}
	if err := json.Unmarshal(meta, &parsed); err != nil {
		t.Fatalf("decode event metadata: %v", err)
	}
	if parsed.FailedQuality != "480p" {
		t.Fatalf("failedQuality = %q, want 480p", parsed.FailedQuality)
	}
	if len(parsed.CompletedRenditions) != 1 || parsed.CompletedRenditions[0] != "720p" {
		t.Fatalf("completedRenditions = %v, want [720p]", parsed.CompletedRenditions)
	}

	// No finalize was enqueued — a doomed rendition never reaches the counter.
	assertJobCount(t, db, videoID, "finalize", "queued", 0)
}

// TestRenditionExhaustsRetriesFailsVideo drives a rendition through the real broker
// with a missing raw object so the download keeps failing. With max attempts of 1
// the first failure is terminal: the worker fails the whole video and parks the
// message in the DLQ — the pipeline converges to `failed`, never stuck.
func TestRenditionExhaustsRetriesFailsVideo(t *testing.T) {
	requireFFmpeg(t)

	ctx := context.Background()
	db := openDB(t)
	truncateAll(t, db)

	videoID := uuid.NewString()
	planJobID := uuid.NewString()
	missingKey := "raw-videos/" + videoID + "/original.mp4" // deliberately never uploaded
	seedProcessingVideo(t, db, videoID)
	seedPlanCompleted(t, db, planJobID, videoID, 1)
	renditionJobID := seedRendition(t, db, videoID, planJobID, "360p", "queued")

	w := newTestWorker(t, db, func(c *config.Config) {
		c.JobMaxAttempts = 1 // the first failure is terminal
		c.RetryBaseDelay = 100 * time.Millisecond
	})
	purgeQueues(t)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = w.Run(runCtx) }()

	body, err := json.Marshal(job.RenditionJob{
		JobID:        renditionJobID,
		ParentJobID:  planJobID,
		VideoID:      videoID,
		RawBucket:    rawBucket,
		RawObjectKey: missingKey,
		Spec:         job.RenditionSpec{Quality: "360p", Width: 640, Height: 360, Bitrate: 800000, Codec: "h264"},
		RequestedAt:  time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("marshal rendition job: %v", err)
	}
	publishRaw(t, job.RenditionRoutingKey, body)

	if status := waitForTerminalStatus(t, db, videoID, 60*time.Second); status != "failed" {
		t.Fatalf("expected video failed, got %q", status)
	}

	msg := getMessage(t, job.DLQRoutingKey, 10*time.Second)
	if reason, _ := msg.Headers["x-failure-reason"].(string); !strings.Contains(reason, "attempts exhausted") {
		t.Fatalf("expected DLQ message tagged exhausted, got reason=%q", reason)
	}
}
