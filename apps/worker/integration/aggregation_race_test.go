//go:build integration

package integration

import (
	"context"
	"sync"
	"testing"

	"github.com/google/uuid"
	"mediaflow/apps/worker/internal/database"
	"mediaflow/apps/worker/internal/job"
)

// TestAggregationRaceFinalizesOnce is the M7 counter-race proof: when two
// renditions finish at the same instant, the atomic pending-counter decrement
// (UPDATE ... RETURNING, serialised by the row lock on the plan job) must let
// exactly one of them observe zero and enqueue finalize — never zero, never twice.
func TestAggregationRaceFinalizesOnce(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	truncateAll(t, db)

	videoID := uuid.NewString()
	planJobID := uuid.NewString()
	seedProcessingVideo(t, db, videoID)
	seedPlanCompleted(t, db, planJobID, videoID, 2)
	// Both renditions are mid-flight (processing) so each passes CompleteRendition's
	// "still processing" guard exactly once.
	rA := seedRendition(t, db, videoID, planJobID, "720p", "processing")
	rB := seedRendition(t, db, videoID, planJobID, "480p", "processing")

	repo := database.NewRepository(db)

	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		lastCount int
	)
	complete := func(jobID, quality string, bitrate int) {
		defer wg.Done()
		last, _, err := repo.CompleteRendition(ctx, jobID, planJobID, videoID, renditionVariant(videoID, quality, bitrate))
		if err != nil {
			t.Errorf("CompleteRendition(%s): %v", quality, err)
			return
		}
		if last {
			mu.Lock()
			lastCount++
			mu.Unlock()
		}
	}

	wg.Add(2)
	go complete(rA, "720p", 2800000)
	go complete(rB, "480p", 1400000)
	wg.Wait()

	if lastCount != 1 {
		t.Fatalf("exactly one rendition must observe the last decrement, got %d", lastCount)
	}

	// Exactly one finalize job was enqueued, the counter landed on zero, and exactly
	// one finalize message went to the outbox.
	assertJobCount(t, db, videoID, "finalize", "queued", 1)

	var pending int
	if err := db.QueryRowContext(ctx, `SELECT pending_renditions FROM video_jobs WHERE id = $1`, planJobID).Scan(&pending); err != nil {
		t.Fatalf("read pending: %v", err)
	}
	if pending != 0 {
		t.Fatalf("pending_renditions = %d, want 0", pending)
	}

	var outbox int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM outbox_messages WHERE routing_key = $1`, job.FinalizeRoutingKey).Scan(&outbox); err != nil {
		t.Fatalf("count finalize outbox: %v", err)
	}
	if outbox != 1 {
		t.Fatalf("expected exactly 1 finalize outbox message, got %d", outbox)
	}
}
