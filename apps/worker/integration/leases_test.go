//go:build integration

package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"mediaflow/apps/worker/internal/database"
	"mediaflow/apps/worker/internal/job"
	"mediaflow/apps/worker/internal/reaper"
)

func TestClaimStampsLeaseAndAttempt(t *testing.T) {
	db := openDB(t)
	truncateAll(t, db)
	ctx := context.Background()

	videoID := uuid.NewString()
	jobID := uuid.NewString()
	seedQueued(t, db, videoID, jobID)

	repo := database.NewRepository(db)
	claimed, err := repo.ClaimJob(ctx, jobID, videoID, "worker-1", 2*time.Minute)
	if err != nil {
		t.Fatalf("ClaimJob: %v", err)
	}
	if !claimed {
		t.Fatal("expected job to be claimed")
	}

	var (
		status     string
		attempts   int
		claimedBy  sql.NullString
		leaseValid bool
	)
	if err := db.QueryRowContext(ctx, `
		SELECT status, attempts, claimed_by, lease_expires_at > now()
		FROM video_jobs WHERE id = $1
	`, jobID).Scan(&status, &attempts, &claimedBy, &leaseValid); err != nil {
		t.Fatalf("read job: %v", err)
	}
	if status != "processing" || attempts != 1 {
		t.Fatalf("expected processing/attempts=1, got %s/%d", status, attempts)
	}
	if claimedBy.String != "worker-1" {
		t.Fatalf("expected claimed_by worker-1, got %q", claimedBy.String)
	}
	if !leaseValid {
		t.Fatal("expected lease_expires_at in the future")
	}
}

func TestHeartbeatExtendsLease(t *testing.T) {
	db := openDB(t)
	truncateAll(t, db)
	ctx := context.Background()

	videoID := uuid.NewString()
	jobID := uuid.NewString()
	// Claimed by worker-1 with a lease about to expire.
	seedProcessing(t, db, videoID, jobID, 1, "worker-1", time.Now().Add(5*time.Second))

	repo := database.NewRepository(db)
	if err := repo.Heartbeat(ctx, jobID, "worker-1", 2*time.Minute); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	var extended bool
	if err := db.QueryRowContext(ctx, `
		SELECT lease_expires_at > now() + interval '1 minute' FROM video_jobs WHERE id = $1
	`, jobID).Scan(&extended); err != nil {
		t.Fatalf("read lease: %v", err)
	}
	if !extended {
		t.Fatal("expected heartbeat to extend the lease well into the future")
	}

	// A heartbeat from a worker that does not hold the claim must be a no-op.
	if err := repo.Heartbeat(ctx, jobID, "intruder", 10*time.Minute); err != nil {
		t.Fatalf("Heartbeat (intruder): %v", err)
	}
	var stillReasonable bool
	if err := db.QueryRowContext(ctx, `
		SELECT lease_expires_at < now() + interval '5 minute' FROM video_jobs WHERE id = $1
	`, jobID).Scan(&stillReasonable); err != nil {
		t.Fatalf("read lease: %v", err)
	}
	if !stillReasonable {
		t.Fatal("a non-owning worker should not have extended the lease")
	}
}

func TestReaperRequeuesExpiredLeaseBelowMax(t *testing.T) {
	db := openDB(t)
	truncateAll(t, db)
	ctx := context.Background()

	videoID := uuid.NewString()
	jobID := uuid.NewString()
	seedProcessing(t, db, videoID, jobID, 1, "dead-worker", time.Now().Add(-1*time.Minute))

	rp := reaper.New(db, discardLogger(), "mediaflow-raw", 3, time.Minute)
	requeued, failed, err := rp.Reap(ctx)
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if requeued != 1 || failed != 0 {
		t.Fatalf("expected 1 requeued / 0 failed, got %d/%d", requeued, failed)
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
	if jobStatus != "queued" || videoStatus != "queued" {
		t.Fatalf("expected job+video requeued, got job=%s video=%s", jobStatus, videoStatus)
	}
	if attempts != 1 {
		t.Fatalf("reaper must not change attempts, got %d", attempts)
	}
	if claimedBy.Valid || lease.Valid {
		t.Fatalf("expected claim/lease cleared, got claimed_by=%v lease=%v", claimedBy, lease)
	}

	// A transcode message was written to the outbox for the API relay to publish.
	var exchange, routingKey string
	var payload []byte
	if err := db.QueryRowContext(ctx, `
		SELECT exchange, routing_key, payload_json FROM outbox_messages
	`).Scan(&exchange, &routingKey, &payload); err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	if exchange != job.VideoExchange || routingKey != job.TranscodeRoutingKey {
		t.Fatalf("unexpected outbox routing: %s/%s", exchange, routingKey)
	}
	var requeuedJob job.TranscodeJob
	if err := json.Unmarshal(payload, &requeuedJob); err != nil {
		t.Fatalf("decode outbox payload: %v", err)
	}
	if requeuedJob.VideoID != videoID || requeuedJob.JobID != jobID {
		t.Fatalf("outbox job does not match: %#v", requeuedJob)
	}

	var events int
	if err := db.QueryRowContext(ctx, `
		SELECT count(*) FROM video_events WHERE video_id = $1 AND event_type = 'video.job.requeued'
	`, videoID).Scan(&events); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if events != 1 {
		t.Fatalf("expected 1 requeue event, got %d", events)
	}
}

func TestReaperFailsAtMaxAttempts(t *testing.T) {
	db := openDB(t)
	truncateAll(t, db)
	ctx := context.Background()

	videoID := uuid.NewString()
	jobID := uuid.NewString()
	seedProcessing(t, db, videoID, jobID, 3, "dead-worker", time.Now().Add(-1*time.Minute))

	rp := reaper.New(db, discardLogger(), "mediaflow-raw", 3, time.Minute)
	requeued, failed, err := rp.Reap(ctx)
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if requeued != 0 || failed != 1 {
		t.Fatalf("expected 0 requeued / 1 failed, got %d/%d", requeued, failed)
	}

	var (
		jobStatus   string
		lastError   sql.NullString
		videoStatus string
		videoError  sql.NullString
	)
	if err := db.QueryRowContext(ctx, `
		SELECT j.status, j.last_error, v.status, v.error_message
		FROM video_jobs j JOIN videos v ON v.id = j.video_id WHERE j.id = $1
	`, jobID).Scan(&jobStatus, &lastError, &videoStatus, &videoError); err != nil {
		t.Fatalf("read job/video: %v", err)
	}
	if jobStatus != "failed" || videoStatus != "failed" {
		t.Fatalf("expected job+video failed, got job=%s video=%s", jobStatus, videoStatus)
	}
	if !lastError.Valid || !videoError.Valid {
		t.Fatal("expected an error message on the failed job and video")
	}

	var outboxRows int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM outbox_messages`).Scan(&outboxRows); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	if outboxRows != 0 {
		t.Fatalf("an exhausted job must not be re-enqueued, found %d outbox rows", outboxRows)
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func seedQueued(t *testing.T, db *sql.DB, videoID, jobID string) {
	t.Helper()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO videos (id, title, status, raw_object_key, original_filename, content_type, size_bytes)
		VALUES ($1, 'Lease Test', 'queued', $2, 'fixture.mp4', 'video/mp4', 0)
	`, videoID, "raw-videos/"+videoID+"/original.mp4"); err != nil {
		t.Fatalf("seed video: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO video_jobs (id, video_id, job_type, status, attempts)
		VALUES ($1, $2, 'transcode', 'queued', 0)
	`, jobID, videoID); err != nil {
		t.Fatalf("seed job: %v", err)
	}
}

func seedProcessing(t *testing.T, db *sql.DB, videoID, jobID string, attempts int, claimedBy string, leaseExpiresAt time.Time) {
	t.Helper()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO videos (id, title, status, raw_object_key, original_filename, content_type, size_bytes)
		VALUES ($1, 'Lease Test', 'processing', $2, 'fixture.mp4', 'video/mp4', 0)
	`, videoID, "raw-videos/"+videoID+"/original.mp4"); err != nil {
		t.Fatalf("seed video: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO video_jobs (id, video_id, job_type, status, attempts, claimed_by, lease_expires_at)
		VALUES ($1, $2, 'transcode', 'processing', $3, $4, $5)
	`, jobID, videoID, attempts, claimedBy, leaseExpiresAt.UTC()); err != nil {
		t.Fatalf("seed job: %v", err)
	}
}
