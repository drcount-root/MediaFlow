package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"mediaflow/apps/worker/internal/job"
)

type Repository struct {
	db *sql.DB
}

func Open(ctx context.Context, databaseURL string) (*sql.DB, error) {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(30 * time.Minute)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func NewRepository(db *sql.DB) *Repository {
	return &Repository{db: db}
}

func (r *Repository) ClaimJob(ctx context.Context, jobID, videoID, workerID string, lease time.Duration) (bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	var currentStatus string
	err = tx.QueryRowContext(ctx, `
		SELECT status FROM videos WHERE id = $1 FOR UPDATE
	`, videoID).Scan(&currentStatus)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if currentStatus == job.StatusReady {
		return false, tx.Commit()
	}

	// Claiming stamps this worker's id and a fresh lease, and counts the attempt.
	// Only queued/failed jobs are claimable; a job another worker is processing
	// (with a live lease) is not — the reaper resets it to queued if its lease
	// expires.
	result, err := tx.ExecContext(ctx, `
		UPDATE video_jobs
		SET status = 'processing',
			attempts = attempts + 1,
			claimed_by = $3,
			lease_expires_at = now() + make_interval(secs => $4),
			updated_at = now()
		WHERE id = $1 AND video_id = $2 AND status IN ('queued', 'failed')
	`, jobID, videoID, workerID, int(lease.Seconds()))
	if err != nil {
		return false, err
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if affected == 0 {
		return false, tx.Commit()
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE videos SET status = 'processing', error_message = NULL, updated_at = now()
		WHERE id = $1
	`, videoID)
	if err != nil {
		return false, err
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO video_events (video_id, event_type, message)
		VALUES ($1, 'video.processing.started', 'Transcoding worker started processing.')
	`, videoID)
	if err != nil {
		return false, err
	}

	return true, tx.Commit()
}

// Heartbeat extends the lease on a job this worker still holds. The guard on
// claimed_by + status means a worker that lost its claim (e.g. the reaper
// already requeued it) silently stops extending — it affects zero rows.
func (r *Repository) Heartbeat(ctx context.Context, jobID, workerID string, lease time.Duration) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE video_jobs
		SET lease_expires_at = now() + make_interval(secs => $3), updated_at = now()
		WHERE id = $1 AND claimed_by = $2 AND status = 'processing'
	`, jobID, workerID, int(lease.Seconds()))
	return err
}

func (r *Repository) SaveProbe(ctx context.Context, videoID string, probe job.ProbeResult) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE videos SET duration_seconds = $2, updated_at = now()
		WHERE id = $1
	`, videoID, probe.DurationSeconds)
	if err != nil {
		return err
	}
	return r.addEvent(ctx, videoID, "video.probe.completed", "Video metadata probe completed.")
}

// ClaimChildJob claims a fanned-out rendition or finalize job: it stamps this
// worker's id and a fresh lease and counts the attempt, but — unlike the plan
// claim — does not churn the video's status (the plan job already moved it to
// `processing`). The EXISTS guard makes a worker drop the job if a sibling
// rendition has already driven the video to a terminal state.
func (r *Repository) ClaimChildJob(ctx context.Context, jobID, videoID, workerID string, lease time.Duration) (bool, error) {
	result, err := r.db.ExecContext(ctx, `
		UPDATE video_jobs
		SET status = 'processing',
			attempts = attempts + 1,
			claimed_by = $3,
			lease_expires_at = now() + make_interval(secs => $4),
			updated_at = now()
		WHERE id = $1 AND video_id = $2 AND status IN ('queued', 'failed')
		  AND EXISTS (SELECT 1 FROM videos WHERE id = $2 AND status NOT IN ('ready', 'failed'))
	`, jobID, videoID, workerID, int(lease.Seconds()))
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected == 1, nil
}

// SaveThumbnail records the thumbnail the planner produced. Stored on the video
// up front so it is visible before the renditions finish.
func (r *Repository) SaveThumbnail(ctx context.Context, videoID, thumbnailKey string) error {
	if _, err := r.db.ExecContext(ctx, `
		UPDATE videos SET thumbnail_key = $2, updated_at = now() WHERE id = $1
	`, videoID, thumbnailKey); err != nil {
		return err
	}
	return r.addEvent(ctx, videoID, "video.thumbnail.generated", "Thumbnail generated and uploaded.")
}

// FanOutRenditions is the map step: in one transaction it creates a rendition
// job (+ outbox message) per spec and stamps the pending-rendition counter on
// the plan job, which it marks completed. The guard on the plan job being
// `processing` makes this idempotent — a re-delivered plan message whose fan-out
// already committed claims nothing and never double-fans-out.
func (r *Repository) FanOutRenditions(ctx context.Context, planJobID, videoID, rawBucket, rawObjectKey string, specs []job.RenditionSpec) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	now := time.Now().UTC()
	for _, spec := range specs {
		specJSON, err := json.Marshal(spec)
		if err != nil {
			return err
		}
		var renditionJobID string
		if err := tx.QueryRowContext(ctx, `
			INSERT INTO video_jobs (video_id, job_type, status, parent_job_id, rendition_spec)
			VALUES ($1, 'rendition', 'queued', $2, $3)
			RETURNING id
		`, videoID, planJobID, specJSON).Scan(&renditionJobID); err != nil {
			return err
		}

		payload, err := json.Marshal(job.RenditionJob{
			JobID:        renditionJobID,
			ParentJobID:  planJobID,
			VideoID:      videoID,
			RawBucket:    rawBucket,
			RawObjectKey: rawObjectKey,
			Spec:         spec,
			RequestedAt:  now,
		})
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO outbox_messages (exchange, routing_key, payload_json)
			VALUES ($1, $2, $3)
		`, job.VideoExchange, job.RenditionRoutingKey, payload); err != nil {
			return err
		}
	}

	result, err := tx.ExecContext(ctx, `
		UPDATE video_jobs
		SET status = 'completed', pending_renditions = $2, claimed_by = NULL, lease_expires_at = NULL, updated_at = now()
		WHERE id = $1 AND status = 'processing'
	`, planJobID, len(specs))
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		// Lost the claim (e.g. the reaper requeued the plan job). Roll back so the
		// rendition rows above never escape without a counter behind them.
		return errPlanClaimLost
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO video_events (video_id, event_type, message, metadata_json)
		VALUES ($1, 'video.plan.completed', 'Planned renditions and fanned out jobs.', jsonb_build_object('renditions', $2::int))
	`, videoID, len(specs)); err != nil {
		return err
	}

	return tx.Commit()
}

// errPlanClaimLost signals that the plan job was no longer claimed by this worker
// at fan-out time, so the transaction was rolled back. The caller treats it as a
// benign skip: another worker owns the plan now.
var errPlanClaimLost = errors.New("plan job claim lost before fan-out")

// IsPlanClaimLost reports whether err is the benign lost-claim signal.
func IsPlanClaimLost(err error) bool { return errors.Is(err, errPlanClaimLost) }

// CompleteRendition is the reduce step for one rendition: it records the variant
// (upsert, so a retry is safe) and atomically decrements the plan's pending
// counter. The worker that drives the counter to zero enqueues the finalize job
// (returned via last/finalizeJobID). The guard on the rendition job being
// `processing` makes a duplicate delivery a no-op — it never double-decrements.
func (r *Repository) CompleteRendition(ctx context.Context, renditionJobID, parentJobID, videoID string, v job.Variant) (last bool, finalizeJobID string, err error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return false, "", err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO video_variants (video_id, quality, width, height, bitrate, codec, playlist_key)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (video_id, quality) DO UPDATE
		SET width = EXCLUDED.width, height = EXCLUDED.height, bitrate = EXCLUDED.bitrate,
			codec = EXCLUDED.codec, playlist_key = EXCLUDED.playlist_key
	`, videoID, v.Quality, v.Width, v.Height, v.Bitrate, v.Codec, v.PlaylistKey); err != nil {
		return false, "", err
	}

	result, err := tx.ExecContext(ctx, `
		UPDATE video_jobs
		SET status = 'completed', claimed_by = NULL, lease_expires_at = NULL, updated_at = now()
		WHERE id = $1 AND status = 'processing'
	`, renditionJobID)
	if err != nil {
		return false, "", err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, "", err
	}
	if affected == 0 {
		// Duplicate delivery: this rendition was already completed. The upsert above
		// is idempotent; do not decrement the counter again.
		return false, "", tx.Commit()
	}

	var pending int
	if err := tx.QueryRowContext(ctx, `
		UPDATE video_jobs SET pending_renditions = pending_renditions - 1, updated_at = now()
		WHERE id = $1
		RETURNING pending_renditions
	`, parentJobID).Scan(&pending); err != nil {
		return false, "", err
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO video_events (video_id, event_type, message, metadata_json)
		VALUES ($1, 'video.rendition.completed', 'Rendition transcoded and uploaded.', jsonb_build_object('quality', $2::text, 'pending', $3::int))
	`, videoID, v.Quality, pending); err != nil {
		return false, "", err
	}

	if pending > 0 {
		return false, "", tx.Commit()
	}

	// Last rendition: enqueue finalize via the outbox.
	if err := tx.QueryRowContext(ctx, `
		INSERT INTO video_jobs (video_id, job_type, status, parent_job_id)
		VALUES ($1, 'finalize', 'queued', $2)
		RETURNING id
	`, videoID, parentJobID).Scan(&finalizeJobID); err != nil {
		return false, "", err
	}
	payload, err := json.Marshal(job.FinalizeJob{
		JobID:       finalizeJobID,
		ParentJobID: parentJobID,
		VideoID:     videoID,
		RequestedAt: time.Now().UTC(),
	})
	if err != nil {
		return false, "", err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO outbox_messages (exchange, routing_key, payload_json)
		VALUES ($1, $2, $3)
	`, job.VideoExchange, job.FinalizeRoutingKey, payload); err != nil {
		return false, "", err
	}

	return true, finalizeJobID, tx.Commit()
}

// ListVariants returns the recorded variants for a video, used by the finalizer
// to build the master playlist from what the renditions actually produced.
func (r *Repository) ListVariants(ctx context.Context, videoID string) ([]job.Variant, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT quality, width, height, bitrate, COALESCE(codec, ''), playlist_key
		FROM video_variants WHERE video_id = $1
		ORDER BY bitrate DESC
	`, videoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var variants []job.Variant
	for rows.Next() {
		var v job.Variant
		if err := rows.Scan(&v.Quality, &v.Width, &v.Height, &v.Bitrate, &v.Codec, &v.PlaylistKey); err != nil {
			return nil, err
		}
		variants = append(variants, v)
	}
	return variants, rows.Err()
}

// CompleteFinalize writes the master key on the video, marks it ready, and
// completes the finalize job — all in one transaction. The job guard makes a
// duplicate finalize delivery a no-op.
func (r *Repository) CompleteFinalize(ctx context.Context, finalizeJobID, videoID, hlsMasterKey string) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	result, err := tx.ExecContext(ctx, `
		UPDATE video_jobs
		SET status = 'completed', claimed_by = NULL, lease_expires_at = NULL, updated_at = now()
		WHERE id = $1 AND status = 'processing'
	`, finalizeJobID)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return tx.Commit()
	}

	// status <> 'failed' guard: finalize only ever fires on the all-renditions-
	// completed path (a failed rendition never decrements the counter), so a
	// failed video should be unreachable here — but never resurrect one if it is.
	if _, err := tx.ExecContext(ctx, `
		UPDATE videos
		SET status = 'ready', hls_master_key = $2, error_message = NULL, updated_at = now()
		WHERE id = $1 AND status <> 'failed'
	`, videoID, hlsMasterKey); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO video_events (video_id, event_type, message)
		VALUES
			($1, 'video.hls.generated', 'Master playlist assembled.'),
			($1, 'video.processing.completed', 'Video processing completed.')
	`, videoID); err != nil {
		return err
	}

	return tx.Commit()
}

// JobAttempts returns the current attempt count on a job. The worker reads it
// after a failed run to decide between scheduling a retry and giving up.
func (r *Repository) JobAttempts(ctx context.Context, jobID string) (int, error) {
	var attempts int
	err := r.db.QueryRowContext(ctx, `SELECT attempts FROM video_jobs WHERE id = $1`, jobID).Scan(&attempts)
	return attempts, err
}

// MarkQueuedForRetry returns a transiently-failed job to the claimable `queued`
// state and clears its lease, so the redelivered retry message can re-claim it.
// The video stays `processing` — from the user's view it is still in flight —
// and a `video.job.retry_scheduled` event records the attempt and backoff. The
// re-claim increments attempts, so this does not.
func (r *Repository) MarkQueuedForRetry(ctx context.Context, jobID, videoID string, cause error, attempts int, delay time.Duration) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	message := "unknown processing error"
	if cause != nil {
		message = cause.Error()
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE video_jobs
		SET status = 'queued', last_error = $2, claimed_by = NULL, lease_expires_at = NULL, updated_at = now()
		WHERE id = $1
	`, jobID, message); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO video_events (video_id, event_type, message, metadata_json)
		VALUES ($1, 'video.job.retry_scheduled', $2, jsonb_build_object('attempts', $3::int, 'delayMs', $4::bigint))
	`, videoID, message, attempts, delay.Milliseconds()); err != nil {
		return err
	}

	return tx.Commit()
}

func (r *Repository) FailJob(ctx context.Context, jobID, videoID string, cause error) error {
	message := "unknown processing error"
	if cause != nil {
		message = cause.Error()
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `
		UPDATE videos SET status = 'failed', error_message = $2, updated_at = now()
		WHERE id = $1
	`, videoID, message); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE video_jobs SET status = 'failed', last_error = $3, updated_at = now()
		WHERE id = $1 AND video_id = $2
	`, jobID, videoID, message); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO video_events (video_id, event_type, message)
		VALUES ($1, 'video.processing.failed', $2)
	`, videoID, message); err != nil {
		return err
	}

	return tx.Commit()
}

// FailVideoFromRendition terminally fails a video because one rendition exhausted
// its retries (or hit a permanent error). In a single transaction it: locks the
// parent plan row (serialising with CompleteRendition's decrement/finalize hand-off
// on the same row, so a concurrent last-rendition completion can't finalise a video
// we are failing); marks this rendition job failed; cancels sibling rendition and
// finalize jobs still queued or processing (so they stop retrying and the reaper
// ignores them — a sibling mid-transcode finds its job no longer `processing` and
// no-ops in CompleteRendition); marks the video failed; and records an event
// listing the renditions that had already completed and are now discarded.
//
// A terminally-failed rendition never decrements the pending counter, so finalize
// can never have fired for this video — cleanup is about stopping in-flight
// siblings and surfacing the failure promptly, not unwinding a completed pipeline.
func (r *Repository) FailVideoFromRendition(ctx context.Context, renditionJobID, videoID, parentJobID string, cause error) error {
	message := "unknown processing error"
	if cause != nil {
		message = cause.Error()
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Lock the plan row first to serialise with the aggregation counter.
	if parentJobID != "" {
		if _, err := tx.ExecContext(ctx, `SELECT 1 FROM video_jobs WHERE id = $1 FOR UPDATE`, parentJobID); err != nil {
			return err
		}
	}

	// Record the failed quality (if known) for the audit log before we touch it.
	var failedQuality sql.NullString
	if err := tx.QueryRowContext(ctx, `
		SELECT rendition_spec->>'quality' FROM video_jobs WHERE id = $1
	`, renditionJobID).Scan(&failedQuality); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE video_jobs
		SET status = 'failed', last_error = $2, claimed_by = NULL, lease_expires_at = NULL, updated_at = now()
		WHERE id = $1
	`, renditionJobID, message); err != nil {
		return err
	}

	// Cancel siblings (other renditions + the finalize job) still in flight so they
	// stop retrying; completed siblings are left as-is (their objects are orphaned).
	if parentJobID != "" {
		if _, err := tx.ExecContext(ctx, `
			UPDATE video_jobs
			SET status = 'failed', last_error = 'cancelled: sibling rendition failed',
				claimed_by = NULL, lease_expires_at = NULL, updated_at = now()
			WHERE parent_job_id = $1 AND id <> $2 AND status IN ('queued', 'processing')
		`, parentJobID, renditionJobID); err != nil {
			return err
		}
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE videos SET status = 'failed', error_message = $2, updated_at = now()
		WHERE id = $1 AND status <> 'failed'
	`, videoID, message); err != nil {
		return err
	}

	// Note which renditions had already finished (now discarded) for the audit log.
	completed, err := completedQualities(ctx, tx, videoID)
	if err != nil {
		return err
	}
	completedJSON, err := json.Marshal(completed)
	if err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO video_events (video_id, event_type, message, metadata_json)
		VALUES ($1, 'video.processing.failed', $2,
			jsonb_build_object('failedQuality', $3::text, 'completedRenditions', $4::jsonb))
	`, videoID, message, failedQuality.String, string(completedJSON)); err != nil {
		return err
	}

	return tx.Commit()
}

// completedQualities returns the qualities already recorded for a video, highest
// bitrate first — the renditions that finished before a sibling doomed the video.
func completedQualities(ctx context.Context, tx *sql.Tx, videoID string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT quality FROM video_variants WHERE video_id = $1 ORDER BY bitrate DESC
	`, videoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	qualities := []string{}
	for rows.Next() {
		var q string
		if err := rows.Scan(&q); err != nil {
			return nil, err
		}
		qualities = append(qualities, q)
	}
	return qualities, rows.Err()
}

func (r *Repository) addEvent(ctx context.Context, videoID, eventType, message string) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO video_events (video_id, event_type, message)
		VALUES ($1, $2, $3)
	`, videoID, eventType, message)
	return err
}
