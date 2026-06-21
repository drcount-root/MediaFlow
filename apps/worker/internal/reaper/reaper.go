// Package reaper recovers jobs whose worker died without releasing them
// (Milestone 5.2). It scans video_jobs for `processing` rows whose lease has
// expired and either re-enqueues them (below max attempts) by writing an outbox
// row for the API relay to publish, or marks them failed (at max attempts).
//
// A `kill -9` worker can't run its own cleanup, so the reaper is the safety net
// that guarantees every video reaches `ready` or `failed` — never stuck.
package reaper

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"mediaflow/apps/worker/internal/job"
)

type Reaper struct {
	db          *sql.DB
	logger      *slog.Logger
	rawBucket   string
	maxAttempts int
	interval    time.Duration
	batchSize   int
}

func New(db *sql.DB, logger *slog.Logger, rawBucket string, maxAttempts int, interval time.Duration) *Reaper {
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &Reaper{
		db:          db,
		logger:      logger,
		rawBucket:   rawBucket,
		maxAttempts: maxAttempts,
		interval:    interval,
		batchSize:   50,
	}
}

// Run scans for expired leases on a ticker until ctx is cancelled.
func (r *Reaper) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	r.logger.Info("reaper started", "interval", r.interval.String(), "maxAttempts", r.maxAttempts)
	for {
		select {
		case <-ctx.Done():
			r.logger.Info("reaper stopped")
			return
		case <-ticker.C:
			requeued, failed, err := r.Reap(ctx)
			if err != nil && ctx.Err() == nil {
				r.logger.Error("reaper scan failed", "error", err)
			}
			if requeued > 0 || failed > 0 {
				r.logger.Info("reaper recovered jobs", "requeued", requeued, "failed", failed)
			}
		}
	}
}

// Reap processes all currently-expired leases (in batches) and returns how many
// were requeued and failed. Run calls it on a ticker; tests call it directly.
func (r *Reaper) Reap(ctx context.Context) (requeued, failed int, err error) {
	for {
		if ctx.Err() != nil {
			return requeued, failed, ctx.Err()
		}
		rq, fl, n, batchErr := r.reapBatch(ctx)
		requeued += rq
		failed += fl
		if batchErr != nil {
			return requeued, failed, batchErr
		}
		if n < r.batchSize {
			return requeued, failed, nil
		}
	}
}

type expiredJob struct {
	jobID         string
	videoID       string
	attempts      int
	rawObjectKey  string
	jobType       string
	parentJobID   sql.NullString
	renditionSpec []byte
}

func (r *Reaper) reapBatch(ctx context.Context) (requeued, failed, scanned int, err error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, 0, err
	}
	defer tx.Rollback()

	// Lock only the job rows, and skip any another reaper already holds.
	rows, err := tx.QueryContext(ctx, `
		SELECT j.id, j.video_id, j.attempts, COALESCE(v.raw_object_key, ''),
			j.job_type, j.parent_job_id, j.rendition_spec
		FROM video_jobs j
		JOIN videos v ON v.id = j.video_id
		WHERE j.status = 'processing' AND j.lease_expires_at < now()
		ORDER BY j.lease_expires_at
		FOR UPDATE OF j SKIP LOCKED
		LIMIT $1
	`, r.batchSize)
	if err != nil {
		return 0, 0, 0, err
	}

	var batch []expiredJob
	for rows.Next() {
		var e expiredJob
		if err := rows.Scan(&e.jobID, &e.videoID, &e.attempts, &e.rawObjectKey, &e.jobType, &e.parentJobID, &e.renditionSpec); err != nil {
			rows.Close()
			return 0, 0, 0, err
		}
		batch = append(batch, e)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, 0, 0, err
	}
	rows.Close()

	for _, e := range batch {
		if e.attempts < r.maxAttempts {
			if err := r.requeue(ctx, tx, e); err != nil {
				return 0, 0, 0, err
			}
			requeued++
		} else {
			if err := r.fail(ctx, tx, e); err != nil {
				return 0, 0, 0, err
			}
			failed++
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, 0, 0, err
	}
	return requeued, failed, len(batch), nil
}

// requeue resets the job to queued and writes an outbox row (routed to the queue
// for the job's stage) for the API relay to publish. A plan job restarts the
// whole pipeline, so the video also returns to queued; a rendition or finalize
// job is mid-fan-out, so the video stays processing.
func (r *Reaper) requeue(ctx context.Context, tx *sql.Tx, e expiredJob) error {
	routingKey, payload, resetVideo, err := r.requeueMessage(e)
	if err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE video_jobs
		SET status = 'queued', claimed_by = NULL, lease_expires_at = NULL, updated_at = now()
		WHERE id = $1
	`, e.jobID); err != nil {
		return err
	}

	if resetVideo {
		if _, err := tx.ExecContext(ctx, `
			UPDATE videos SET status = 'queued', updated_at = now() WHERE id = $1
		`, e.videoID); err != nil {
			return err
		}
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO outbox_messages (exchange, routing_key, payload_json)
		VALUES ($1, $2, $3)
	`, job.VideoExchange, routingKey, payload); err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO video_events (video_id, event_type, message, metadata_json)
		VALUES ($1, 'video.job.requeued', 'Lease expired; job re-enqueued by reaper.', jsonb_build_object('attempts', $2::int, 'stage', $3::text))
	`, e.videoID, e.attempts, e.jobType)
	return err
}

// requeueMessage builds the queue message for re-enqueuing an expired job,
// rebuilding it entirely from durable DB state so no in-flight context is needed.
func (r *Reaper) requeueMessage(e expiredJob) (routingKey string, payload []byte, resetVideo bool, err error) {
	now := time.Now().UTC()
	switch e.jobType {
	case job.JobTypeRendition:
		var spec job.RenditionSpec
		if err := json.Unmarshal(e.renditionSpec, &spec); err != nil {
			return "", nil, false, fmt.Errorf("decode rendition_spec for job %s: %w", e.jobID, err)
		}
		payload, err = json.Marshal(job.RenditionJob{
			JobID:        e.jobID,
			ParentJobID:  e.parentJobID.String,
			VideoID:      e.videoID,
			RawBucket:    r.rawBucket,
			RawObjectKey: e.rawObjectKey,
			Spec:         spec,
			RequestedAt:  now,
		})
		return job.RenditionRoutingKey, payload, false, err
	case job.JobTypeFinalize:
		payload, err = json.Marshal(job.FinalizeJob{
			JobID:        e.jobID,
			ParentJobID:  e.parentJobID.String,
			VideoID:      e.videoID,
			RawBucket:    r.rawBucket,
			RawObjectKey: e.rawObjectKey,
			RequestedAt:  now,
		})
		return job.FinalizeRoutingKey, payload, false, err
	default: // plan (and legacy 'transcode') jobs restart the whole pipeline
		payload, err = json.Marshal(job.TranscodeJob{
			JobID:        e.jobID,
			VideoID:      e.videoID,
			RawBucket:    r.rawBucket,
			RawObjectKey: e.rawObjectKey,
			RequestedAt:  now,
		})
		return job.TranscodeRoutingKey, payload, true, err
	}
}

// fail terminates a job that has exhausted its attempts.
func (r *Reaper) fail(ctx context.Context, tx *sql.Tx, e expiredJob) error {
	message := fmt.Sprintf("lease expired after %d attempts (max %d)", e.attempts, r.maxAttempts)

	if _, err := tx.ExecContext(ctx, `
		UPDATE video_jobs
		SET status = 'failed', last_error = $2, claimed_by = NULL, lease_expires_at = NULL, updated_at = now()
		WHERE id = $1
	`, e.jobID, message); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE videos SET status = 'failed', error_message = $2, updated_at = now() WHERE id = $1
	`, e.videoID, message); err != nil {
		return err
	}

	_, err := tx.ExecContext(ctx, `
		INSERT INTO video_events (video_id, event_type, message)
		VALUES ($1, 'video.processing.failed', $2)
	`, e.videoID, message)
	return err
}
