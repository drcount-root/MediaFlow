package database

import (
	"context"
	"database/sql"
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

func (r *Repository) CompleteJob(ctx context.Context, jobID, videoID, hlsMasterKey, thumbnailKey string, variants []job.Variant) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `DELETE FROM video_variants WHERE video_id = $1`, videoID); err != nil {
		return err
	}

	for _, variant := range variants {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO video_variants (video_id, quality, width, height, bitrate, codec, playlist_key)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
		`, videoID, variant.Quality, variant.Width, variant.Height, variant.Bitrate, variant.Codec, variant.PlaylistKey)
		if err != nil {
			return err
		}
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE videos
		SET status = 'ready', hls_master_key = $2, thumbnail_key = $3, error_message = NULL, updated_at = now()
		WHERE id = $1
	`, videoID, hlsMasterKey, thumbnailKey)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE video_jobs SET status = 'completed', updated_at = now()
		WHERE id = $1 AND video_id = $2
	`, jobID, videoID)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO video_events (video_id, event_type, message)
		VALUES
			($1, 'video.thumbnail.generated', 'Thumbnail generated and uploaded.'),
			($1, 'video.hls.generated', 'HLS renditions generated and uploaded.'),
			($1, 'video.processing.completed', 'Video processing completed.')
	`, videoID)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (r *Repository) FailJob(ctx context.Context, jobID, videoID string, cause error) error {
	message := "unknown processing error"
	if cause != nil {
		message = cause.Error()
	}
	_, err := r.db.ExecContext(ctx, `
		UPDATE videos SET status = 'failed', error_message = $3, updated_at = now()
		WHERE id = $1;
		UPDATE video_jobs SET status = 'failed', last_error = $3, updated_at = now()
		WHERE id = $2 AND video_id = $1;
		INSERT INTO video_events (video_id, event_type, message)
		VALUES ($1, 'video.processing.failed', $3);
	`, videoID, jobID, message)
	return err
}

func (r *Repository) addEvent(ctx context.Context, videoID, eventType, message string) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO video_events (video_id, event_type, message)
		VALUES ($1, $2, $3)
	`, videoID, eventType, message)
	return err
}
