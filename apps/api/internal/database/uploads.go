package database

import (
	"context"
	"database/sql"
	"errors"

	"mediaflow/apps/api/internal/uploads"
	"mediaflow/apps/api/internal/videos"
)

// Milestone 6 upload-session persistence. PostgresRepository implements
// uploads.Repository alongside videos.Repository.

const uploadSessionColumns = `
	id, title, description, object_key, upload_id, part_size, total_size,
	part_count, content_type, original_filename, checksum_sha256, status,
	video_id, created_at, updated_at, expires_at
`

func (r *PostgresRepository) CreateSession(ctx context.Context, params uploads.CreateSessionParams) (uploads.Session, error) {
	row := r.db.QueryRowContext(ctx, `
		INSERT INTO upload_sessions (
			id, title, description, object_key, upload_id, part_size, total_size,
			part_count, content_type, original_filename, checksum_sha256, status, expires_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'pending', $12)
		RETURNING `+uploadSessionColumns, params.ID, params.Title, params.Description,
		params.ObjectKey, params.UploadID, params.PartSize, params.TotalSize,
		params.PartCount, params.ContentType, params.OriginalFilename,
		params.ChecksumSHA256, params.ExpiresAt)

	return scanSession(row)
}

func (r *PostgresRepository) GetSession(ctx context.Context, id string) (uploads.Session, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT `+uploadSessionColumns+`
		FROM upload_sessions
		WHERE id = $1
	`, id)

	session, err := scanSession(row)
	if errors.Is(err, sql.ErrNoRows) {
		return uploads.Session{}, uploads.ErrNotFound
	}
	if err != nil {
		return uploads.Session{}, err
	}
	return session, nil
}

func (r *PostgresRepository) SetSessionStatus(ctx context.Context, id, status string) error {
	res, err := r.db.ExecContext(ctx, `
		UPDATE upload_sessions SET status = $2, updated_at = now()
		WHERE id = $1
	`, id, status)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return uploads.ErrNotFound
	}
	return nil
}

// CompleteSession finalizes an upload in one transaction: it creates the video
// (status queued), its transcode job, the lifecycle events, and the outbox
// message — exactly like videos.CreateQueuedVideo — and marks the session
// completed and linked to the new video. Either all of it commits or none does,
// so a session can never end up completed without an enqueued job (or vice
// versa).
func (r *PostgresRepository) CompleteSession(ctx context.Context, params uploads.CompleteSessionParams) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, `
		INSERT INTO videos (
			id, title, description, status, raw_object_key,
			original_filename, content_type, size_bytes
		)
		VALUES ($1, $2, $3, 'queued', $4, $5, $6, $7)
	`, params.VideoID, params.Title, params.Description, params.RawObjectKey,
		params.OriginalFilename, params.ContentType, params.SizeBytes)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO video_jobs (id, video_id, job_type, status)
		VALUES ($1, $2, $3, 'queued')
	`, params.JobID, params.VideoID, videos.JobTypeTranscode)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO video_events (video_id, event_type, message, metadata_json)
		VALUES
			($1, 'video.upload.completed', 'Multipart upload completed to object storage.', jsonb_build_object('rawObjectKey', $2::text)),
			($1, 'video.job.queued', 'Transcode job queued.', jsonb_build_object('jobId', $3::text))
	`, params.VideoID, params.RawObjectKey, params.JobID)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO outbox_messages (exchange, routing_key, payload_json)
		VALUES ($1, $2, $3)
	`, params.OutboxExchange, params.OutboxRoutingKey, params.OutboxPayloadJSON)
	if err != nil {
		return err
	}

	res, err := tx.ExecContext(ctx, `
		UPDATE upload_sessions
		SET status = 'completed', video_id = $2, updated_at = now()
		WHERE id = $1 AND status IN ('pending', 'uploading')
	`, params.SessionID, params.VideoID)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	// Another completion won the race (session no longer pending/uploading).
	if affected == 0 {
		return uploads.ErrConflict
	}

	return tx.Commit()
}

func scanSession(row interface{ Scan(...any) error }) (uploads.Session, error) {
	var s uploads.Session
	err := row.Scan(
		&s.ID, &s.Title, &s.Description, &s.ObjectKey, &s.UploadID, &s.PartSize,
		&s.TotalSize, &s.PartCount, &s.ContentType, &s.OriginalFilename,
		&s.ChecksumSHA256, &s.Status, &s.VideoID, &s.CreatedAt, &s.UpdatedAt, &s.ExpiresAt,
	)
	if err != nil {
		return uploads.Session{}, err
	}
	return s, nil
}
