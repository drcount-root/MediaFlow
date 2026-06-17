package database

import (
	"context"
	"database/sql"
	"errors"

	"mediaflow/apps/api/internal/uploads"
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
