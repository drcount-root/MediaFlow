package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
	"mediaflow/apps/api/internal/videos"
)

type PostgresRepository struct {
	db *sql.DB
}

func Open(ctx context.Context, databaseURL string) (*sql.DB, error) {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

func NewPostgresRepository(db *sql.DB) *PostgresRepository {
	return &PostgresRepository{db: db}
}

func (r *PostgresRepository) CreateQueuedVideo(ctx context.Context, params videos.CreateQueuedVideoParams) (videos.Video, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return videos.Video{}, err
	}
	defer tx.Rollback()

	row := tx.QueryRowContext(ctx, `
		INSERT INTO videos (
			id, title, description, status, raw_object_key,
			original_filename, content_type, size_bytes, idempotency_key
		)
		VALUES ($1, $2, $3, 'queued', $4, $5, $6, $7, $8)
		RETURNING id, title, description, status, raw_object_key, hls_master_key,
			thumbnail_key, duration_seconds, original_filename, content_type, size_bytes,
			error_message, created_at, updated_at
	`, params.VideoID, params.Title, params.Description, params.RawObjectKey, params.OriginalFilename, params.ContentType, params.SizeBytes, params.IdempotencyKey)

	video, err := scanVideo(row)
	if isUniqueViolation(err) {
		return videos.Video{}, videos.ErrDuplicateKey
	}
	if err != nil {
		return videos.Video{}, err
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO video_jobs (id, video_id, job_type, status)
		VALUES ($1, $2, $3, 'queued')
	`, params.JobID, params.VideoID, videos.JobTypeTranscode)
	if err != nil {
		return videos.Video{}, err
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO video_events (video_id, event_type, message, metadata_json)
		VALUES
			($1, 'video.upload.completed', 'Original video uploaded to object storage.', jsonb_build_object('rawObjectKey', $2::text)),
			($1, 'video.job.queued', 'Transcode job queued.', jsonb_build_object('jobId', $3::text))
	`, params.VideoID, params.RawObjectKey, params.JobID)
	if err != nil {
		return videos.Video{}, err
	}

	// Outbox row in the same transaction: either everything commits (video + job
	// + events + the message to publish) or nothing does. No dual-write to the
	// broker on the request path.
	_, err = tx.ExecContext(ctx, `
		INSERT INTO outbox_messages (exchange, routing_key, payload_json)
		VALUES ($1, $2, $3)
	`, params.OutboxExchange, params.OutboxRoutingKey, params.OutboxPayloadJSON)
	if err != nil {
		return videos.Video{}, err
	}

	if err := tx.Commit(); err != nil {
		return videos.Video{}, err
	}

	return video, nil
}

func (r *PostgresRepository) GetVideoByIdempotencyKey(ctx context.Context, key string) (videos.Video, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, title, description, status, raw_object_key, hls_master_key,
			thumbnail_key, duration_seconds, original_filename, content_type, size_bytes,
			error_message, created_at, updated_at
		FROM videos
		WHERE idempotency_key = $1
	`, key)

	video, err := scanVideo(row)
	if errors.Is(err, sql.ErrNoRows) {
		return videos.Video{}, videos.ErrNotFound
	}
	if err != nil {
		return videos.Video{}, err
	}

	return video, nil
}

func (r *PostgresRepository) ListVideos(ctx context.Context) ([]videos.Video, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, title, description, status, raw_object_key, hls_master_key,
			thumbnail_key, duration_seconds, original_filename, content_type, size_bytes,
			error_message, created_at, updated_at
		FROM videos
		ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []videos.Video
	for rows.Next() {
		video, err := scanVideo(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, video)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return items, nil
}

func (r *PostgresRepository) GetVideo(ctx context.Context, id string) (videos.Video, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, title, description, status, raw_object_key, hls_master_key,
			thumbnail_key, duration_seconds, original_filename, content_type, size_bytes,
			error_message, created_at, updated_at
		FROM videos
		WHERE id = $1
	`, id)

	video, err := scanVideo(row)
	if errors.Is(err, sql.ErrNoRows) {
		return videos.Video{}, videos.ErrNotFound
	}
	if err != nil {
		return videos.Video{}, err
	}

	return video, nil
}

func (r *PostgresRepository) GetVariants(ctx context.Context, videoID string) ([]videos.Variant, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT quality, width, height, bitrate, COALESCE(codec, ''), playlist_key
		FROM video_variants
		WHERE video_id = $1
		ORDER BY height DESC
	`, videoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var variants []videos.Variant
	for rows.Next() {
		var variant videos.Variant
		if err := rows.Scan(&variant.Quality, &variant.Width, &variant.Height, &variant.Bitrate, &variant.Codec, &variant.PlaylistKey); err != nil {
			return nil, err
		}
		variants = append(variants, variant)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return variants, nil
}

type videoScanner interface {
	Scan(dest ...any) error
}

func scanVideo(scanner videoScanner) (videos.Video, error) {
	var video videos.Video
	var description sql.NullString
	var rawObjectKey sql.NullString
	var hlsMasterKey sql.NullString
	var thumbnailKey sql.NullString
	var durationSeconds sql.NullFloat64
	var originalFilename sql.NullString
	var contentType sql.NullString
	var sizeBytes sql.NullInt64
	var errorMessage sql.NullString

	err := scanner.Scan(
		&video.ID,
		&video.Title,
		&description,
		&video.Status,
		&rawObjectKey,
		&hlsMasterKey,
		&thumbnailKey,
		&durationSeconds,
		&originalFilename,
		&contentType,
		&sizeBytes,
		&errorMessage,
		&video.CreatedAt,
		&video.UpdatedAt,
	)
	if err != nil {
		return videos.Video{}, fmt.Errorf("scan video: %w", err)
	}

	video.Description = nullableString(description)
	video.RawObjectKey = nullableString(rawObjectKey)
	video.HLSMasterKey = nullableString(hlsMasterKey)
	video.ThumbnailKey = nullableString(thumbnailKey)
	video.DurationSeconds = nullableFloat64(durationSeconds)
	video.OriginalFilename = nullableString(originalFilename)
	video.ContentType = nullableString(contentType)
	video.SizeBytes = nullableInt64(sizeBytes)
	video.ErrorMessage = nullableString(errorMessage)

	return video, nil
}

// isUniqueViolation reports whether err is a Postgres unique-constraint error
// (SQLSTATE 23505), e.g. a concurrent upload racing on the same idempotency key.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func nullableString(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	return &value.String
}

func nullableFloat64(value sql.NullFloat64) *float64 {
	if !value.Valid {
		return nil
	}
	return &value.Float64
}

func nullableInt64(value sql.NullInt64) *int64 {
	if !value.Valid {
		return nil
	}
	return &value.Int64
}
