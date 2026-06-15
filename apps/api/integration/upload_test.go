//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
	"mediaflow/apps/api/internal/database"
	"mediaflow/apps/api/internal/storage"
	"mediaflow/apps/api/internal/videos"
)

// TestUploadStoresAndWritesOutbox drives the real upload service against live
// MinIO + Postgres: the raw object lands in storage and the transcode job is
// written to the outbox in the same transaction as the video — and critically,
// the upload path never touches RabbitMQ (no broker is involved here at all).
func TestUploadStoresAndWritesOutbox(t *testing.T) {
	db := openDB(t)
	truncateAll(t, db)
	ctx := context.Background()

	repo := database.NewPostgresRepository(db)
	objStore, err := storage.NewMinIOStorage(
		infra.minioEndpoint, infra.minioAccessKey, infra.minioSecretKey, false,
		rawBucket, processedBucket, thumbnailBucket,
	)
	if err != nil {
		t.Fatalf("new minio storage: %v", err)
	}

	service := videos.NewService(repo, objStore, rawBucket, 0)

	contents := "fake mp4 bytes for integration"
	created, isNew, err := service.Upload(ctx, videos.UploadParams{
		Title:            "Pipeline Upload",
		OriginalFilename: "pipeline.mp4",
		ContentType:      "video/mp4",
		SizeBytes:        int64(len(contents)),
		Body:             strings.NewReader(contents),
	})
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if !isNew {
		t.Fatal("expected a freshly created video")
	}
	if created.Status != videos.StatusQueued {
		t.Fatalf("expected queued status, got %s", created.Status)
	}

	// Raw object stored.
	stored, err := service.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if stored.RawObjectKey == nil {
		t.Fatal("expected raw object key on stored video")
	}
	client, err := minioClient()
	if err != nil {
		t.Fatalf("minio client: %v", err)
	}
	stat, err := client.StatObject(ctx, rawBucket, *stored.RawObjectKey, minio.StatObjectOptions{})
	if err != nil {
		t.Fatalf("stat raw object %q: %v", *stored.RawObjectKey, err)
	}
	if stat.Size != int64(len(contents)) {
		t.Fatalf("expected raw object size %d, got %d", len(contents), stat.Size)
	}

	// Exactly one unpublished outbox row, carrying the transcode job for this video.
	var (
		exchange   string
		routingKey string
		payload    []byte
		published  *string
	)
	err = db.QueryRowContext(ctx, `
		SELECT exchange, routing_key, payload_json, published_at::text
		FROM outbox_messages
	`).Scan(&exchange, &routingKey, &payload, &published)
	if err != nil {
		t.Fatalf("read outbox row: %v", err)
	}
	if published != nil {
		t.Fatalf("expected unpublished outbox row, got published_at=%v", *published)
	}
	if exchange != videos.VideoExchange || routingKey != videos.TranscodeRoutingKey {
		t.Fatalf("unexpected outbox routing: %s/%s", exchange, routingKey)
	}

	var job videos.TranscodeJob
	if err := json.Unmarshal(payload, &job); err != nil {
		t.Fatalf("decode outbox payload: %v", err)
	}
	if job.VideoID != created.ID || job.RawObjectKey != *stored.RawObjectKey {
		t.Fatalf("outbox job does not match video: %#v", job)
	}
}

// TestUploadIdempotencyKeyReplays proves that replaying an upload with the same
// Idempotency-Key returns the original video and creates no duplicate, against a
// real Postgres unique index.
func TestUploadIdempotencyKeyReplays(t *testing.T) {
	db := openDB(t)
	truncateAll(t, db)
	ctx := context.Background()

	repo := database.NewPostgresRepository(db)
	objStore, err := storage.NewMinIOStorage(
		infra.minioEndpoint, infra.minioAccessKey, infra.minioSecretKey, false,
		rawBucket, processedBucket, thumbnailBucket,
	)
	if err != nil {
		t.Fatalf("new minio storage: %v", err)
	}
	service := videos.NewService(repo, objStore, rawBucket, 0)

	params := func() videos.UploadParams {
		return videos.UploadParams{
			Title:            "Replay",
			OriginalFilename: "replay.mp4",
			ContentType:      "video/mp4",
			SizeBytes:        4,
			Body:             strings.NewReader("data"),
			IdempotencyKey:   "key-xyz",
		}
	}

	first, isNew, err := service.Upload(ctx, params())
	if err != nil {
		t.Fatalf("first upload: %v", err)
	}
	if !isNew {
		t.Fatal("first upload should be new")
	}

	second, isNew, err := service.Upload(ctx, params())
	if err != nil {
		t.Fatalf("replay upload: %v", err)
	}
	if isNew {
		t.Fatal("replay should not create a new video")
	}
	if second.ID != first.ID {
		t.Fatalf("replay returned a different video: %q vs %q", second.ID, first.ID)
	}

	var count int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM videos`).Scan(&count); err != nil {
		t.Fatalf("count videos: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected exactly one video after replay, got %d", count)
	}
}
