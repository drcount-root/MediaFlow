//go:build integration

package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"mediaflow/apps/api/internal/database"
	"mediaflow/apps/api/internal/queue"
	"mediaflow/apps/api/internal/storage"
	"mediaflow/apps/api/internal/videos"
)

// TestUploadStoresQueuesAndPublishes drives the real upload service against
// live Postgres, MinIO, and RabbitMQ: the raw object lands in storage, the
// video row is queued, and a transcode job is published — the store→queue half
// of the pipeline.
func TestUploadStoresQueuesAndPublishes(t *testing.T) {
	db := openDB(t)
	truncateAll(t, db)
	drainTranscodeQueue(t)
	ctx := context.Background()

	repo := database.NewPostgresRepository(db)
	objStore, err := storage.NewMinIOStorage(
		infra.minioEndpoint, infra.minioAccessKey, infra.minioSecretKey, false,
		rawBucket, processedBucket, thumbnailBucket,
	)
	if err != nil {
		t.Fatalf("new minio storage: %v", err)
	}
	publisher, err := queue.NewRabbitPublisher(infra.rabbitURL)
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	t.Cleanup(func() { _ = publisher.Close() })

	service := videos.NewService(repo, objStore, publisher, rawBucket, 0)

	contents := "fake mp4 bytes for integration"
	created, err := service.Upload(ctx, videos.UploadParams{
		Title:            "Pipeline Upload",
		OriginalFilename: "pipeline.mp4",
		ContentType:      "video/mp4",
		SizeBytes:        int64(len(contents)),
		Body:             strings.NewReader(contents),
	})
	if err != nil {
		t.Fatalf("Upload: %v", err)
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

	// Transcode job published for this video.
	job := consumeOneTranscodeJob(t, 10*time.Second)
	if job.VideoID != created.ID {
		t.Fatalf("expected published job for video %s, got %s", created.ID, job.VideoID)
	}
	if job.RawObjectKey != *stored.RawObjectKey {
		t.Fatalf("published job key %q does not match stored key %q", job.RawObjectKey, *stored.RawObjectKey)
	}
}
