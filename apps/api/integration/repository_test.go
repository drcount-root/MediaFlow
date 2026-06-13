//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"mediaflow/apps/api/internal/database"
	"mediaflow/apps/api/internal/videos"
)

func TestRepositoryCreateQueuedVideoPersistsRows(t *testing.T) {
	db := openDB(t)
	truncateAll(t, db)
	repo := database.NewPostgresRepository(db)
	ctx := context.Background()

	videoID := uuid.NewString()
	jobID := uuid.NewString()
	description := "an integration video"

	created, err := repo.CreateQueuedVideo(ctx, videos.CreateQueuedVideoParams{
		VideoID:           videoID,
		JobID:             jobID,
		Title:             "Integration Title",
		Description:       &description,
		RawObjectKey:      "raw-videos/" + videoID + "/original.mp4",
		OriginalFilename:  "clip.mp4",
		ContentType:       "video/mp4",
		SizeBytes:         2048,
		OutboxExchange:    videos.VideoExchange,
		OutboxRoutingKey:  videos.TranscodeRoutingKey,
		OutboxPayloadJSON: []byte(`{"videoId":"` + videoID + `"}`),
	})
	if err != nil {
		t.Fatalf("CreateQueuedVideo: %v", err)
	}
	if created.ID != videoID {
		t.Fatalf("expected video id %s, got %s", videoID, created.ID)
	}
	if created.Status != videos.StatusQueued {
		t.Fatalf("expected status %s, got %s", videos.StatusQueued, created.Status)
	}

	// The transaction must also create the job and the two lifecycle events.
	var jobCount int
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM video_jobs WHERE id = $1 AND video_id = $2 AND status = 'queued'`,
		jobID, videoID).Scan(&jobCount); err != nil {
		t.Fatalf("query job: %v", err)
	}
	if jobCount != 1 {
		t.Fatalf("expected 1 queued job, got %d", jobCount)
	}

	var eventCount int
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM video_events WHERE video_id = $1`, videoID).Scan(&eventCount); err != nil {
		t.Fatalf("query events: %v", err)
	}
	if eventCount != 2 {
		t.Fatalf("expected 2 lifecycle events, got %d", eventCount)
	}

	got, err := repo.GetVideo(ctx, videoID)
	if err != nil {
		t.Fatalf("GetVideo: %v", err)
	}
	if got.Description == nil || *got.Description != description {
		t.Fatalf("description not round-tripped: %#v", got.Description)
	}
	if got.SizeBytes == nil || *got.SizeBytes != 2048 {
		t.Fatalf("size not round-tripped: %#v", got.SizeBytes)
	}
}

func TestRepositoryGetVideoReturnsNotFound(t *testing.T) {
	db := openDB(t)
	truncateAll(t, db)
	repo := database.NewPostgresRepository(db)

	_, err := repo.GetVideo(context.Background(), uuid.NewString())
	if err != videos.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestRepositoryListAndVariantsReflectStoredData(t *testing.T) {
	db := openDB(t)
	truncateAll(t, db)
	repo := database.NewPostgresRepository(db)
	ctx := context.Background()

	videoID := uuid.NewString()
	if _, err := repo.CreateQueuedVideo(ctx, videos.CreateQueuedVideoParams{
		VideoID:           videoID,
		JobID:             uuid.NewString(),
		Title:             "Listed",
		RawObjectKey:      "raw-videos/" + videoID + "/original.mp4",
		OriginalFilename:  "clip.mp4",
		ContentType:       "video/mp4",
		SizeBytes:         1,
		OutboxExchange:    videos.VideoExchange,
		OutboxRoutingKey:  videos.TranscodeRoutingKey,
		OutboxPayloadJSON: []byte(`{"videoId":"` + videoID + `"}`),
	}); err != nil {
		t.Fatalf("CreateQueuedVideo: %v", err)
	}

	// Variants are written by the worker in production; insert directly here so
	// GetVariants has data to read back.
	if _, err := db.ExecContext(ctx, `
		INSERT INTO video_variants (video_id, quality, width, height, bitrate, codec, playlist_key)
		VALUES ($1, '720p', 1280, 720, 2800000, 'h264', $2), ($1, '360p', 640, 360, 800000, 'h264', $3)
	`, videoID, "processed-videos/"+videoID+"/720p/index.m3u8", "processed-videos/"+videoID+"/360p/index.m3u8"); err != nil {
		t.Fatalf("insert variants: %v", err)
	}

	list, err := repo.ListVideos(ctx)
	if err != nil {
		t.Fatalf("ListVideos: %v", err)
	}
	if len(list) != 1 || list[0].ID != videoID {
		t.Fatalf("expected one listed video %s, got %#v", videoID, list)
	}

	variants, err := repo.GetVariants(ctx, videoID)
	if err != nil {
		t.Fatalf("GetVariants: %v", err)
	}
	if len(variants) != 2 {
		t.Fatalf("expected 2 variants, got %d", len(variants))
	}
	// Ordered by height DESC.
	if variants[0].Quality != "720p" || variants[1].Quality != "360p" {
		t.Fatalf("unexpected variant order: %#v", variants)
	}
}
