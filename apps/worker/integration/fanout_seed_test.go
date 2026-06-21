//go:build integration

package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	"mediaflow/apps/worker/internal/job"
)

// Shared seeding helpers for the fan-out aggregation/partial-failure tests, which
// drive the repository directly against a job hierarchy rather than running ffmpeg.

func seedProcessingVideo(t *testing.T, db *sql.DB, videoID string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO videos (id, title, status, raw_object_key, original_filename, content_type, size_bytes)
		VALUES ($1, 'Fanout Test', 'processing', $2, 'fixture.mp4', 'video/mp4', 0)
	`, videoID, "raw-videos/"+videoID+"/original.mp4"); err != nil {
		t.Fatalf("seed video: %v", err)
	}
}

// seedPlanCompleted inserts a plan job already fanned out: completed, with the
// pending-rendition counter stamped on it.
func seedPlanCompleted(t *testing.T, db *sql.DB, planJobID, videoID string, pending int) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO video_jobs (id, video_id, job_type, status, pending_renditions)
		VALUES ($1, $2, 'plan', 'completed', $3)
	`, planJobID, videoID, pending); err != nil {
		t.Fatalf("seed plan job: %v", err)
	}
}

// seedRendition inserts a rendition job (with a spec for the given quality) in the
// requested status and returns its generated id.
func seedRendition(t *testing.T, db *sql.DB, videoID, planJobID, quality, status string) string {
	t.Helper()
	specJSON, err := json.Marshal(job.RenditionSpec{
		Quality: quality, Width: 1280, Height: 720, Bitrate: 2800000, Codec: "h264",
	})
	if err != nil {
		t.Fatalf("marshal spec: %v", err)
	}
	var id string
	if err := db.QueryRowContext(context.Background(), `
		INSERT INTO video_jobs (video_id, job_type, status, parent_job_id, rendition_spec)
		VALUES ($1, 'rendition', $2, $3, $4)
		RETURNING id
	`, videoID, status, planJobID, specJSON).Scan(&id); err != nil {
		t.Fatalf("seed rendition: %v", err)
	}
	return id
}

func insertVariant(t *testing.T, db *sql.DB, videoID, quality string, bitrate int) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO video_variants (video_id, quality, width, height, bitrate, codec, playlist_key)
		VALUES ($1, $2, 1280, 720, $3, 'h264', $4)
	`, videoID, quality, bitrate, "processed-videos/"+videoID+"/"+quality+"/index.m3u8"); err != nil {
		t.Fatalf("insert variant: %v", err)
	}
}

func assertJobStatus(t *testing.T, db *sql.DB, jobID, want string) {
	t.Helper()
	var got string
	if err := db.QueryRowContext(context.Background(), `SELECT status FROM video_jobs WHERE id = $1`, jobID).Scan(&got); err != nil {
		t.Fatalf("read job %s: %v", jobID, err)
	}
	if got != want {
		t.Fatalf("job %s status = %q, want %q", jobID, got, want)
	}
}

// renditionVariant builds the variant a completed rendition would record.
func renditionVariant(videoID, quality string, bitrate int) job.Variant {
	return job.Variant{
		Quality: quality, Width: 1280, Height: 720, Bitrate: bitrate, Codec: "h264",
		PlaylistKey: "processed-videos/" + videoID + "/" + quality + "/index.m3u8",
	}
}
