//go:build integration

package integration

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"mediaflow/apps/api/internal/database"
	"mediaflow/apps/api/internal/storage"
	"mediaflow/apps/api/internal/uploads"
)

const partSize = 5 * 1024 * 1024 // 5 MiB — the multipart minimum for non-final parts

func newUploadService(t *testing.T) *uploads.Service {
	t.Helper()
	st, err := storage.NewMinIOStorage(infra.minioEndpoint, infra.minioAccessKey, infra.minioSecretKey, false, rawBucket, processedBucket, thumbnailBucket)
	if err != nil {
		t.Fatalf("new minio storage: %v", err)
	}
	repo := database.NewPostgresRepository(openDB(t))
	return uploads.NewService(repo, st, rawBucket, 500*1024*1024, time.Hour, time.Hour)
}

// putPart uploads bytes to a presigned part URL exactly as a browser would, and
// returns the ETag the object store assigns (needed later for completion).
func putPart(t *testing.T, url string, body []byte) string {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build part PUT: %v", err)
	}
	req.ContentLength = int64(len(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT part: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(resp.Body)
		t.Fatalf("part PUT status %d: %s", resp.StatusCode, msg)
	}
	return resp.Header.Get("ETag")
}

// TestUploadSessionPresignedPartsRoundTrip proves the M6 control plane end to
// end against real MinIO: create a session, PUT two parts directly to object
// storage via presigned URLs (bytes never touch the API), and see them reported
// back for resume.
func TestUploadSessionPresignedPartsRoundTrip(t *testing.T) {
	db := openDB(t)
	truncateAll(t, db)
	svc := newUploadService(t)
	ctx := context.Background()

	// 5 MiB + 1 KiB -> two parts (a full part and a small final part).
	total := int64(partSize + 1024)
	session, err := svc.Create(ctx, uploads.CreateParams{
		Title:            "Multipart Clip",
		OriginalFilename: "clip.mp4",
		ContentType:      "video/mp4",
		TotalSize:        total,
		PartSize:         partSize,
	})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	if session.PartCount != 2 {
		t.Fatalf("expected 2 parts, got %d", session.PartCount)
	}
	if session.Status != uploads.StatusPending {
		t.Fatalf("expected pending, got %q", session.Status)
	}

	// Upload both parts directly to object storage using presigned URLs.
	part1 := bytes.Repeat([]byte("a"), partSize)
	part2 := bytes.Repeat([]byte("b"), 1024)
	for n, body := range map[int][]byte{1: part1, 2: part2} {
		url, _, err := svc.PartURL(ctx, session.ID, n)
		if err != nil {
			t.Fatalf("part %d url: %v", n, err)
		}
		if etag := putPart(t, url, body); etag == "" {
			t.Fatalf("part %d: expected an ETag from object storage", n)
		}
	}

	// Issuing a part URL flipped the session to uploading.
	got, err := svc.Get(ctx, session.ID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if got.Status != uploads.StatusUploading {
		t.Fatalf("expected uploading after part URLs issued, got %q", got.Status)
	}
	// Both parts are reported for resume, with the right sizes.
	if len(got.UploadedParts) != 2 {
		t.Fatalf("expected 2 uploaded parts reported, got %d (%#v)", len(got.UploadedParts), got.UploadedParts)
	}
	sizes := map[int]int64{}
	for _, p := range got.UploadedParts {
		sizes[p.PartNumber] = p.Size
	}
	if sizes[1] != int64(partSize) || sizes[2] != 1024 {
		t.Fatalf("unexpected part sizes: %#v", sizes)
	}
}

// TestUploadSessionCompleteEnqueuesTranscode drives a full multipart upload
// against real MinIO and finalizes it: a video row appears (queued), an outbox
// message is written (M5 enqueue), and the session is marked completed.
func TestUploadSessionCompleteEnqueuesTranscode(t *testing.T) {
	db := openDB(t)
	truncateAll(t, db)
	svc := newUploadService(t)
	ctx := context.Background()

	total := int64(partSize + 1024)
	session, err := svc.Create(ctx, uploads.CreateParams{
		Title:            "Complete Me",
		OriginalFilename: "clip.mp4",
		ContentType:      "video/mp4",
		TotalSize:        total,
		PartSize:         partSize,
	})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}

	declared := uploadAllParts(t, svc, session.ID, map[int][]byte{
		1: bytes.Repeat([]byte("a"), partSize),
		2: bytes.Repeat([]byte("b"), 1024),
	})

	videoID, created, err := svc.Complete(ctx, session.ID, declared)
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if !created || videoID == "" {
		t.Fatalf("expected a created video, got created=%v id=%q", created, videoID)
	}

	// Video row exists, queued, pointing at the assembled object.
	var status, rawKey string
	var sizeBytes int64
	if err := db.QueryRowContext(ctx,
		`SELECT status, raw_object_key, size_bytes FROM videos WHERE id = $1`, videoID).
		Scan(&status, &rawKey, &sizeBytes); err != nil {
		t.Fatalf("load video: %v", err)
	}
	if status != "queued" {
		t.Fatalf("expected video queued, got %q", status)
	}
	if rawKey != session.ObjectKey {
		t.Fatalf("video raw key %q != session object key %q", rawKey, session.ObjectKey)
	}
	if sizeBytes != total {
		t.Fatalf("expected assembled size %d, got %d", total, sizeBytes)
	}

	// Exactly one outbox message was written (the transcode enqueue).
	var outbox int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM outbox_messages`).Scan(&outbox); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	if outbox != 1 {
		t.Fatalf("expected 1 outbox message, got %d", outbox)
	}

	// Session is completed and linked to the video.
	var sessStatus string
	var sessVideoID *string
	if err := db.QueryRowContext(ctx,
		`SELECT status, video_id FROM upload_sessions WHERE id = $1`, session.ID).
		Scan(&sessStatus, &sessVideoID); err != nil {
		t.Fatalf("load session: %v", err)
	}
	if sessStatus != "completed" || sessVideoID == nil || *sessVideoID != videoID {
		t.Fatalf("session not linked: status=%q videoID=%v", sessStatus, sessVideoID)
	}
}

// TestUploadSessionCompleteRejectsTamperedPart proves the tampered-part drill: a
// declared ETag that doesn't match the stored part fails completion cleanly —
// no video, no outbox row, session left resumable.
func TestUploadSessionCompleteRejectsTamperedPart(t *testing.T) {
	db := openDB(t)
	truncateAll(t, db)
	svc := newUploadService(t)
	ctx := context.Background()

	session, err := svc.Create(ctx, uploads.CreateParams{
		Title:            "Tampered",
		OriginalFilename: "clip.mp4",
		ContentType:      "video/mp4",
		TotalSize:        int64(partSize + 1024),
		PartSize:         partSize,
	})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}

	declared := uploadAllParts(t, svc, session.ID, map[int][]byte{
		1: bytes.Repeat([]byte("a"), partSize),
		2: bytes.Repeat([]byte("b"), 1024),
	})
	// Forge the second part's ETag.
	for i := range declared {
		if declared[i].PartNumber == 2 {
			declared[i].ETag = "\"deadbeefdeadbeefdeadbeefdeadbeef\""
		}
	}

	if _, _, err := svc.Complete(ctx, session.ID, declared); !errors.Is(err, uploads.ErrChecksumMismatch) {
		t.Fatalf("expected ErrChecksumMismatch, got %v", err)
	}

	var videoCount, outboxCount int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM videos`).Scan(&videoCount); err != nil {
		t.Fatalf("count videos: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM outbox_messages`).Scan(&outboxCount); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	if videoCount != 0 || outboxCount != 0 {
		t.Fatalf("tampered completion must create nothing; videos=%d outbox=%d", videoCount, outboxCount)
	}

	// The session is still resumable (not completed).
	var sessStatus string
	if err := db.QueryRowContext(ctx,
		`SELECT status FROM upload_sessions WHERE id = $1`, session.ID).Scan(&sessStatus); err != nil {
		t.Fatalf("load session: %v", err)
	}
	if sessStatus == "completed" {
		t.Fatal("session must not be completed after a tampered-part failure")
	}
}

// uploadAllParts PUTs each part via a presigned URL and returns the declared
// (partNumber, etag) list the client would send to complete.
func uploadAllParts(t *testing.T, svc *uploads.Service, sessionID string, parts map[int][]byte) []uploads.CompletePart {
	t.Helper()
	declared := make([]uploads.CompletePart, 0, len(parts))
	for n, body := range parts {
		url, _, err := svc.PartURL(context.Background(), sessionID, n)
		if err != nil {
			t.Fatalf("part %d url: %v", n, err)
		}
		etag := putPart(t, url, body)
		if etag == "" {
			t.Fatalf("part %d: missing ETag", n)
		}
		declared = append(declared, uploads.CompletePart{PartNumber: n, ETag: etag})
	}
	return declared
}

// TestUploadSessionAbortReleasesMultipart proves abort tears down the multipart
// upload in object storage and blocks further part URLs.
func TestUploadSessionAbortReleasesMultipart(t *testing.T) {
	db := openDB(t)
	truncateAll(t, db)
	svc := newUploadService(t)
	ctx := context.Background()

	session, err := svc.Create(ctx, uploads.CreateParams{
		Title:            "To Abort",
		OriginalFilename: "clip.mp4",
		ContentType:      "video/mp4",
		TotalSize:        int64(partSize + 1024),
		PartSize:         partSize,
	})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}

	// Upload one part, then abort.
	url, _, err := svc.PartURL(ctx, session.ID, 1)
	if err != nil {
		t.Fatalf("part url: %v", err)
	}
	putPart(t, url, bytes.Repeat([]byte("a"), partSize))

	if err := svc.Abort(ctx, session.ID); err != nil {
		t.Fatalf("abort: %v", err)
	}

	// The session is aborted and refuses new part URLs.
	after, err := svc.Get(ctx, session.ID)
	if err != nil {
		t.Fatalf("get after abort: %v", err)
	}
	if after.Status != uploads.StatusAborted {
		t.Fatalf("expected aborted, got %q", after.Status)
	}
	if _, _, err := svc.PartURL(ctx, session.ID, 1); !errors.Is(err, uploads.ErrConflict) {
		t.Fatalf("expected ErrConflict after abort, got %v", err)
	}
}
