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
	return uploads.NewService(repo, st, 500*1024*1024, time.Hour, time.Hour)
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
