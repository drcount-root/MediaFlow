package uploads

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	mib            = 1024 * 1024
	testMaxUpload  = 500 * mib
	testSessionTTL = 24 * time.Hour
	testPartTTL    = time.Hour
)

type fakeRepo struct {
	sessions    map[string]Session
	createCalls int
	statusCalls int
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{sessions: map[string]Session{}}
}

func (f *fakeRepo) CreateSession(_ context.Context, p CreateSessionParams) (Session, error) {
	f.createCalls++
	s := Session{
		ID:               p.ID,
		Title:            p.Title,
		Description:      p.Description,
		ObjectKey:        p.ObjectKey,
		UploadID:         p.UploadID,
		Status:           StatusPending,
		PartSize:         p.PartSize,
		TotalSize:        p.TotalSize,
		PartCount:        p.PartCount,
		ContentType:      p.ContentType,
		OriginalFilename: p.OriginalFilename,
		ChecksumSHA256:   p.ChecksumSHA256,
		ExpiresAt:        p.ExpiresAt,
	}
	f.sessions[p.ID] = s
	return s, nil
}

func (f *fakeRepo) GetSession(_ context.Context, id string) (Session, error) {
	s, ok := f.sessions[id]
	if !ok {
		return Session{}, ErrNotFound
	}
	return s, nil
}

func (f *fakeRepo) SetSessionStatus(_ context.Context, id, status string) error {
	f.statusCalls++
	s, ok := f.sessions[id]
	if !ok {
		return ErrNotFound
	}
	s.Status = status
	f.sessions[id] = s
	return nil
}

func (f *fakeRepo) ListExpiredSessions(_ context.Context, limit int) ([]Session, error) {
	var out []Session
	now := time.Now().UTC()
	for _, s := range f.sessions {
		if (s.Status == StatusPending || s.Status == StatusUploading) && s.ExpiresAt.Before(now) {
			out = append(out, s)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (f *fakeRepo) ExpireSession(_ context.Context, id string) (bool, error) {
	s, ok := f.sessions[id]
	if !ok {
		return false, ErrNotFound
	}
	if s.Status != StatusPending && s.Status != StatusUploading {
		return false, nil
	}
	s.Status = StatusExpired
	f.sessions[id] = s
	return true, nil
}

func (f *fakeRepo) CompleteSession(_ context.Context, p CompleteSessionParams) error {
	s, ok := f.sessions[p.SessionID]
	if !ok {
		return ErrNotFound
	}
	if s.Status != StatusPending && s.Status != StatusUploading {
		return ErrConflict
	}
	s.Status = StatusCompleted
	s.VideoID = &p.VideoID
	f.sessions[p.SessionID] = s
	return nil
}

type fakeStorage struct {
	initiated    int
	aborted      int
	completed    int
	parts        []UploadedPart
	failInitiate bool
}

func (f *fakeStorage) InitiateMultipart(_ context.Context, _, _ string) (string, error) {
	if f.failInitiate {
		return "", errors.New("boom")
	}
	f.initiated++
	return "upload-123", nil
}

func (f *fakeStorage) PresignPartURL(_ context.Context, _, _ string, partNumber int, _ time.Duration) (string, error) {
	return "https://minio.local/part?partNumber=" + strconv.Itoa(partNumber), nil
}

func (f *fakeStorage) ListParts(_ context.Context, _, _ string) ([]UploadedPart, error) {
	return f.parts, nil
}

func (f *fakeStorage) CompleteMultipart(_ context.Context, _, _ string, _ []CompletePart) error {
	f.completed++
	return nil
}

func (f *fakeStorage) AbortMultipart(_ context.Context, _, _ string) error {
	f.aborted++
	return nil
}

func newService(repo Repository, storage ObjectStorage) *Service {
	return NewService(repo, storage, "mediaflow-raw", testMaxUpload, testSessionTTL, testPartTTL)
}

func validCreate() CreateParams {
	return CreateParams{
		Title:            "My Clip",
		OriginalFilename: "clip.mp4",
		ContentType:      "video/mp4",
		TotalSize:        10 * mib,
		PartSize:         5 * mib,
	}
}

func TestCreateInitiatesMultipartAndComputesPartCount(t *testing.T) {
	repo := newFakeRepo()
	storage := &fakeStorage{}
	svc := newService(repo, storage)

	session, err := svc.Create(context.Background(), validCreate())
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if storage.initiated != 1 {
		t.Fatalf("expected multipart initiated once, got %d", storage.initiated)
	}
	if session.PartCount != 2 {
		t.Fatalf("expected 2 parts for 10MiB/5MiB, got %d", session.PartCount)
	}
	if session.Status != StatusPending {
		t.Fatalf("expected pending, got %q", session.Status)
	}
	if session.ObjectKey == "" {
		t.Fatal("expected an object key")
	}
	// The MinIO upload id is tracked server-side but must never be serialized to
	// the client (part URLs are presigned for it).
	encoded, err := json.Marshal(session)
	if err != nil {
		t.Fatalf("marshal session: %v", err)
	}
	if strings.Contains(string(encoded), "upload-123") {
		t.Fatalf("upload id must not appear in the JSON sent to clients: %s", encoded)
	}
}

func TestCreateRejectsInvalidInput(t *testing.T) {
	cases := map[string]func(CreateParams) CreateParams{
		"empty title":      func(p CreateParams) CreateParams { p.Title = "  "; return p },
		"zero total size":  func(p CreateParams) CreateParams { p.TotalSize = 0; return p },
		"zero part size":   func(p CreateParams) CreateParams { p.PartSize = 0; return p },
		"undersized parts": func(p CreateParams) CreateParams { p.PartSize = 1 * mib; return p }, // 10 parts < 5MiB
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			svc := newService(newFakeRepo(), &fakeStorage{})
			if _, err := svc.Create(context.Background(), mutate(validCreate())); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("expected ErrInvalidInput, got %v", err)
			}
		})
	}
}

func TestCreateRejectsUnsupportedMedia(t *testing.T) {
	svc := newService(newFakeRepo(), &fakeStorage{})
	p := validCreate()
	p.ContentType = "image/png"
	p.OriginalFilename = "clip.png"
	if _, err := svc.Create(context.Background(), p); !errors.Is(err, ErrUnsupportedMedia) {
		t.Fatalf("expected ErrUnsupportedMedia, got %v", err)
	}
}

func TestCreateRejectsOversize(t *testing.T) {
	svc := newService(newFakeRepo(), &fakeStorage{})
	p := validCreate()
	p.TotalSize = testMaxUpload + 1
	if _, err := svc.Create(context.Background(), p); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("expected ErrTooLarge, got %v", err)
	}
}

func TestCreateDoesNotPersistWhenInitiateFails(t *testing.T) {
	repo := newFakeRepo()
	svc := newService(repo, &fakeStorage{failInitiate: true})
	if _, err := svc.Create(context.Background(), validCreate()); err == nil {
		t.Fatal("expected error when multipart initiate fails")
	}
	if repo.createCalls != 0 {
		t.Fatalf("session must not be persisted when initiate fails; createCalls=%d", repo.createCalls)
	}
}

func TestPartURLFlipsPendingToUploading(t *testing.T) {
	repo := newFakeRepo()
	storage := &fakeStorage{}
	svc := newService(repo, storage)
	session, _ := svc.Create(context.Background(), validCreate())

	url, _, err := svc.PartURL(context.Background(), session.ID, 1)
	if err != nil {
		t.Fatalf("part url: %v", err)
	}
	if url == "" {
		t.Fatal("expected a presigned url")
	}
	if got := repo.sessions[session.ID].Status; got != StatusUploading {
		t.Fatalf("expected uploading after first part url, got %q", got)
	}
}

func TestPartURLRejectsOutOfRangePart(t *testing.T) {
	repo := newFakeRepo()
	svc := newService(repo, &fakeStorage{})
	session, _ := svc.Create(context.Background(), validCreate()) // 2 parts

	for _, n := range []int{0, 3} {
		if _, _, err := svc.PartURL(context.Background(), session.ID, n); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("part %d: expected ErrInvalidInput, got %v", n, err)
		}
	}
}

func TestPartURLConflictOnAbortedSession(t *testing.T) {
	repo := newFakeRepo()
	svc := newService(repo, &fakeStorage{})
	session, _ := svc.Create(context.Background(), validCreate())
	_ = svc.Abort(context.Background(), session.ID)

	if _, _, err := svc.PartURL(context.Background(), session.ID, 1); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict on aborted session, got %v", err)
	}
}

func TestGetPopulatesUploadedParts(t *testing.T) {
	repo := newFakeRepo()
	storage := &fakeStorage{parts: []UploadedPart{{PartNumber: 1, ETag: "abc", Size: 5 * mib}}}
	svc := newService(repo, storage)
	session, _ := svc.Create(context.Background(), validCreate())

	got, err := svc.Get(context.Background(), session.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(got.UploadedParts) != 1 || got.UploadedParts[0].PartNumber != 1 {
		t.Fatalf("expected one uploaded part for resume, got %#v", got.UploadedParts)
	}
}

func TestAbortReleasesMultipartAndIsIdempotent(t *testing.T) {
	repo := newFakeRepo()
	storage := &fakeStorage{}
	svc := newService(repo, storage)
	session, _ := svc.Create(context.Background(), validCreate())

	if err := svc.Abort(context.Background(), session.ID); err != nil {
		t.Fatalf("abort: %v", err)
	}
	if storage.aborted != 1 {
		t.Fatalf("expected multipart aborted once, got %d", storage.aborted)
	}
	if got := repo.sessions[session.ID].Status; got != StatusAborted {
		t.Fatalf("expected aborted status, got %q", got)
	}
	// Second abort is a no-op (no extra storage call, no error).
	if err := svc.Abort(context.Background(), session.ID); err != nil {
		t.Fatalf("second abort: %v", err)
	}
	if storage.aborted != 1 {
		t.Fatalf("second abort should be a no-op, aborted=%d", storage.aborted)
	}
}

// completeFixture creates a 2-part session and stocks storage with two matching
// 5 MiB parts so the declared parts line up with what storage "holds".
func completeFixture(t *testing.T) (*Service, *fakeRepo, *fakeStorage, string, []CompletePart) {
	t.Helper()
	repo := newFakeRepo()
	storage := &fakeStorage{parts: []UploadedPart{
		{PartNumber: 1, ETag: "etag1", Size: 5 * mib},
		{PartNumber: 2, ETag: "etag2", Size: 5 * mib},
	}}
	svc := newService(repo, storage)
	session, err := svc.Create(context.Background(), validCreate()) // total 10MiB, 2 parts
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	declared := []CompletePart{{PartNumber: 1, ETag: "etag1"}, {PartNumber: 2, ETag: "etag2"}}
	return svc, repo, storage, session.ID, declared
}

func TestCompleteFinalizesAndEnqueues(t *testing.T) {
	svc, repo, storage, id, declared := completeFixture(t)

	videoID, created, err := svc.Complete(context.Background(), id, declared)
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if !created {
		t.Fatal("expected created=true on first completion")
	}
	if videoID == "" {
		t.Fatal("expected a video id")
	}
	if storage.completed != 1 {
		t.Fatalf("expected multipart completed once, got %d", storage.completed)
	}
	if got := repo.sessions[id].Status; got != StatusCompleted {
		t.Fatalf("expected session completed, got %q", got)
	}
}

func TestCompleteReplaysWhenAlreadyCompleted(t *testing.T) {
	svc, _, storage, id, declared := completeFixture(t)

	first, _, err := svc.Complete(context.Background(), id, declared)
	if err != nil {
		t.Fatalf("first complete: %v", err)
	}
	second, created, err := svc.Complete(context.Background(), id, declared)
	if err != nil {
		t.Fatalf("second complete: %v", err)
	}
	if created {
		t.Fatal("expected created=false on replay")
	}
	if second != first {
		t.Fatalf("replay returned a different video id: %s vs %s", second, first)
	}
	if storage.completed != 1 {
		t.Fatalf("replay must not re-finalize the multipart, completed=%d", storage.completed)
	}
}

func TestCompleteRejectsChecksumMismatch(t *testing.T) {
	svc, _, storage, id, _ := completeFixture(t)
	bad := []CompletePart{{PartNumber: 1, ETag: "etag1"}, {PartNumber: 2, ETag: "WRONG"}}

	if _, _, err := svc.Complete(context.Background(), id, bad); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("expected ErrChecksumMismatch, got %v", err)
	}
	if storage.completed != 0 {
		t.Fatalf("multipart must not be finalized on checksum mismatch, completed=%d", storage.completed)
	}
}

func TestCompleteRejectsMissingParts(t *testing.T) {
	svc, _, _, id, _ := completeFixture(t)
	short := []CompletePart{{PartNumber: 1, ETag: "etag1"}}

	if _, _, err := svc.Complete(context.Background(), id, short); !errors.Is(err, ErrIncompleteUpload) {
		t.Fatalf("expected ErrIncompleteUpload, got %v", err)
	}
}

func TestCompleteRejectsSizeMismatch(t *testing.T) {
	svc, _, storage, id, declared := completeFixture(t)
	// Stored part 2 is smaller than declared -> assembled size != declared total.
	storage.parts = []UploadedPart{
		{PartNumber: 1, ETag: "etag1", Size: 5 * mib},
		{PartNumber: 2, ETag: "etag2", Size: 4 * mib},
	}

	if _, _, err := svc.Complete(context.Background(), id, declared); !errors.Is(err, ErrSizeMismatch) {
		t.Fatalf("expected ErrSizeMismatch, got %v", err)
	}
	if storage.completed != 0 {
		t.Fatalf("multipart must not be finalized on size mismatch, completed=%d", storage.completed)
	}
}

func TestGetUnknownSessionReturnsNotFound(t *testing.T) {
	svc := newService(newFakeRepo(), &fakeStorage{})
	if _, err := svc.Get(context.Background(), "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
