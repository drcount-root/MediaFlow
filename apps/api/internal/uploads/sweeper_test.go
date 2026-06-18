package uploads

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

func newSweeper(repo Repository, storage ObjectStorage) *Sweeper {
	return NewSweeper(repo, storage, slog.New(slog.NewTextHandler(io.Discard, nil)), time.Minute, 100)
}

// seedSession inserts a session directly with the given status and deadline so
// tests can stage expiry scenarios without driving the full create flow.
func seedSession(repo *fakeRepo, id, status string, expiresAt time.Time) {
	repo.sessions[id] = Session{
		ID:        id,
		ObjectKey: "raw-uploads/" + id + "/original.mp4",
		UploadID:  "upload-" + id,
		Status:    status,
		ExpiresAt: expiresAt,
	}
}

func TestSweepExpiresOpenSessionsAndAbortsMultipart(t *testing.T) {
	repo := newFakeRepo()
	storage := &fakeStorage{}
	seedSession(repo, "old-pending", StatusPending, time.Now().Add(-time.Hour))
	seedSession(repo, "old-uploading", StatusUploading, time.Now().Add(-time.Minute))

	n, err := newSweeper(repo, storage).SweepOnce(context.Background())
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected 2 sessions swept, got %d", n)
	}
	if storage.aborted != 2 {
		t.Fatalf("expected 2 multipart aborts, got %d", storage.aborted)
	}
	for _, id := range []string{"old-pending", "old-uploading"} {
		if got := repo.sessions[id].Status; got != StatusExpired {
			t.Fatalf("session %q: expected expired, got %q", id, got)
		}
	}
}

func TestSweepLeavesUnexpiredAndTerminalSessions(t *testing.T) {
	repo := newFakeRepo()
	storage := &fakeStorage{}
	seedSession(repo, "fresh", StatusUploading, time.Now().Add(time.Hour)) // not yet expired
	seedSession(repo, "done", StatusCompleted, time.Now().Add(-time.Hour)) // terminal
	seedSession(repo, "gone", StatusAborted, time.Now().Add(-time.Hour))   // terminal
	seedSession(repo, "stale", StatusPending, time.Now().Add(-time.Hour))  // should expire

	n, err := newSweeper(repo, storage).SweepOnce(context.Background())
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected only the stale session swept, got %d", n)
	}
	if storage.aborted != 1 {
		t.Fatalf("expected 1 abort, got %d", storage.aborted)
	}
	if got := repo.sessions["fresh"].Status; got != StatusUploading {
		t.Fatalf("fresh session must be untouched, got %q", got)
	}
	if got := repo.sessions["done"].Status; got != StatusCompleted {
		t.Fatalf("completed session must be untouched, got %q", got)
	}
}

// TestSweepSkipsAbortWhenSessionRacedAway proves the claim-first ordering: if a
// session is no longer open by the time ExpireSession runs (a completion won the
// race), the sweeper must not abort its — possibly finalized — multipart upload.
func TestSweepSkipsAbortWhenSessionRacedAway(t *testing.T) {
	repo := newFakeRepo()
	storage := &fakeStorage{}
	seedSession(repo, "raced", StatusPending, time.Now().Add(-time.Hour))

	// Simulate completion landing between the list and the claim.
	racing := &racingRepo{fakeRepo: repo, completeOnExpire: "raced"}

	n, err := newSweeper(racing, storage).SweepOnce(context.Background())
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 swept when the session raced away, got %d", n)
	}
	if storage.aborted != 0 {
		t.Fatalf("must not abort a session that completed under us, aborted=%d", storage.aborted)
	}
}

// racingRepo flips a session to completed just before ExpireSession is asked to
// claim it, so ExpireSession returns false (lost the race).
type racingRepo struct {
	*fakeRepo
	completeOnExpire string
}

func (r *racingRepo) ExpireSession(ctx context.Context, id string) (bool, error) {
	if id == r.completeOnExpire {
		s := r.sessions[id]
		s.Status = StatusCompleted
		r.sessions[id] = s
	}
	return r.fakeRepo.ExpireSession(ctx, id)
}
