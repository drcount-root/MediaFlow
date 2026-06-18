package uploads

import (
	"context"
	"log/slog"
	"time"
)

// Sweeper is the Milestone 6 cleanup loop: it expires upload sessions whose
// deadline has passed and aborts their MinIO multipart uploads so the staged
// parts don't linger in object storage forever. Without it, a client that
// abandons an upload (closed the tab, lost the network) leaves both a stuck
// `pending`/`uploading` row and an orphaned multipart upload.
//
// It mirrors the outbox relay's shape: a ticker drives passes until ctx is
// cancelled, and each pass drains in batches so a backlog clears quickly.
type Sweeper struct {
	repo      Repository
	storage   ObjectStorage
	logger    *slog.Logger
	interval  time.Duration
	batchSize int
}

func NewSweeper(repo Repository, storage ObjectStorage, logger *slog.Logger, interval time.Duration, batchSize int) *Sweeper {
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	if batchSize <= 0 {
		batchSize = 100
	}
	return &Sweeper{
		repo:      repo,
		storage:   storage,
		logger:    logger,
		interval:  interval,
		batchSize: batchSize,
	}
}

// Run sweeps expired sessions on a ticker until ctx is cancelled.
func (s *Sweeper) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	s.logger.Info("upload sweeper started", "interval", s.interval.String(), "batchSize", s.batchSize)
	for {
		select {
		case <-ctx.Done():
			s.logger.Info("upload sweeper stopped")
			return
		case <-ticker.C:
			if n, err := s.SweepOnce(ctx); err != nil && ctx.Err() == nil {
				s.logger.Error("upload sweep failed", "error", err)
			} else if n > 0 {
				s.logger.Info("expired upload sessions swept", "count", n)
			}
		}
	}
}

// SweepOnce expires sessions in batches until none remain (or an error stops
// it), so a backlog of abandoned uploads clears within one pass. It is what the
// ticker calls, and is exported so tests (and any future ops trigger) can drive
// a single deterministic pass.
func (s *Sweeper) SweepOnce(ctx context.Context) (int, error) {
	total := 0
	for {
		if ctx.Err() != nil {
			return total, ctx.Err()
		}
		n, err := s.sweepBatch(ctx)
		total += n
		if err != nil {
			return total, err
		}
		if n < s.batchSize {
			return total, nil
		}
	}
}

// sweepBatch claims-then-releases each expired session: it first flips the
// status to `expired` (the guarded UPDATE is what makes this safe against a
// concurrent completion), and only on winning that race does it abort the
// multipart upload. A failed abort is logged but does not revert the status —
// MinIO's incomplete-multipart lifecycle rule is the backstop for the rare
// orphan, and leaving the row `expired` keeps the sweep from spinning on it.
func (s *Sweeper) sweepBatch(ctx context.Context) (int, error) {
	sessions, err := s.repo.ListExpiredSessions(ctx, s.batchSize)
	if err != nil {
		return 0, err
	}

	expired := 0
	for _, session := range sessions {
		claimed, err := s.repo.ExpireSession(ctx, session.ID)
		if err != nil {
			return expired, err
		}
		if !claimed {
			// Completion or abort moved the session between the list and now;
			// whoever owns it will release its multipart upload.
			continue
		}
		if err := s.storage.AbortMultipart(ctx, session.ObjectKey, session.UploadID); err != nil {
			s.logger.Warn("abort multipart for expired session failed",
				"sessionId", session.ID, "objectKey", session.ObjectKey, "error", err)
		}
		expired++
	}
	return expired, nil
}
