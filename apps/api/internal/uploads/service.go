package uploads

import (
	"context"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
)

type Service struct {
	repo           Repository
	storage        ObjectStorage
	maxUploadBytes int64
	sessionTTL     time.Duration
	partURLTTL     time.Duration
}

func NewService(repo Repository, storage ObjectStorage, maxUploadBytes int64, sessionTTL, partURLTTL time.Duration) *Service {
	return &Service{
		repo:           repo,
		storage:        storage,
		maxUploadBytes: maxUploadBytes,
		sessionTTL:     sessionTTL,
		partURLTTL:     partURLTTL,
	}
}

// Create validates the declared upload, initiates a MinIO multipart upload, and
// records the session. No bytes are read here — the client uploads parts
// directly to object storage using the presigned URLs from PartURL.
func (s *Service) Create(ctx context.Context, params CreateParams) (Session, error) {
	title := strings.TrimSpace(params.Title)
	if title == "" {
		return Session{}, ErrInvalidInput
	}
	if params.TotalSize <= 0 || params.PartSize <= 0 {
		return Session{}, ErrInvalidInput
	}
	if !isSupportedMP4(params.ContentType, params.OriginalFilename) {
		return Session{}, ErrUnsupportedMedia
	}
	if s.maxUploadBytes > 0 && params.TotalSize > s.maxUploadBytes {
		return Session{}, ErrTooLarge
	}

	// The last part may be smaller than PartSize, but every other part must meet
	// the multipart minimum. Reject a part size that would force an undersized
	// non-final part (i.e. more than one part but PartSize below the floor).
	partCount := int((params.TotalSize + params.PartSize - 1) / params.PartSize)
	if partCount > 1 && params.PartSize < MinPartSize {
		return Session{}, ErrInvalidInput
	}
	if partCount > MaxPartCount {
		return Session{}, ErrInvalidInput
	}

	sessionID := uuid.NewString()
	objectKey := "raw-uploads/" + sessionID + "/original" + normalizedExt(params.OriginalFilename)

	uploadID, err := s.storage.InitiateMultipart(ctx, objectKey, params.ContentType)
	if err != nil {
		return Session{}, err
	}

	return s.repo.CreateSession(ctx, CreateSessionParams{
		ID:               sessionID,
		Title:            title,
		Description:      optionalString(params.Description),
		ObjectKey:        objectKey,
		UploadID:         uploadID,
		PartSize:         params.PartSize,
		TotalSize:        params.TotalSize,
		PartCount:        partCount,
		ContentType:      params.ContentType,
		OriginalFilename: params.OriginalFilename,
		ChecksumSHA256:   optionalString(params.ChecksumSHA256),
		ExpiresAt:        time.Now().UTC().Add(s.sessionTTL),
	})
}

// Get returns the session with the parts object storage has already received, so
// a client that reloaded mid-upload can skip the parts it already sent.
func (s *Service) Get(ctx context.Context, id string) (Session, error) {
	session, err := s.repo.GetSession(ctx, id)
	if err != nil {
		return Session{}, err
	}

	if session.Status == StatusPending || session.Status == StatusUploading {
		parts, err := s.storage.ListParts(ctx, session.ObjectKey, session.UploadID)
		if err != nil {
			return Session{}, err
		}
		session.UploadedParts = parts
	}
	return session, nil
}

// PartURL issues a presigned PUT URL for one part. The first issued URL flips the
// session to `uploading` so status reflects that bytes are flowing.
func (s *Service) PartURL(ctx context.Context, id string, partNumber int) (string, time.Time, error) {
	session, err := s.repo.GetSession(ctx, id)
	if err != nil {
		return "", time.Time{}, err
	}
	if session.Status != StatusPending && session.Status != StatusUploading {
		return "", time.Time{}, ErrConflict
	}
	if partNumber < 1 || partNumber > session.PartCount {
		return "", time.Time{}, ErrInvalidInput
	}

	url, err := s.storage.PresignPartURL(ctx, session.ObjectKey, session.UploadID, partNumber, s.partURLTTL)
	if err != nil {
		return "", time.Time{}, err
	}

	if session.Status == StatusPending {
		// Best-effort: a failure here doesn't invalidate the URL we just issued.
		_ = s.repo.SetSessionStatus(ctx, id, StatusUploading)
	}

	return url, time.Now().UTC().Add(s.partURLTTL), nil
}

// Abort cancels an in-progress upload and releases the multipart upload in object
// storage. Completed sessions cannot be aborted; already-aborted ones are a no-op.
func (s *Service) Abort(ctx context.Context, id string) error {
	session, err := s.repo.GetSession(ctx, id)
	if err != nil {
		return err
	}
	if session.Status == StatusCompleted {
		return ErrConflict
	}
	if session.Status == StatusAborted {
		return nil
	}

	if err := s.storage.AbortMultipart(ctx, session.ObjectKey, session.UploadID); err != nil {
		return err
	}
	return s.repo.SetSessionStatus(ctx, id, StatusAborted)
}

func optionalString(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}

func isSupportedMP4(contentType, filename string) bool {
	ct := strings.ToLower(strings.TrimSpace(strings.Split(contentType, ";")[0]))
	if ct == "video/mp4" {
		return true
	}
	return strings.EqualFold(filepath.Ext(filename), ".mp4")
}

func normalizedExt(filename string) string {
	ext := strings.ToLower(filepath.Ext(filename))
	if ext == "" {
		return ".mp4"
	}
	return ext
}
