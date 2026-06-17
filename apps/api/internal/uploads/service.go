package uploads

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"mediaflow/apps/api/internal/videos"
)

type Service struct {
	repo           Repository
	storage        ObjectStorage
	rawBucket      string
	maxUploadBytes int64
	sessionTTL     time.Duration
	partURLTTL     time.Duration
}

func NewService(repo Repository, storage ObjectStorage, rawBucket string, maxUploadBytes int64, sessionTTL, partURLTTL time.Duration) *Service {
	return &Service{
		repo:           repo,
		storage:        storage,
		rawBucket:      rawBucket,
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

// Complete validates the uploaded parts against what object storage actually
// holds, finalizes the multipart upload, and — in one transaction — creates the
// video/job/outbox rows (reusing the M5 enqueue) and marks the session
// completed. The returned bool is true when this call created the video and
// false when a prior completion is being replayed (idempotent).
func (s *Service) Complete(ctx context.Context, id string, declared []CompletePart) (videoID string, created bool, err error) {
	session, err := s.repo.GetSession(ctx, id)
	if err != nil {
		return "", false, err
	}
	// Replay: already completed -> return the existing video.
	if session.Status == StatusCompleted {
		if session.VideoID == nil {
			return "", false, ErrConflict
		}
		return *session.VideoID, false, nil
	}
	if session.Status != StatusPending && session.Status != StatusUploading {
		return "", false, ErrConflict
	}

	// What object storage actually holds, keyed by part number.
	actualParts, err := s.storage.ListParts(ctx, session.ObjectKey, session.UploadID)
	if err != nil {
		return "", false, err
	}
	actual := make(map[int]UploadedPart, len(actualParts))
	for _, p := range actualParts {
		actual[p.PartNumber] = p
	}

	// Every expected part must be present and its declared ETag must match the
	// stored part. We rebuild the complete list from the stored ETags so the
	// finalize call can never use a client-forged value.
	if len(declared) != session.PartCount {
		return "", false, ErrIncompleteUpload
	}
	declaredETag := make(map[int]string, len(declared))
	for _, d := range declared {
		declaredETag[d.PartNumber] = normalizeETag(d.ETag)
	}

	var total int64
	complete := make([]CompletePart, 0, session.PartCount)
	for n := 1; n <= session.PartCount; n++ {
		stored, ok := actual[n]
		if !ok {
			return "", false, ErrIncompleteUpload
		}
		want, ok := declaredETag[n]
		if !ok {
			return "", false, ErrIncompleteUpload
		}
		if want != normalizeETag(stored.ETag) {
			return "", false, ErrChecksumMismatch
		}
		total += stored.Size
		complete = append(complete, CompletePart{PartNumber: n, ETag: stored.ETag})
	}

	if total != session.TotalSize {
		return "", false, ErrSizeMismatch
	}
	if s.maxUploadBytes > 0 && total > s.maxUploadBytes {
		return "", false, ErrTooLarge
	}

	sort.Slice(complete, func(i, j int) bool { return complete[i].PartNumber < complete[j].PartNumber })
	if err := s.storage.CompleteMultipart(ctx, session.ObjectKey, session.UploadID, complete); err != nil {
		return "", false, err
	}

	newVideoID := uuid.NewString()
	jobID := uuid.NewString()
	payload, err := json.Marshal(videos.TranscodeJob{
		JobID:        jobID,
		VideoID:      newVideoID,
		RawBucket:    s.rawBucket,
		RawObjectKey: session.ObjectKey,
		RequestedAt:  time.Now().UTC(),
	})
	if err != nil {
		return "", false, err
	}

	if err := s.repo.CompleteSession(ctx, CompleteSessionParams{
		SessionID:         session.ID,
		VideoID:           newVideoID,
		JobID:             jobID,
		Title:             session.Title,
		Description:       session.Description,
		RawObjectKey:      session.ObjectKey,
		OriginalFilename:  session.OriginalFilename,
		ContentType:       session.ContentType,
		SizeBytes:         total,
		OutboxExchange:    videos.VideoExchange,
		OutboxRoutingKey:  videos.TranscodeRoutingKey,
		OutboxPayloadJSON: payload,
	}); err != nil {
		return "", false, err
	}

	return newVideoID, true, nil
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

// normalizeETag strips surrounding quotes and lowercases an ETag so values from
// a PUT response header (quoted) and from ListObjectParts (unquoted) compare
// equal.
func normalizeETag(etag string) string {
	return strings.ToLower(strings.Trim(strings.TrimSpace(etag), `"`))
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
