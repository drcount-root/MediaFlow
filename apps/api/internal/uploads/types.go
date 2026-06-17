// Package uploads is the Milestone 6 ingest control plane. Bytes never transit
// the API: a client creates an upload session, the API initiates a MinIO
// multipart upload and hands back presigned per-part URLs, the client PUTs parts
// directly to object storage, and completion validates and enqueues the
// transcode job through the M5 outbox. This package holds the session lifecycle;
// completion/validation lands in a later slice.
package uploads

import (
	"context"
	"errors"
	"time"
)

const (
	StatusPending   = "pending"
	StatusUploading = "uploading"
	StatusCompleted = "completed"
	StatusAborted   = "aborted"
	StatusExpired   = "expired"

	// S3/MinIO multipart limits: a part (except the last) must be at least 5 MiB
	// and an upload may have at most 10,000 parts.
	MinPartSize  = 5 * 1024 * 1024
	MaxPartCount = 10000
)

var (
	ErrInvalidInput     = errors.New("invalid input")
	ErrNotFound         = errors.New("upload session not found")
	ErrTooLarge         = errors.New("declared size exceeds limit")
	ErrUnsupportedMedia = errors.New("unsupported media type")
	// ErrConflict means the session is not in a state that allows the requested
	// action (e.g. asking for a part URL on a completed or aborted session).
	ErrConflict = errors.New("upload session not in a usable state")
	// ErrIncompleteUpload means object storage is missing parts the session
	// expects (the client never finished uploading).
	ErrIncompleteUpload = errors.New("upload is missing parts")
	// ErrChecksumMismatch means a declared part ETag does not match the part
	// object storage actually holds — a tampered or re-uploaded part.
	ErrChecksumMismatch = errors.New("part checksum mismatch")
	// ErrSizeMismatch means the assembled object size differs from the size the
	// client declared at session creation.
	ErrSizeMismatch = errors.New("assembled size does not match declared size")
)

// Session is a multipart upload in progress (or finished). UploadedParts is only
// populated by Get, from object storage, so a client can resume after a reload.
type Session struct {
	ID          string  `json:"id"`
	Title       string  `json:"title"`
	Description *string `json:"description,omitempty"`
	ObjectKey   string  `json:"objectKey"`
	// UploadID is the MinIO multipart upload id. Kept server-side only — the
	// client never needs it (part URLs are presigned for it) so it is not
	// serialized.
	UploadID         string         `json:"-"`
	Status           string         `json:"status"`
	PartSize         int64          `json:"partSize"`
	TotalSize        int64          `json:"totalSize"`
	PartCount        int            `json:"partCount"`
	ContentType      string         `json:"contentType"`
	OriginalFilename string         `json:"originalFilename"`
	ChecksumSHA256   *string        `json:"checksumSha256,omitempty"`
	VideoID          *string        `json:"videoId,omitempty"`
	CreatedAt        time.Time      `json:"createdAt"`
	UpdatedAt        time.Time      `json:"updatedAt"`
	ExpiresAt        time.Time      `json:"expiresAt"`
	UploadedParts    []UploadedPart `json:"uploadedParts,omitempty"`
}

// UploadedPart describes a part already stored by object storage.
type UploadedPart struct {
	PartNumber int    `json:"partNumber"`
	ETag       string `json:"etag"`
	Size       int64  `json:"size"`
}

// CreateParams is the client-declared shape of a new upload.
type CreateParams struct {
	Title            string
	Description      string
	OriginalFilename string
	ContentType      string
	TotalSize        int64
	PartSize         int64
	ChecksumSHA256   string
}

// CompletePart is a client-declared (partNumber, etag) pair, validated against
// what object storage holds before the multipart upload is finalized.
type CompletePart struct {
	PartNumber int    `json:"partNumber"`
	ETag       string `json:"etag"`
}

// CompleteSessionParams is the atomic completion the repository persists: the
// video/job/events/outbox rows (reusing the M5 machinery) plus marking the
// session completed and linking it to the new video — all in one transaction.
type CompleteSessionParams struct {
	SessionID         string
	VideoID           string
	JobID             string
	Title             string
	Description       *string
	RawObjectKey      string
	OriginalFilename  string
	ContentType       string
	SizeBytes         int64
	OutboxExchange    string
	OutboxRoutingKey  string
	OutboxPayloadJSON []byte
}

// CreateSessionParams is the fully-resolved row the repository persists.
type CreateSessionParams struct {
	ID               string
	Title            string
	Description      *string
	ObjectKey        string
	UploadID         string
	PartSize         int64
	TotalSize        int64
	PartCount        int
	ContentType      string
	OriginalFilename string
	ChecksumSHA256   *string
	ExpiresAt        time.Time
}

type Repository interface {
	CreateSession(ctx context.Context, params CreateSessionParams) (Session, error)
	GetSession(ctx context.Context, id string) (Session, error)
	SetSessionStatus(ctx context.Context, id, status string) error
	CompleteSession(ctx context.Context, params CompleteSessionParams) error
}

// ObjectStorage is the multipart surface the service needs from MinIO. The
// concrete implementation lives in internal/storage.
type ObjectStorage interface {
	InitiateMultipart(ctx context.Context, objectKey, contentType string) (uploadID string, err error)
	PresignPartURL(ctx context.Context, objectKey, uploadID string, partNumber int, expires time.Duration) (string, error)
	ListParts(ctx context.Context, objectKey, uploadID string) ([]UploadedPart, error)
	CompleteMultipart(ctx context.Context, objectKey, uploadID string, parts []CompletePart) error
	AbortMultipart(ctx context.Context, objectKey, uploadID string) error
}
