package videos

import (
	"context"
	"errors"
	"io"
	"time"
)

const (
	StatusUploading  = "uploading"
	StatusUploaded   = "uploaded"
	StatusQueued     = "queued"
	StatusProcessing = "processing"
	StatusReady      = "ready"
	StatusFailed     = "failed"

	JobTypeTranscode = "transcode"
)

var (
	ErrInvalidInput     = errors.New("invalid input")
	ErrNotFound         = errors.New("video not found")
	ErrVideoNotReady    = errors.New("video not ready")
	ErrUnsupportedMedia = errors.New("unsupported media type")
	ErrFileTooLarge     = errors.New("file too large")
)

type Video struct {
	ID               string    `json:"id"`
	Title            string    `json:"title"`
	Description      *string   `json:"description"`
	Status           string    `json:"status"`
	RawObjectKey     *string   `json:"rawObjectKey,omitempty"`
	HLSMasterKey     *string   `json:"hlsMasterKey,omitempty"`
	ThumbnailKey     *string   `json:"thumbnailKey,omitempty"`
	ThumbnailURL     *string   `json:"thumbnailUrl,omitempty"`
	DurationSeconds  *float64  `json:"durationSeconds"`
	OriginalFilename *string   `json:"originalFilename,omitempty"`
	ContentType      *string   `json:"contentType,omitempty"`
	SizeBytes        *int64    `json:"sizeBytes,omitempty"`
	ErrorMessage     *string   `json:"errorMessage"`
	Variants         []Variant `json:"variants,omitempty"`
	CreatedAt        time.Time `json:"createdAt"`
	UpdatedAt        time.Time `json:"updatedAt"`
}

type Variant struct {
	Quality     string `json:"quality"`
	Width       int    `json:"width"`
	Height      int    `json:"height"`
	Bitrate     int    `json:"bitrate"`
	Codec       string `json:"codec,omitempty"`
	PlaylistKey string `json:"playlistKey,omitempty"`
}

type CreateQueuedVideoParams struct {
	VideoID          string
	JobID            string
	Title            string
	Description      *string
	RawObjectKey     string
	OriginalFilename string
	ContentType      string
	SizeBytes        int64
}

type UploadParams struct {
	Title            string
	Description      string
	OriginalFilename string
	ContentType      string
	SizeBytes        int64
	Body             io.Reader
}

type TranscodeJob struct {
	JobID        string    `json:"jobId"`
	VideoID      string    `json:"videoId"`
	RawBucket    string    `json:"rawBucket"`
	RawObjectKey string    `json:"rawObjectKey"`
	RequestedAt  time.Time `json:"requestedAt"`
}

type Repository interface {
	CreateQueuedVideo(ctx context.Context, params CreateQueuedVideoParams) (Video, error)
	ListVideos(ctx context.Context) ([]Video, error)
	GetVideo(ctx context.Context, id string) (Video, error)
	GetVariants(ctx context.Context, videoID string) ([]Variant, error)
}

type ObjectStorage interface {
	UploadRaw(ctx context.Context, objectKey string, body io.Reader, size int64, contentType string) error
	PresignedProcessedURL(ctx context.Context, objectKey string, expires time.Duration) (string, error)
	PresignedThumbnailURL(ctx context.Context, objectKey string, expires time.Duration) (string, error)
}

type JobPublisher interface {
	PublishTranscode(ctx context.Context, job TranscodeJob) error
}
