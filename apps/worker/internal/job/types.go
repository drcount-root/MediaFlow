package job

import (
	"errors"
	"time"
)

const (
	StatusProcessing = "processing"
	StatusReady      = "ready"
	StatusFailed     = "failed"

	// Queue contract for the transcode job, mirrored from apps/api
	// (videos.VideoExchange / videos.TranscodeRoutingKey). Kept in sync by hand —
	// the reaper re-enqueues via the outbox using these.
	VideoExchange       = "mediaflow.video"
	TranscodeRoutingKey = "video.transcode"

	// Retry/dead-letter routing (M5.3). A transient failure below max attempts is
	// republished to RetryRoutingKey with a per-message TTL; that queue dead-letters
	// back to TranscodeRoutingKey when the TTL expires. Poison and exhausted
	// messages go to DLQRoutingKey for inspection.
	RetryRoutingKey = "video.transcode.retry"
	DLQRoutingKey   = "video.transcode.dlq"
)

// PermanentError marks a failure that retrying cannot fix — a corrupt upload, a
// file with no video stream, etc. The worker sends these straight to the DLQ
// instead of scheduling a retry.
type PermanentError struct{ Err error }

func (e *PermanentError) Error() string { return e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// Permanent wraps err so the worker treats it as non-retryable.
func Permanent(err error) error { return &PermanentError{Err: err} }

// IsPermanent reports whether err (or anything it wraps) is a PermanentError.
func IsPermanent(err error) bool {
	var pe *PermanentError
	return errors.As(err, &pe)
}

type TranscodeJob struct {
	JobID        string    `json:"jobId"`
	VideoID      string    `json:"videoId"`
	RawBucket    string    `json:"rawBucket"`
	RawObjectKey string    `json:"rawObjectKey"`
	RequestedAt  time.Time `json:"requestedAt"`
}

type Variant struct {
	Quality     string
	Width       int
	Height      int
	Bitrate     int
	Codec       string
	PlaylistKey string
	LocalDir    string
}

type ProbeResult struct {
	DurationSeconds float64
	Width           int
	Height          int
	HasAudio        bool
}
