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

	// Fan-out routing (M7). The plan job is the one ingest enqueues on
	// TranscodeRoutingKey. The planner fans out one rendition message per quality
	// on RenditionRoutingKey; whoever finishes the last rendition enqueues one
	// finalize message on FinalizeRoutingKey.
	RenditionRoutingKey = "video.rendition"
	FinalizeRoutingKey  = "video.finalize"

	// Job types in video_jobs.job_type (M7).
	JobTypePlan      = "plan"
	JobTypeRendition = "rendition"
	JobTypeFinalize  = "finalize"

	// Retry/dead-letter routing (M5.3, extended per-stage in M7 slice B). A transient
	// failure below max attempts is republished to the stage's retry key with a
	// per-message TTL; that queue dead-letters back to the stage's main queue when
	// the TTL expires, so one quality retrying never redoes the others. Poison and
	// exhausted messages from every stage go to the shared DLQRoutingKey.
	RetryRoutingKey          = "video.transcode.retry"
	RenditionRetryRoutingKey = "video.rendition.retry"
	FinalizeRetryRoutingKey  = "video.finalize.retry"
	DLQRoutingKey            = "video.transcode.dlq"
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

// TranscodeJob is the plan-stage message ingest enqueues on the transcode queue.
type TranscodeJob struct {
	JobID        string    `json:"jobId"`
	VideoID      string    `json:"videoId"`
	RawBucket    string    `json:"rawBucket"`
	RawObjectKey string    `json:"rawObjectKey"`
	RequestedAt  time.Time `json:"requestedAt"`
}

// RenditionSpec is the single quality a rendition job must produce. It is stored
// on the job row (rendition_spec) and carried in the queue message so the worker
// and the reaper agree on what to encode without re-running the planner.
type RenditionSpec struct {
	Quality string `json:"quality"`
	Width   int    `json:"width"`
	Height  int    `json:"height"`
	Bitrate int    `json:"bitrate"`
	Codec   string `json:"codec"`
}

// RenditionJob is one fanned-out unit of work: transcode exactly one quality.
type RenditionJob struct {
	JobID        string        `json:"jobId"`
	ParentJobID  string        `json:"parentJobId"`
	VideoID      string        `json:"videoId"`
	RawBucket    string        `json:"rawBucket"`
	RawObjectKey string        `json:"rawObjectKey"`
	Spec         RenditionSpec `json:"spec"`
	RequestedAt  time.Time     `json:"requestedAt"`
}

// FinalizeJob assembles the master playlist and marks the video ready once every
// rendition has finished. It is enqueued by whichever rendition decrements the
// pending counter to zero.
type FinalizeJob struct {
	JobID        string    `json:"jobId"`
	ParentJobID  string    `json:"parentJobId"`
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
