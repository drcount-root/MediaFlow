package job

import "time"

const (
	StatusProcessing = "processing"
	StatusReady      = "ready"
	StatusFailed     = "failed"

	// Queue contract for the transcode job, mirrored from apps/api
	// (videos.VideoExchange / videos.TranscodeRoutingKey). Kept in sync by hand —
	// the reaper re-enqueues via the outbox using these.
	VideoExchange       = "mediaflow.video"
	TranscodeRoutingKey = "video.transcode"
)

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
