package job

import "time"

const (
	StatusProcessing = "processing"
	StatusReady      = "ready"
	StatusFailed     = "failed"
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
