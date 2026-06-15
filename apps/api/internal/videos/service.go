package videos

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
)

type Service struct {
	repo           Repository
	storage        ObjectStorage
	rawBucket      string
	maxUploadBytes int64
}

func NewService(repo Repository, storage ObjectStorage, rawBucket string, maxUploadBytes int64) *Service {
	return &Service{
		repo:           repo,
		storage:        storage,
		rawBucket:      rawBucket,
		maxUploadBytes: maxUploadBytes,
	}
}

// Upload stores the raw video and enqueues a transcode job. The returned bool is
// true when a new video was created and false when an Idempotency-Key replay
// returned an existing one.
func (s *Service) Upload(ctx context.Context, params UploadParams) (Video, bool, error) {
	title := strings.TrimSpace(params.Title)
	if title == "" || params.Body == nil {
		return Video{}, false, ErrInvalidInput
	}

	if s.maxUploadBytes > 0 && params.SizeBytes > s.maxUploadBytes {
		return Video{}, false, ErrFileTooLarge
	}

	if !isSupportedMP4(params.ContentType, params.OriginalFilename) {
		return Video{}, false, ErrUnsupportedMedia
	}

	key := strings.TrimSpace(params.IdempotencyKey)

	// Fast path: a prior request with this key already created the video. Return
	// it without re-uploading the bytes or creating a duplicate.
	if key != "" {
		existing, err := s.repo.GetVideoByIdempotencyKey(ctx, key)
		if err == nil {
			return existing, false, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return Video{}, false, err
		}
	}

	videoID := uuid.NewString()
	jobID := uuid.NewString()
	description := optionalString(params.Description)
	rawKey := "raw-videos/" + videoID + "/original" + normalizedExt(params.OriginalFilename)

	if err := s.storage.UploadRaw(ctx, rawKey, params.Body, params.SizeBytes, params.ContentType); err != nil {
		return Video{}, false, err
	}

	// Build the transcode job and hand it to the repository as an outbox row.
	// The video, job, and outbox message are committed in one transaction; the
	// API never publishes to RabbitMQ on the request path (no dual-write). The
	// relay loop delivers it.
	payload, err := json.Marshal(TranscodeJob{
		JobID:        jobID,
		VideoID:      videoID,
		RawBucket:    s.rawBucket,
		RawObjectKey: rawKey,
		RequestedAt:  time.Now().UTC(),
	})
	if err != nil {
		return Video{}, false, err
	}

	video, err := s.repo.CreateQueuedVideo(ctx, CreateQueuedVideoParams{
		VideoID:           videoID,
		JobID:             jobID,
		Title:             title,
		Description:       description,
		RawObjectKey:      rawKey,
		OriginalFilename:  params.OriginalFilename,
		ContentType:       params.ContentType,
		SizeBytes:         params.SizeBytes,
		IdempotencyKey:    optionalString(key),
		OutboxExchange:    VideoExchange,
		OutboxRoutingKey:  TranscodeRoutingKey,
		OutboxPayloadJSON: payload,
	})
	// Lost the race: a concurrent request with the same key committed first.
	// Return its row — the duplicate raw object we just wrote is harmless.
	if errors.Is(err, ErrDuplicateKey) && key != "" {
		existing, getErr := s.repo.GetVideoByIdempotencyKey(ctx, key)
		if getErr != nil {
			return Video{}, false, getErr
		}
		return existing, false, nil
	}
	if err != nil {
		return Video{}, false, err
	}

	return video, true, nil
}

func (s *Service) List(ctx context.Context) ([]Video, error) {
	videos, err := s.repo.ListVideos(ctx)
	if err != nil {
		return nil, err
	}

	for idx := range videos {
		s.attachThumbnailURL(ctx, &videos[idx])
	}

	return videos, nil
}

func (s *Service) Get(ctx context.Context, id string) (Video, error) {
	video, err := s.repo.GetVideo(ctx, id)
	if err != nil {
		return Video{}, err
	}

	variants, err := s.repo.GetVariants(ctx, id)
	if err != nil {
		return Video{}, err
	}

	video.Variants = variants
	s.attachThumbnailURL(ctx, &video)
	return video, nil
}

func (s *Service) Playback(ctx context.Context, id string) (string, time.Time, error) {
	video, err := s.repo.GetVideo(ctx, id)
	if err != nil {
		return "", time.Time{}, err
	}

	if video.Status != StatusReady || video.HLSMasterKey == nil || *video.HLSMasterKey == "" {
		return "", time.Time{}, ErrVideoNotReady
	}

	expires := time.Hour
	url, err := s.storage.PresignedProcessedURL(ctx, *video.HLSMasterKey, expires)
	if err != nil {
		return "", time.Time{}, err
	}

	return url, time.Now().UTC().Add(expires), nil
}

func (s *Service) attachThumbnailURL(ctx context.Context, video *Video) {
	if video.ThumbnailKey == nil || *video.ThumbnailKey == "" {
		return
	}

	url, err := s.storage.PresignedThumbnailURL(ctx, *video.ThumbnailKey, time.Hour)
	if err != nil {
		return
	}

	video.ThumbnailURL = &url
}

func optionalString(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}

func isSupportedMP4(contentType, filename string) bool {
	contentType = strings.ToLower(strings.TrimSpace(strings.Split(contentType, ";")[0]))
	if contentType == "video/mp4" {
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
