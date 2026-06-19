package videos

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestUploadCreatesQueuedVideo(t *testing.T) {
	gin.SetMode(gin.TestMode)

	repo := &fakeRepository{}
	storage := &fakeStorage{}
	service := NewService(repo, storage, "mediaflow-raw", 1024)

	router := gin.New()
	NewHandler(service).RegisterRoutes(router, true)

	body, contentType := multipartBody(t, map[string]string{
		"title":       "Demo",
		"description": "Sample upload",
	}, "file", "demo.mp4", "video/mp4", "video bytes")

	request := httptest.NewRequest(http.MethodPost, "/videos/upload", body)
	request.Header.Set("Content-Type", contentType)
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d: %s", http.StatusCreated, response.Code, response.Body.String())
	}

	if storage.uploadedKey == "" {
		t.Fatal("expected raw object upload")
	}

	if repo.created.VideoID == "" {
		t.Fatal("expected video row creation")
	}

	// The upload path enqueues via the outbox, not a direct publish: the created
	// params must carry a transcode message for this video.
	if repo.created.OutboxRoutingKey != TranscodeRoutingKey || repo.created.OutboxExchange != VideoExchange {
		t.Fatalf("expected outbox routed to %s/%s, got %s/%s",
			VideoExchange, TranscodeRoutingKey, repo.created.OutboxExchange, repo.created.OutboxRoutingKey)
	}

	var job TranscodeJob
	if err := json.Unmarshal(repo.created.OutboxPayloadJSON, &job); err != nil {
		t.Fatalf("decode outbox payload: %v", err)
	}
	if job.VideoID != repo.created.VideoID {
		t.Fatalf("expected outbox job for video %q, got %q", repo.created.VideoID, job.VideoID)
	}
}

func TestUploadReplaysIdempotencyKey(t *testing.T) {
	gin.SetMode(gin.TestMode)

	repo := &fakeRepository{}
	storage := &fakeStorage{}
	service := NewService(repo, storage, "mediaflow-raw", 1<<20)

	router := gin.New()
	NewHandler(service).RegisterRoutes(router, true)

	doUpload := func() *httptest.ResponseRecorder {
		body, contentType := multipartBody(t, map[string]string{"title": "Demo"}, "file", "demo.mp4", "video/mp4", "video bytes")
		request := httptest.NewRequest(http.MethodPost, "/videos/upload", body)
		request.Header.Set("Content-Type", contentType)
		request.Header.Set("Idempotency-Key", "abc-123")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		return response
	}

	first := doUpload()
	if first.Code != http.StatusCreated {
		t.Fatalf("first upload: expected 201, got %d: %s", first.Code, first.Body.String())
	}

	second := doUpload()
	if second.Code != http.StatusOK {
		t.Fatalf("replay: expected 200, got %d: %s", second.Code, second.Body.String())
	}

	if repo.createCalls != 1 {
		t.Fatalf("expected exactly one video created across two requests, got %d", repo.createCalls)
	}

	var v1, v2 Video
	if err := json.Unmarshal(first.Body.Bytes(), &v1); err != nil {
		t.Fatalf("decode first: %v", err)
	}
	if err := json.Unmarshal(second.Body.Bytes(), &v2); err != nil {
		t.Fatalf("decode second: %v", err)
	}
	if v1.ID != v2.ID {
		t.Fatalf("replay returned a different video: %q vs %q", v1.ID, v2.ID)
	}
}

func TestUploadRejectsMissingTitle(t *testing.T) {
	gin.SetMode(gin.TestMode)

	service := NewService(&fakeRepository{}, &fakeStorage{}, "mediaflow-raw", 1024)
	router := gin.New()
	NewHandler(service).RegisterRoutes(router, true)

	body, contentType := multipartBody(t, nil, "file", "demo.mp4", "video/mp4", "video bytes")
	request := httptest.NewRequest(http.MethodPost, "/videos/upload", body)
	request.Header.Set("Content-Type", contentType)
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, response.Code)
	}
}

func TestUploadRejectsUnsupportedFileType(t *testing.T) {
	gin.SetMode(gin.TestMode)

	service := NewService(&fakeRepository{}, &fakeStorage{}, "mediaflow-raw", 1024)
	router := gin.New()
	NewHandler(service).RegisterRoutes(router, true)

	body, contentType := multipartBody(t, map[string]string{"title": "Demo"}, "file", "demo.txt", "text/plain", "not video")
	request := httptest.NewRequest(http.MethodPost, "/videos/upload", body)
	request.Header.Set("Content-Type", contentType)
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("expected status %d, got %d", http.StatusUnsupportedMediaType, response.Code)
	}
}

func multipartBody(t *testing.T, fields map[string]string, fileField, filename, contentType, contents string) (*bytes.Buffer, string) {
	t.Helper()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	for key, value := range fields {
		if err := writer.WriteField(key, value); err != nil {
			t.Fatalf("write field: %v", err)
		}
	}

	header := make(textproto.MIMEHeader)
	header.Set("Content-Disposition", `form-data; name="`+fileField+`"; filename="`+filename+`"`)
	header.Set("Content-Type", contentType)

	part, err := writer.CreatePart(header)
	if err != nil {
		t.Fatalf("create file part: %v", err)
	}

	if _, err := io.Copy(part, strings.NewReader(contents)); err != nil {
		t.Fatalf("write file part: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}

	return body, writer.FormDataContentType()
}

type fakeRepository struct {
	created     CreateQueuedVideoParams
	video       Video
	byKey       map[string]Video
	createCalls int
}

func (r *fakeRepository) CreateQueuedVideo(_ context.Context, params CreateQueuedVideoParams) (Video, error) {
	r.createCalls++
	r.created = params
	video := Video{
		ID:               params.VideoID,
		Title:            params.Title,
		Description:      params.Description,
		Status:           StatusQueued,
		RawObjectKey:     &params.RawObjectKey,
		OriginalFilename: &params.OriginalFilename,
		ContentType:      &params.ContentType,
		SizeBytes:        &params.SizeBytes,
		CreatedAt:        time.Now().UTC(),
		UpdatedAt:        time.Now().UTC(),
	}
	if params.IdempotencyKey != nil {
		if r.byKey == nil {
			r.byKey = map[string]Video{}
		}
		r.byKey[*params.IdempotencyKey] = video
	}
	return video, nil
}

func (r *fakeRepository) GetVideoByIdempotencyKey(_ context.Context, key string) (Video, error) {
	if stored, ok := r.byKey[key]; ok {
		return stored, nil
	}
	return Video{}, ErrNotFound
}

func (r *fakeRepository) ListVideos(context.Context) ([]Video, error) {
	return []Video{r.video}, nil
}

func (r *fakeRepository) GetVideo(context.Context, string) (Video, error) {
	if r.video.ID == "" {
		return Video{}, ErrNotFound
	}
	return r.video, nil
}

func (r *fakeRepository) GetVariants(context.Context, string) ([]Variant, error) {
	return nil, nil
}

type fakeStorage struct {
	uploadedKey string
}

func (s *fakeStorage) UploadRaw(_ context.Context, objectKey string, _ io.Reader, _ int64, _ string) error {
	s.uploadedKey = objectKey
	return nil
}

func (s *fakeStorage) PresignedProcessedURL(context.Context, string, time.Duration) (string, error) {
	return "http://example.test/master.m3u8", nil
}

func (s *fakeStorage) PresignedThumbnailURL(context.Context, string, time.Duration) (string, error) {
	return "http://example.test/thumbnail.jpg", nil
}
