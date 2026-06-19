package videos

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
)

type Handler struct {
	service *Service
}

func NewHandler(service *Service) *Handler {
	return &Handler{service: service}
}

// RegisterRoutes wires the video read endpoints. The legacy proxy upload
// (POST /videos/upload, which streams bytes through the API) is registered only
// when enableLegacyUpload is set — M6 replaced it with the presigned multipart
// ingest in internal/uploads, and it is kept solely for comparison benchmarks.
func (h *Handler) RegisterRoutes(router gin.IRouter, enableLegacyUpload bool) {
	if enableLegacyUpload {
		router.POST("/videos/upload", h.upload)
	}
	router.GET("/videos", h.list)
	router.GET("/videos/:id", h.get)
	router.GET("/videos/:id/playback", h.playback)
}

func (h *Handler) upload(ctx *gin.Context) {
	file, err := ctx.FormFile("file")
	if err != nil {
		writeError(ctx, http.StatusBadRequest, "missing_file", "A video file is required.")
		return
	}

	body, err := file.Open()
	if err != nil {
		writeError(ctx, http.StatusBadRequest, "invalid_file", "The uploaded file could not be read.")
		return
	}
	defer body.Close()

	video, created, err := h.service.Upload(ctx.Request.Context(), UploadParams{
		Title:            ctx.PostForm("title"),
		Description:      ctx.PostForm("description"),
		OriginalFilename: file.Filename,
		ContentType:      file.Header.Get("Content-Type"),
		SizeBytes:        file.Size,
		Body:             body,
		IdempotencyKey:   ctx.GetHeader("Idempotency-Key"),
	})
	if err != nil {
		writeServiceError(ctx, err)
		return
	}

	// A replayed Idempotency-Key returns the original resource with 200; a fresh
	// upload returns 201.
	if created {
		ctx.JSON(http.StatusCreated, video)
		return
	}
	ctx.JSON(http.StatusOK, video)
}

func (h *Handler) list(ctx *gin.Context) {
	items, err := h.service.List(ctx.Request.Context())
	if err != nil {
		writeServiceError(ctx, err)
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"items": items})
}

func (h *Handler) get(ctx *gin.Context) {
	video, err := h.service.Get(ctx.Request.Context(), ctx.Param("id"))
	if err != nil {
		writeServiceError(ctx, err)
		return
	}

	ctx.JSON(http.StatusOK, video)
}

func (h *Handler) playback(ctx *gin.Context) {
	url, expiresAt, err := h.service.Playback(ctx.Request.Context(), ctx.Param("id"))
	if err != nil {
		writeServiceError(ctx, err)
		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"videoId":   ctx.Param("id"),
		"hlsUrl":    url,
		"expiresAt": expiresAt,
	})
}

func writeServiceError(ctx *gin.Context, err error) {
	switch {
	case errors.Is(err, ErrInvalidInput):
		writeError(ctx, http.StatusBadRequest, "invalid_input", "Title and file are required.")
	case errors.Is(err, ErrFileTooLarge):
		writeError(ctx, http.StatusRequestEntityTooLarge, "file_too_large", "The uploaded file exceeds the configured size limit.")
	case errors.Is(err, ErrUnsupportedMedia):
		writeError(ctx, http.StatusUnsupportedMediaType, "invalid_file_type", "Only MP4 uploads are supported in the MVP.")
	case errors.Is(err, ErrNotFound):
		writeError(ctx, http.StatusNotFound, "video_not_found", "Video not found.")
	case errors.Is(err, ErrVideoNotReady):
		writeError(ctx, http.StatusConflict, "video_not_ready", "Video is not ready for playback.")
	default:
		writeError(ctx, http.StatusInternalServerError, "internal_error", "Unexpected server error.")
	}
}

func writeError(ctx *gin.Context, status int, code, message string) {
	ctx.JSON(status, gin.H{
		"error": gin.H{
			"code":    code,
			"message": message,
		},
	})
}
