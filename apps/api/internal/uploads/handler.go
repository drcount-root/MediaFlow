package uploads

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

type Handler struct {
	service *Service
}

func NewHandler(service *Service) *Handler {
	return &Handler{service: service}
}

func (h *Handler) RegisterRoutes(router gin.IRouter) {
	router.POST("/uploads", h.create)
	router.GET("/uploads/:id", h.get)
	router.GET("/uploads/:id/parts/:n/url", h.partURL)
	router.DELETE("/uploads/:id", h.abort)
}

type createRequest struct {
	Title            string `json:"title"`
	Description      string `json:"description"`
	OriginalFilename string `json:"originalFilename"`
	ContentType      string `json:"contentType"`
	TotalSize        int64  `json:"totalSize"`
	PartSize         int64  `json:"partSize"`
	ChecksumSHA256   string `json:"checksumSha256"`
}

func (h *Handler) create(ctx *gin.Context) {
	var req createRequest
	if err := ctx.ShouldBindJSON(&req); err != nil {
		writeError(ctx, http.StatusBadRequest, "invalid_input", "A valid JSON body is required.")
		return
	}

	session, err := h.service.Create(ctx.Request.Context(), CreateParams{
		Title:            req.Title,
		Description:      req.Description,
		OriginalFilename: req.OriginalFilename,
		ContentType:      req.ContentType,
		TotalSize:        req.TotalSize,
		PartSize:         req.PartSize,
		ChecksumSHA256:   req.ChecksumSHA256,
	})
	if err != nil {
		writeServiceError(ctx, err)
		return
	}

	ctx.JSON(http.StatusCreated, session)
}

func (h *Handler) get(ctx *gin.Context) {
	session, err := h.service.Get(ctx.Request.Context(), ctx.Param("id"))
	if err != nil {
		writeServiceError(ctx, err)
		return
	}
	ctx.JSON(http.StatusOK, session)
}

func (h *Handler) partURL(ctx *gin.Context) {
	partNumber, err := strconv.Atoi(ctx.Param("n"))
	if err != nil {
		writeError(ctx, http.StatusBadRequest, "invalid_input", "Part number must be an integer.")
		return
	}

	url, expiresAt, err := h.service.PartURL(ctx.Request.Context(), ctx.Param("id"), partNumber)
	if err != nil {
		writeServiceError(ctx, err)
		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"partNumber": partNumber,
		"url":        url,
		"method":     http.MethodPut,
		"expiresAt":  expiresAt,
	})
}

func (h *Handler) abort(ctx *gin.Context) {
	if err := h.service.Abort(ctx.Request.Context(), ctx.Param("id")); err != nil {
		writeServiceError(ctx, err)
		return
	}
	ctx.Status(http.StatusNoContent)
}

func writeServiceError(ctx *gin.Context, err error) {
	switch {
	case errors.Is(err, ErrInvalidInput):
		writeError(ctx, http.StatusBadRequest, "invalid_input", "The upload request is invalid.")
	case errors.Is(err, ErrUnsupportedMedia):
		writeError(ctx, http.StatusUnsupportedMediaType, "invalid_file_type", "Only MP4 uploads are supported.")
	case errors.Is(err, ErrTooLarge):
		writeError(ctx, http.StatusRequestEntityTooLarge, "file_too_large", "The declared size exceeds the configured limit.")
	case errors.Is(err, ErrNotFound):
		writeError(ctx, http.StatusNotFound, "upload_not_found", "Upload session not found.")
	case errors.Is(err, ErrConflict):
		writeError(ctx, http.StatusConflict, "upload_conflict", "The upload session is not in a state that allows this action.")
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
