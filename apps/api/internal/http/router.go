package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"mediaflow/apps/api/internal/config"
	"mediaflow/apps/api/internal/videos"
)

func NewRouter(cfg config.Config) http.Handler {
	return NewRouterWithVideos(cfg, nil)
}

func NewRouterWithVideos(cfg config.Config, videoService *videos.Service) http.Handler {
	if cfg.AppEnv == "test" {
		gin.SetMode(gin.TestMode)
	}

	router := gin.New()
	router.Use(gin.Recovery())
	router.MaxMultipartMemory = cfg.MaxUploadBytes

	router.GET("/health", func(ctx *gin.Context) {
		ctx.JSON(http.StatusOK, gin.H{
			"status":      "ok",
			"service":     "mediaflow-api",
			"environment": cfg.AppEnv,
		})
	})

	if videoService != nil {
		videos.NewHandler(videoService).RegisterRoutes(router)
	}

	return router
}
