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
	router.Use(corsMiddleware())
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

func corsMiddleware() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		ctx.Header("Access-Control-Allow-Origin", "http://localhost:3000")
		ctx.Header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
		ctx.Header("Access-Control-Allow-Headers", "Content-Type")

		if ctx.Request.Method == http.MethodOptions {
			ctx.AbortWithStatus(http.StatusNoContent)
			return
		}

		ctx.Next()
	}
}
