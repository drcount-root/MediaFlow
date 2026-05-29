package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"mediaflow/apps/api/internal/config"
)

func NewRouter(cfg config.Config) http.Handler {
	if cfg.AppEnv == "test" {
		gin.SetMode(gin.TestMode)
	}

	router := gin.New()
	router.Use(gin.Recovery())

	router.GET("/health", func(ctx *gin.Context) {
		ctx.JSON(http.StatusOK, gin.H{
			"status":      "ok",
			"service":     "mediaflow-api",
			"environment": cfg.AppEnv,
		})
	})

	return router
}
