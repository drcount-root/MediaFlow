package config

import (
	"os"
	"strconv"
)

type Config struct {
	AppEnv               string
	DatabaseURL          string
	RabbitMQURL          string
	MinIOEndpoint        string
	MinIOAccessKey       string
	MinIOSecretKey       string
	MinIOUseSSL          bool
	MinIORawBucket       string
	MinIOProcessedBucket string
	MinIOThumbnailBucket string
	WorkerConcurrency    int
	WorkDir              string
	FFmpegPath           string
	FFprobePath          string
}

func Load() Config {
	return Config{
		AppEnv:               getEnv("APP_ENV", "local"),
		DatabaseURL:          getEnv("DATABASE_URL", "postgres://mediaflow:mediaflow@localhost:55432/mediaflow?sslmode=disable"),
		RabbitMQURL:          getEnv("RABBITMQ_URL", "amqp://mediaflow:mediaflow@localhost:5672/"),
		MinIOEndpoint:        getEnv("MINIO_ENDPOINT", "localhost:9000"),
		MinIOAccessKey:       getEnv("MINIO_ACCESS_KEY", "mediaflow"),
		MinIOSecretKey:       getEnv("MINIO_SECRET_KEY", "mediaflow-secret"),
		MinIOUseSSL:          getBoolEnv("MINIO_USE_SSL", false),
		MinIORawBucket:       getEnv("MINIO_RAW_BUCKET", "mediaflow-raw"),
		MinIOProcessedBucket: getEnv("MINIO_PROCESSED_BUCKET", "mediaflow-processed"),
		MinIOThumbnailBucket: getEnv("MINIO_THUMBNAIL_BUCKET", "mediaflow-thumbnails"),
		WorkerConcurrency:    getIntEnv("WORKER_CONCURRENCY", 1),
		WorkDir:              getEnv("WORK_DIR", "/tmp/mediaflow-worker"),
		FFmpegPath:           getEnv("FFMPEG_PATH", "ffmpeg"),
		FFprobePath:          getEnv("FFPROBE_PATH", "ffprobe"),
	}
}

func getEnv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

func getBoolEnv(key string, fallback bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func getIntEnv(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}
