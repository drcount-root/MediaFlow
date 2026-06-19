package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	AppEnv               string
	HTTPAddr             string
	DatabaseURL          string
	RabbitMQURL          string
	RedisAddr            string
	MinIOEndpoint        string
	MinIOAccessKey       string
	MinIOSecretKey       string
	MinIOUseSSL          bool
	MinIORawBucket       string
	MinIOProcessedBucket string
	MinIOThumbnailBucket string
	MaxUploadBytes       int64
	OutboxPollInterval   time.Duration
	OutboxBatchSize      int
	UploadSessionTTL     time.Duration
	UploadPartURLTTL     time.Duration
	UploadSweepInterval  time.Duration
	UploadSweepBatchSize int
	EnableLegacyUpload   bool
}

func Load() Config {
	return Config{
		AppEnv:               getEnv("APP_ENV", "local"),
		HTTPAddr:             getEnv("HTTP_ADDR", ":8080"),
		DatabaseURL:          getEnv("DATABASE_URL", "postgres://mediaflow:mediaflow@localhost:55432/mediaflow?sslmode=disable"),
		RabbitMQURL:          getEnv("RABBITMQ_URL", "amqp://mediaflow:mediaflow@localhost:5672/"),
		RedisAddr:            getEnv("REDIS_ADDR", "localhost:6379"),
		MinIOEndpoint:        getEnv("MINIO_ENDPOINT", "localhost:9000"),
		MinIOAccessKey:       getEnv("MINIO_ACCESS_KEY", "mediaflow"),
		MinIOSecretKey:       getEnv("MINIO_SECRET_KEY", "mediaflow-secret"),
		MinIOUseSSL:          getBoolEnv("MINIO_USE_SSL", false),
		MinIORawBucket:       getEnv("MINIO_RAW_BUCKET", "mediaflow-raw"),
		MinIOProcessedBucket: getEnv("MINIO_PROCESSED_BUCKET", "mediaflow-processed"),
		MinIOThumbnailBucket: getEnv("MINIO_THUMBNAIL_BUCKET", "mediaflow-thumbnails"),
		MaxUploadBytes:       getInt64Env("MAX_UPLOAD_BYTES", 524288000),
		OutboxPollInterval:   getDurationEnv("OUTBOX_POLL_INTERVAL", time.Second),
		OutboxBatchSize:      int(getInt64Env("OUTBOX_BATCH_SIZE", 100)),
		UploadSessionTTL:     getDurationEnv("UPLOAD_SESSION_TTL", 24*time.Hour),
		UploadPartURLTTL:     getDurationEnv("UPLOAD_PART_URL_TTL", time.Hour),
		UploadSweepInterval:  getDurationEnv("UPLOAD_SWEEP_INTERVAL", 5*time.Minute),
		UploadSweepBatchSize: int(getInt64Env("UPLOAD_SWEEP_BATCH_SIZE", 100)),
		EnableLegacyUpload:   getBoolEnv("ENABLE_LEGACY_UPLOAD", false),
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

func getInt64Env(key string, fallback int64) int64 {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fallback
	}

	return parsed
}

func getDurationEnv(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}

	return parsed
}
