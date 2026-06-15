package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
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

	// Job leases (M5.2).
	WorkerID          string        // identifies this worker on the lease it holds
	JobLeaseDuration  time.Duration // how long a claim is valid before the reaper may take it
	JobMaxAttempts    int           // attempts before a job is failed permanently
	HeartbeatInterval time.Duration // how often a busy worker extends its lease
	ReaperInterval    time.Duration // how often the reaper scans for expired leases

	// Retries/DLQ (M5.3).
	RetryBaseDelay time.Duration // backoff base; per-message TTL is RetryBaseDelay * 2^attempts

	// Graceful shutdown (M5.4).
	ShutdownGrace time.Duration // how long to let an in-flight job finish on SIGTERM before aborting it
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
		WorkerID:             getEnv("WORKER_ID", defaultWorkerID()),
		JobLeaseDuration:     time.Duration(getIntEnv("JOB_LEASE_SECONDS", 120)) * time.Second,
		JobMaxAttempts:       getIntEnv("JOB_MAX_ATTEMPTS", 3),
		HeartbeatInterval:    getDurationEnv("JOB_HEARTBEAT_INTERVAL", 30*time.Second),
		ReaperInterval:       getDurationEnv("REAPER_INTERVAL", 30*time.Second),
		RetryBaseDelay:       getDurationEnv("JOB_RETRY_BASE_DELAY", 30*time.Second),
		ShutdownGrace:        getDurationEnv("WORKER_SHUTDOWN_GRACE", 30*time.Second),
	}
}

// defaultWorkerID identifies this process when it claims a job, so the reaper
// and logs can tell workers apart. Overridable via WORKER_ID.
func defaultWorkerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "worker"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
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
