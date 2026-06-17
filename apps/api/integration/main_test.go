//go:build integration

// Package integration holds tests that run against real Postgres, RabbitMQ, and
// MinIO instances started with testcontainers-go. They are gated behind the
// `integration` build tag so the default `go test ./...` run stays hermetic.
//
//	go test -tags integration ./...
//
// A Docker daemon must be reachable. The shared containers are started once in
// TestMain and reused across tests; individual tests reset the state they touch.
package integration

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	miniocreds "github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/testcontainers/testcontainers-go"
	tcminio "github.com/testcontainers/testcontainers-go/modules/minio"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcrabbitmq "github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	"github.com/testcontainers/testcontainers-go/wait"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	rawBucket       = "mediaflow-raw"
	processedBucket = "mediaflow-processed"
	thumbnailBucket = "mediaflow-thumbnails"
)

// Shared infrastructure handles, populated by TestMain.
var infra struct {
	databaseURL    string
	rabbitURL      string
	rabbitMgmtURL  string
	minioEndpoint  string
	minioAccessKey string
	minioSecretKey string
}

func TestMain(m *testing.M) {
	ctx := context.Background()

	terminate, err := startInfra(ctx)
	if err != nil {
		log.Printf("integration setup failed: %v", err)
		terminate()
		os.Exit(1)
	}

	code := m.Run()
	terminate()
	os.Exit(code)
}

// startInfra boots the three backing services and returns a cleanup function
// that is always safe to call, even on partial startup failure.
func startInfra(ctx context.Context) (func(), error) {
	var containers []testcontainers.Container
	terminate := func() {
		for i := len(containers) - 1; i >= 0; i-- {
			_ = containers[i].Terminate(context.Background())
		}
	}

	pg, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("mediaflow"),
		tcpostgres.WithUsername("mediaflow"),
		tcpostgres.WithPassword("mediaflow"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if pg != nil {
		containers = append(containers, pg)
	}
	if err != nil {
		return terminate, fmt.Errorf("start postgres: %w", err)
	}
	infra.databaseURL, err = pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return terminate, fmt.Errorf("postgres connection string: %w", err)
	}

	rabbit, err := tcrabbitmq.Run(ctx, "rabbitmq:3.13-management-alpine",
		tcrabbitmq.WithAdminUsername("mediaflow"),
		tcrabbitmq.WithAdminPassword("mediaflow"),
	)
	if rabbit != nil {
		containers = append(containers, rabbit)
	}
	if err != nil {
		return terminate, fmt.Errorf("start rabbitmq: %w", err)
	}
	infra.rabbitURL, err = rabbit.AmqpURL(ctx)
	if err != nil {
		return terminate, fmt.Errorf("rabbitmq amqp url: %w", err)
	}
	infra.rabbitMgmtURL, err = rabbit.HttpURL(ctx)
	if err != nil {
		return terminate, fmt.Errorf("rabbitmq management url: %w", err)
	}

	mc, err := tcminio.Run(ctx, "minio/minio:latest",
		tcminio.WithUsername("mediaflow"),
		tcminio.WithPassword("mediaflow-secret"),
	)
	if mc != nil {
		containers = append(containers, mc)
	}
	if err != nil {
		return terminate, fmt.Errorf("start minio: %w", err)
	}
	infra.minioEndpoint, err = mc.ConnectionString(ctx)
	if err != nil {
		return terminate, fmt.Errorf("minio connection string: %w", err)
	}
	infra.minioAccessKey = mc.Username
	infra.minioSecretKey = mc.Password

	if err := applyMigrations(ctx, infra.databaseURL); err != nil {
		return terminate, fmt.Errorf("apply migrations: %w", err)
	}
	if err := ensureBuckets(ctx); err != nil {
		return terminate, fmt.Errorf("ensure buckets: %w", err)
	}

	return terminate, nil
}

// applyMigrations runs every infrastructure/migrations/*.sql file in order,
// mirroring cmd/migrate so the integration schema matches production.
func applyMigrations(ctx context.Context, databaseURL string) error {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return err
	}
	defer db.Close()

	if err := pingUntilReady(ctx, db); err != nil {
		return err
	}

	for _, file := range migrationFiles() {
		contents, err := os.ReadFile(file)
		if err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, string(contents)); err != nil {
			return fmt.Errorf("migration %s: %w", filepath.Base(file), err)
		}
	}
	return nil
}

func pingUntilReady(ctx context.Context, db *sql.DB) error {
	deadline := time.Now().Add(30 * time.Second)
	var err error
	for time.Now().Before(deadline) {
		if err = db.PingContext(ctx); err == nil {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("database never became ready: %w", err)
}

func migrationFiles() []string {
	_, thisFile, _, _ := runtime.Caller(0)
	dir := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "infrastructure", "migrations")

	entries, err := os.ReadDir(dir)
	if err != nil {
		log.Fatalf("read migrations dir %s: %v", dir, err)
	}

	var files []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		files = append(files, filepath.Join(dir, entry.Name()))
	}
	sort.Strings(files)
	return files
}

func ensureBuckets(ctx context.Context) error {
	client, err := minioClient()
	if err != nil {
		return err
	}

	for _, bucket := range []string{rawBucket, processedBucket, thumbnailBucket} {
		exists, err := client.BucketExists(ctx, bucket)
		if err != nil {
			return err
		}
		if exists {
			continue
		}
		if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
			return err
		}
	}
	return nil
}

func minioClient() (*minio.Client, error) {
	return minio.New(infra.minioEndpoint, &minio.Options{
		Creds:  miniocreds.NewStaticV4(infra.minioAccessKey, infra.minioSecretKey, ""),
		Secure: false,
	})
}

// openDB returns a connection to the shared Postgres instance.
func openDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", infra.databaseURL)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// truncateAll resets mutable tables so tests do not see each other's rows.
func truncateAll(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.ExecContext(context.Background(),
		`TRUNCATE videos, video_jobs, video_variants, video_events, outbox_messages RESTART IDENTITY CASCADE`)
	if err != nil {
		t.Fatalf("truncate: %v", err)
	}
}
