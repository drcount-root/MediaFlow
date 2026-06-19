# MediaFlow

MediaFlow is a YouTube-like video upload and adaptive streaming project. The MVP uploads MP4 videos, queues processing work, transcodes with FFmpeg, stores HLS output, and plays streams with `hls.js`.

**New here? Follow [docs/LOCAL_SETUP.md](docs/LOCAL_SETUP.md)** for a step-by-step
guide to running the full pipeline locally (infra → API → worker → web → upload
and play a video).

Read [MEDIAFLOW_PLAN.md](MEDIAFLOW_PLAN.md) for the implementation plan and [PROGRESS.md](PROGRESS.md) for current status.

## Planned Layout

```txt
apps/api/                 # Go API
apps/worker/              # Queue-backed FFmpeg worker
apps/web/                 # Next.js frontend
packages/shared/          # Shared contracts if needed
infrastructure/           # Docker Compose and migrations
```

## Local Infrastructure

Start dependencies:

```bash
docker compose -f infrastructure/docker-compose.yml up -d
```

Stop dependencies:

```bash
docker compose -f infrastructure/docker-compose.yml down
```

Useful local URLs:

```txt
PostgreSQL: localhost:55432
RabbitMQ UI: http://localhost:15672
MinIO Console: http://localhost:9001
```

Default local credentials are documented in `.env.example`.

## Database

The initial schema is in:

```txt
infrastructure/migrations/000001_init.sql
```

Run migrations manually:

```bash
cd apps/api
go run ./cmd/migrate
```

Docker Compose also applies the initial schema on first Postgres volume creation.

## CI and Integration Tests

GitHub Actions (`.github/workflows/ci.yml`) runs on every push and PR: API and
worker `gofmt`/`go vet`/`go test`, web `lint`/`build`, and the integration suites.
See `docs/adr/0001-ci-and-integration-harness.md` for the design.

Integration tests are gated behind the `integration` build tag and use
[testcontainers-go] to spin up real Postgres, RabbitMQ, and MinIO. They need a
running Docker daemon (and `ffmpeg`/`ffprobe` on `PATH` for the worker pipeline
test, which skips when those are absent). Run them per module, exactly as CI does:

```bash
cd apps/api    && go test -tags integration ./...
cd apps/worker && go test -tags integration ./...
```

[testcontainers-go]: https://golang.testcontainers.org/

## API

Run tests:

```bash
cd apps/api
go test ./...
```

Run the API:

```bash
cd apps/api
go run ./cmd/api
```

Health check:

```bash
curl http://localhost:8080/health
```

## Worker

Run tests:

```bash
cd apps/worker
go test ./...
```

Run the worker:

```bash
cd apps/worker
go run ./cmd/worker
```

The worker consumes `video.transcode`, downloads the raw MP4 from MinIO, generates thumbnail and HLS outputs with FFmpeg, uploads processed assets, and marks the video `ready`.

## Web

Install dependencies:

```bash
cd apps/web
npm install
```

Run locally:

```bash
cd apps/web
npm run dev
```

Open:

```txt
http://localhost:3000
```

Build check:

```bash
cd apps/web
npm run build
```

## Current Status

Phase 1 (Milestones 0–3, the MVP pipeline) is complete. Phase 2 turns MediaFlow into a hardcore distributed-systems project — Milestones 4–10 cover CI with real-dependency integration tests, failure correctness (outbox, leases, retries, DLQ), presigned multipart uploads, distributed fan-out transcoding, signed manifests behind an edge cache with SSE status push, observability, and load/chaos/disaster-recovery drills. Phase 3 (Milestones 11–12) adds a watch-time analytics pipeline, storyboard seek previews, auth, quotas, and fair scheduling. Milestones 4 (CI + integration harness), 5 (correctness under failure — outbox, leases, retries, DLQ, idempotency, graceful shutdown), and 6 (scalable ingest — presigned multipart uploads straight from the browser, resumable across reloads) are complete and verified locally; the next focus is Milestone 7: Distributed Transcoding. See `PROGRESS.md` for the per-milestone checklist and `docs/adr/` for the design records.
