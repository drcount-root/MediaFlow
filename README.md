# MediaFlow

MediaFlow is a YouTube-like video upload and adaptive streaming project. The MVP uploads MP4 videos, queues processing work, transcodes with FFmpeg, stores HLS output, and plays streams with `hls.js`.

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

Phase 1 (Milestones 0–3, the MVP pipeline) is complete. Phase 2 turns MediaFlow into a hardcore distributed-systems project — Milestones 4–10 cover CI with real-dependency integration tests, failure correctness (outbox, leases, retries, DLQ), presigned multipart uploads, distributed fan-out transcoding, signed manifests behind an edge cache with SSE status push, observability, and load/chaos/disaster-recovery drills. Phase 3 (Milestones 11–12) adds a watch-time analytics pipeline, storyboard seek previews, auth, quotas, and fair scheduling. The next focus is Milestone 4: CI and Integration Test Harness.
