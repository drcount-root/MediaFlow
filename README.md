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

A migration runner will be added when the API scaffold is implemented.

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

## Current Status

Milestone 0 is complete. Milestone 1 has started with the Go API scaffold and health endpoint.
