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

## Current Status

Milestones 0 and 1 are complete. The next focus is Milestone 2: the worker transcoding path.
