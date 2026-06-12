# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

MediaFlow is a YouTube-like video upload and adaptive streaming platform, now being built out as a hardcore distributed-systems project. A user uploads an MP4, it gets transcoded to multi-quality HLS with FFmpeg, and plays back via hls.js. The MVP pipeline (Milestones 0–3) is complete. Phase 2 is Milestones 4–10 (CI/testcontainers, transactional outbox + leases/retries/DLQ, presigned multipart uploads, fan-out transcoding, signed manifests + edge cache + SSE, OpenTelemetry, load/chaos/DR testing); Phase 3 is Milestones 11–12 (analytics pipeline + storyboards, auth/quotas/fair scheduling); Phase 4 capstones (live streaming, Kubernetes) are picked after M10. Designs live in `MEDIAFLOW_PLAN.md` — read the relevant milestone section there before implementing it. Status and milestone checklists are in `PROGRESS.md` (update it after completing tasks — see its "Update Rules" section). Repo conventions are in `AGENTS.md`.

Engineering rules from the plan: never dual-write DB-then-publish — go through the outbox; every queue consumer must be idempotent (delivery is at-least-once); every video state must have a path to `ready` or `failed` via retry, timeout, or reaper; the DB is the source of truth and Redis-derived state is rebuildable; each milestone is proven with a failure drill or measurement, not just passing tests, and ships its ADR/docs alongside the code.

## Commands

```bash
# Infrastructure (PostgreSQL, RabbitMQ, Redis, MinIO) — required before running services
docker compose -f infrastructure/docker-compose.yml up -d
docker compose -f infrastructure/docker-compose.yml down

# API (from apps/api/)
go run ./cmd/api          # serves on :8080
go run ./cmd/migrate      # applies infrastructure/migrations/*.sql
go test ./...             # all tests
go test ./internal/videos -run TestFeatureBehavior   # single test

# Worker (from apps/worker/) — requires ffmpeg/ffprobe on PATH
go run ./cmd/worker
go test ./...

# Web (from apps/web/)
npm run dev               # http://localhost:3000
npm run lint
npm run build             # also type-checks
```

There is no root build system; `apps/api` and `apps/worker` are separate Go modules (`mediaflow/apps/api`, `mediaflow/apps/worker`) and `apps/web` is a standalone npm package. Local config comes from `.env` files (see `.env.example` in root and in each app). Postgres is on the non-standard port **55432**.

## Architecture: the video pipeline (as currently implemented)

This describes the code as it exists today — the Phase 1 MVP. Phase 2 milestones in `MEDIAFLOW_PLAN.md` deliberately replace parts of this flow (the upload proxy, the direct publish, the monolithic transcode job, the presigned-URL playback), so check PROGRESS.md for which milestone is in flight before assuming this section is current.

The system is three services connected by Postgres, RabbitMQ, and MinIO. Understanding the upload→playback flow is the key to the codebase:

1. **Upload** — `POST /videos/upload` (apps/api/internal/videos): validates MP4 + size limit, streams the file to MinIO bucket `mediaflow-raw` under `raw-videos/{videoId}/original.mp4`, creates `videos` (status `queued`) + `video_jobs` rows, then publishes a `TranscodeJob` JSON message to RabbitMQ exchange `mediaflow.video`, routing key/queue `video.transcode`.
2. **Transcode** — the worker (apps/worker/internal/worker) consumes `video.transcode`, claims the job in the DB (idempotency guard — unclaimed/duplicate deliveries are skipped), downloads the raw file to `WORK_DIR/{jobId}`, runs ffprobe, generates a thumbnail and 720p/480p/360p HLS variants (apps/worker/internal/processor/ffmpeg.go), uploads everything to `mediaflow-processed` under `processed-videos/{videoId}/...` and `mediaflow-thumbnails`, inserts `video_variants`, and marks the video `ready`. On failure it marks job/video `failed` and Nacks without requeue.
3. **Playback** — `GET /videos/:id/playback` returns a 1-hour presigned MinIO URL for `master.m3u8` only when status is `ready`. The web watch page feeds that URL to hls.js with manual quality selection.

Video status lifecycle (DB enum): `uploading → uploaded → queued → processing → ready | failed`. The frontend polls `GET /videos/:id` to track it.

### Cross-service contracts

These are duplicated between the two Go modules and the web app, so changes must be kept in sync:

- **Queue message**: `TranscodeJob` is defined in both `apps/api/internal/videos/types.go` and `apps/worker/internal/job/types.go`.
- **Object key layout**: API writes `raw-videos/{videoId}/...`; worker writes `processed-videos/{videoId}/{quality}/index.m3u8`, `.../master.m3u8`, `thumbnails/{videoId}/default.jpg`.
- **DB schema**: `infrastructure/migrations/` (also auto-applied by Docker Compose on first Postgres volume creation). Both Go services use raw `database/sql` with pgx.
- **API JSON shapes**: mirrored in `apps/web/lib/api.ts` (camelCase JSON, snake_case DB columns).

### Service internals

Each Go app follows the same layout: `cmd/` entrypoints, `internal/config` (env-var loading), `internal/database` (repository), `internal/storage` (MinIO client). The API's business logic lives in `internal/videos` behind `Repository`/`ObjectStorage`/`JobPublisher` interfaces so handler/service tests run with fakes — no live infrastructure needed for `go test`. The router (`internal/http`) hardcodes CORS for `http://localhost:3000`.

The web app is Next.js App Router; pages are thin server components with client components for interactive parts (`upload-form.tsx`, `status-panel.tsx`, `watch-panel.tsx`, `components/HlsPlayer.tsx`).

## Conventions

- Conventional commits: `feat(api): ...`, `fix(worker): ...`, `docs: ...`.
- Go tests named `TestFeatureBehavior`; kebab-case for routes and object keys.
- Raw uploads stay private; playback goes through presigned URLs (processed/thumbnail buckets are anonymous-download locally, but don't rely on that in code).
