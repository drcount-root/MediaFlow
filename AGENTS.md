# Repository Guidelines

## Project Structure & Module Organization

This repository is for **MediaFlow**, a video upload and adaptive streaming platform. The detailed implementation plan lives in `MEDIAFLOW_PLAN.md`; consult it before making architectural changes.

Planned structure:

```txt
apps/api/                 # Go API: uploads, metadata, playback URLs
apps/worker/              # Transcoding worker: RabbitMQ + FFmpeg
apps/web/                 # Next.js frontend
packages/shared/          # Shared contracts or generated types, if needed
infrastructure/           # Docker Compose, migrations, deployment assets
```

Keep service-specific code inside its app folder. Put database migrations under `infrastructure/migrations/`. Store test fixtures in each app’s test directory unless they are shared.

## Build, Test, and Development Commands

The repo is currently pre-scaffold. Use these command names once tooling is added:

```bash
docker compose -f infrastructure/docker-compose.yml up
```
Starts PostgreSQL, RabbitMQ, Redis, and MinIO.

```bash
cd apps/api && go test ./...
```
Runs Go API tests.

```bash
cd apps/api && go run ./cmd/api
```
Runs the API server on `localhost:8080`.

```bash
npm run dev
npm test
npm run lint
```
Runs the web app locally, executes frontend tests, and checks lint rules.

Prefer adding a root `Makefile` later with targets such as `make dev`, `make test`, and `make lint`.

## Coding Style & Naming Conventions

Use `gofmt` for Go code and the project’s configured formatter for frontend code. Prefer clear package names such as `storage`, `queue`, `videos`, and `transcode`.

Use lowercase kebab-case for route paths and object storage keys, for example `processed-videos/{videoId}/master.m3u8`. Use snake_case for database columns and JSON camelCase for API responses.

## Testing Guidelines

Add tests with each meaningful behavior change. API tests should cover upload validation, metadata persistence, queue publishing, and playback responses. Worker tests should cover job parsing, failure handling, FFmpeg orchestration, and idempotency.

Name Go tests `TestFeatureBehavior`. Name frontend tests near the component or route they verify.

## Commit & Pull Request Guidelines

There is no Git history yet, so use concise conventional commits:

```txt
feat(api): add video upload endpoint
fix(worker): handle missing source objects
docs: expand local setup guide
```

Pull requests should include a short summary, test evidence, linked issue if available, and screenshots or recordings for UI changes. Call out schema, queue, storage, or environment variable changes explicitly.

## Security & Configuration Tips

Do not commit secrets, real credentials, or large media files. Keep local configuration in `.env` files excluded from Git. Raw uploads should remain private; expose playback through presigned URLs or controlled API responses.
