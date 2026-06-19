# Local Setup Guide

How to get the full MediaFlow pipeline — API, worker, and web — running on your
machine, upload a video, and watch it play. Tested on macOS; the steps are the
same on Linux.

The short version: start the infra with Docker Compose, then run three processes
(API, worker, web). The Go services read their config from environment variables
with defaults that already match the Compose stack, so **no `.env` files are
required for local development** — the defaults just work.

```
                ┌────────────┐   upload    ┌──────────────┐
   Browser ───▶ │  web :3000 │ ──────────▶ │   API :8080  │
                └────────────┘   poll       └──────┬───────┘
                       ▲  playback URL              │ store raw + publish job
                       │                            ▼
                ┌──────┴───────┐   HLS      ┌──────────────┐   ┌──────────────┐
                │  MinIO :9000 │ ◀───────── │ worker (FFmpeg)│◀─│ RabbitMQ:5672│
                └──────────────┘            └──────────────┘   └──────────────┘
                         ▲ Postgres :55432 is the source of truth for all three
```

---

## 1. Prerequisites

| Tool | Version | Check | Install (macOS) |
| --- | --- | --- | --- |
| Docker | daemon running | `docker info` | [Docker Desktop](https://www.docker.com/products/docker-desktop/) |
| Go | 1.25+ | `go version` | `brew install go` |
| Node.js | 22+ | `node --version` | `brew install node` |
| FFmpeg + ffprobe | any recent | `ffmpeg -version` | `brew install ffmpeg` |

`ffmpeg`/`ffprobe` must be on your `PATH` — the **worker** shells out to them.
The API and web do not need FFmpeg.

> Docker Desktop must be **running** before the infra step. On macOS you can
> launch it with `open -a Docker` and wait until `docker info` succeeds.

---

## 2. Start the infrastructure

From the repo root:

```bash
docker compose -f infrastructure/docker-compose.yml up -d
```

This starts five containers: PostgreSQL, RabbitMQ, Redis, MinIO, and a one-shot
`minio-setup` job that creates the three buckets. Wait until they report healthy:

```bash
docker compose -f infrastructure/docker-compose.yml ps
```

What you get:

| Service | Address | Console / UI | Credentials |
| --- | --- | --- | --- |
| PostgreSQL | `localhost:55432` | — | `mediaflow` / `mediaflow` (db `mediaflow`) |
| RabbitMQ | `localhost:5672` | http://localhost:15672 | `mediaflow` / `mediaflow` |
| Redis | `localhost:6379` | — | — |
| MinIO (S3 API) | `localhost:9000` | http://localhost:9001 | `mediaflow` / `mediaflow-secret` |

> Note: Postgres is on the **non-standard port 55432** to avoid clashing with a
> local Postgres on 5432.

### Database schema

Compose applies `infrastructure/migrations/*.sql` automatically the first time
the Postgres volume is created. To (re)apply migrations manually — safe to run
anytime, it is idempotent and tracks applied versions in `schema_migrations`:

```bash
cd apps/api
go run ./cmd/migrate
```

---

## 3. Run the three services

Run each in its **own terminal** so you can see the logs. Leave them running.

### Terminal A — API (`:8080`)

```bash
cd apps/api
go run ./cmd/api
```

Verify it is up:

```bash
curl http://localhost:8080/health
# {"environment":"local","service":"mediaflow-api","status":"ok"}
```

### Terminal B — Worker (needs FFmpeg on PATH)

```bash
cd apps/worker
go run ./cmd/worker
```

You should see `worker consuming queue=video.transcode`. This process pulls
transcode jobs off RabbitMQ, downloads the raw MP4 from MinIO, runs FFmpeg, and
uploads HLS output back to MinIO.

### Terminal C — Web (`:3000`)

```bash
cd apps/web
npm install        # first time only
npm run dev
```

Open http://localhost:3000.

---

## 4. End-to-end smoke test

1. Go to http://localhost:3000/upload and upload a small `.mp4` (max 500 MB).
2. You are taken to the video page; status polls `queued → processing → ready`.
   Watch Terminal B — the worker logs the download / probe / HLS / upload steps.
3. When status is `ready`, open the watch page and play it. Use the quality menu
   to switch renditions (360p/480p/720p depending on the source resolution).

Ingest (M6) is **presigned multipart**: the API is a control plane and the file
bytes go from the browser straight to MinIO, never through the API process. The
scripted flow is therefore multi-step:

```bash
# 1. Create a session (declare size + part size); returns id, partCount, etc.
curl -X POST http://localhost:8080/uploads -H 'Content-Type: application/json' \
  -d '{"title":"Demo","originalFilename":"clip.mp4","contentType":"video/mp4",
       "totalSize":<BYTES>,"partSize":8388608}'

# 2. For each part n: get a presigned PUT URL, then PUT the byte range to MinIO
#    directly (the URL host is :9000, not the API). Capture each ETag header.
curl http://localhost:8080/uploads/<SESSION_ID>/parts/<n>/url

# 3. Complete with the part list; creates the video + enqueues transcode.
curl -X POST http://localhost:8080/uploads/<SESSION_ID>/complete \
  -H 'Content-Type: application/json' \
  -d '{"parts":[{"partNumber":1,"etag":"<ETAG>"}]}'

# Poll status / get a 1-hour presigned playback URL once "ready"
curl http://localhost:8080/videos/<VIDEO_ID>
curl http://localhost:8080/videos/<VIDEO_ID>/playback
```

> The old single-shot proxy `POST /videos/upload` (streams bytes through the API)
> is disabled by default since M6; set `ENABLE_LEGACY_UPLOAD=true` to re-enable it
> for comparison benchmarks.

You can inspect the produced objects in the MinIO console
(http://localhost:9001): raw uploads under `mediaflow-raw`, HLS output under
`mediaflow-processed`, thumbnails under `mediaflow-thumbnails`.

---

## 5. Configuration (optional)

Defaults live in `config.Load()` in each app and match the Compose stack, so you
usually do not need to set anything. To override, export the variable before
running the process (the Go apps read OS env directly; they do **not** auto-load
`.env` files). The available knobs are documented in:

- `.env.example` (root) — every variable in one place
- `apps/api/.env.example`, `apps/worker/.env.example`, `apps/web/.env.example`

The web app does follow Next.js conventions: create `apps/web/.env.local` to set
`NEXT_PUBLIC_API_BASE_URL` if your API is not on `http://localhost:8080`.

Common overrides:

| Variable | Default | Used by |
| --- | --- | --- |
| `HTTP_ADDR` | `:8080` | api |
| `MAX_UPLOAD_BYTES` | `524288000` (500 MB) | api |
| `WORK_DIR` | `/tmp/mediaflow-worker` | worker scratch dir |
| `WORKER_CONCURRENCY` | `1` | worker prefetch |
| `FFMPEG_PATH` / `FFPROBE_PATH` | `ffmpeg` / `ffprobe` | worker |
| `NEXT_PUBLIC_API_BASE_URL` | `http://localhost:8080` | web |

---

## 6. Running the tests

Unit tests (no infra needed — they use fakes):

```bash
cd apps/api    && go test ./...
cd apps/worker && go test ./...
cd apps/web    && npm run lint && npm run build
```

Integration tests (gated behind the `integration` build tag; spin up real
Postgres/RabbitMQ/MinIO via testcontainers — **Docker must be running**, and the
worker suite also needs FFmpeg):

```bash
cd apps/api    && go test -tags integration ./...
cd apps/worker && go test -tags integration ./...
```

See `docs/adr/0001-ci-and-integration-harness.md` for how the harness works.

---

## 7. Teardown

```bash
# Stop services (keeps data volumes)
docker compose -f infrastructure/docker-compose.yml down

# Stop AND wipe all data (Postgres, RabbitMQ, MinIO volumes)
docker compose -f infrastructure/docker-compose.yml down -v
```

Stop the API/worker/web processes with `Ctrl-C` in their terminals.

---

## 8. Troubleshooting

| Symptom | Likely cause / fix |
| --- | --- |
| `Cannot connect to the Docker daemon` | Docker Desktop isn't running. Start it, wait for `docker info` to succeed. |
| API/worker can't connect to Postgres | Infra not up/healthy, or port 55432 in use. `docker compose ... ps`; free the port or stop a local Postgres. |
| Video stuck in `queued` | Worker isn't running, or it can't reach RabbitMQ. Check Terminal B; confirm RabbitMQ is healthy at http://localhost:15672. |
| Video goes to `failed` | Usually a worker/FFmpeg error. Check Terminal B logs and `error_message` on `GET /videos/<id>`. Confirm `ffmpeg -version` works. |
| `ffmpeg: executable file not found` | Install FFmpeg (`brew install ffmpeg`) and restart the worker. |
| Buckets missing in MinIO | The `minio-setup` container creates them. Re-run `docker compose ... up -d`, or create `mediaflow-raw` / `mediaflow-processed` / `mediaflow-thumbnails` in the console. |
| Playback URL 404 / CORS error | API allows only `http://localhost:3000`. Run the web app on port 3000, or adjust the CORS origin in `apps/api/internal/http/router.go`. |
| Port already in use (8080/3000) | Set `HTTP_ADDR` for the API, or `PORT=3001 npm run dev` for the web app. |
