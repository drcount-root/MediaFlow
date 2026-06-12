# MediaFlow Implementation Plan

## Goal

Build a hardcore, portfolio-grade distributed video platform in two phases.

Phase 1 (complete) was the narrow MVP pipeline:

```txt
Upload MP4 -> Queue Job -> FFmpeg creates HLS -> hls.js plays adaptive stream
```

Phase 2 (current) turns MediaFlow into a serious system design project. Each milestone takes a real weakness that exists in the Phase 1 code today, fixes it with a canonical distributed-systems pattern, and then proves the fix under failure and load. The point is not more features; it is correctness, scalability, observability, and evidence.

```txt
M4 Correctness under failure   -> outbox, leases, retries, DLQ, idempotency
M5 Scalable ingest             -> presigned multipart direct-to-storage uploads
M6 Distributed transcoding     -> fan-out renditions, aggregation, parallel workers
M7 Serving at scale            -> signed manifests, edge cache, Redis
M8 Observability               -> traces across the queue, metrics, dashboards
M9 Proof                       -> SLOs, load tests, chaos experiments, postmortems
```

## Core Architecture

Current (Phase 1):

```txt
Frontend
  -> Upload API (proxies whole file)
  -> MinIO: raw video
  -> RabbitMQ: video.transcode
  -> Worker: FFmpeg transcode + thumbnail (whole video, one worker)
  -> MinIO: HLS output
  -> DB status update
  -> Frontend player loads presigned master.m3u8
```

Target (Phase 2):

```txt
Frontend
  -> Upload API: creates upload session, issues presigned multipart URLs
  -> Browser uploads parts directly to MinIO
  -> API completes session: video + job + outbox row in one DB transaction
  -> Outbox relay publishes to RabbitMQ (at-least-once, publisher confirms)
  -> Planner worker: probe + thumbnail + fan out per-rendition jobs
  -> N rendition workers transcode in parallel (leases, heartbeats, retries)
  -> Finalizer assembles master manifest when the last rendition completes
  -> Playback: API rewrites manifests with HMAC-signed segment URLs
  -> nginx edge cache (CDN stand-in) serves segments, validates signatures
  -> Redis: manifest cache, rate limiting, view counters
  -> OpenTelemetry trace spans the whole pipeline; Prometheus + Grafana watch it
```

## Stack

| Area | Choice |
| --- | --- |
| Frontend | Next.js, hls.js |
| Backend API | Go + Gin |
| Queue | RabbitMQ (TTL retry queues + DLX) |
| Database | PostgreSQL (`database/sql` + pgx) |
| Cache / counters / rate limiting | Redis |
| Object storage | MinIO locally, S3-compatible later |
| Video processing | FFmpeg |
| Edge cache (CDN stand-in) | nginx (`proxy_cache` + `secure_link`) |
| Tracing | OpenTelemetry + Jaeger |
| Metrics | Prometheus + Grafana |
| Load testing | k6 |
| Local infrastructure | Docker Compose |
| Production later | Kubernetes, real CDN |

## Phase 1 Record (Shipped)

Milestones 0–3 are complete; the code is the source of truth. The contracts below
are what Phase 2 builds on and must not be broken accidentally.

### Video lifecycle

```txt
uploading -> uploaded -> queued -> processing -> ready
any non-terminal state -> failed (error_message explains why)
```

Each meaningful transition should write a `video_events` row.

### API surface

```txt
POST   /videos/upload            # multipart proxy upload (replaced in M5)
GET    /videos
GET    /videos/:id
GET    /videos/:id/playback      # presigned master.m3u8 (replaced in M7)
GET    /health
```

Error shape: `{"error": {"code": "...", "message": "..."}}` with 400/404/409/413/415/500/503 mapping.

### Queue contract

```txt
exchange: mediaflow.video (direct, durable)
queue/routing key: video.transcode
payload: { jobId, videoId, rawBucket, rawObjectKey, requestedAt }
```

The payload struct is duplicated in `apps/api/internal/videos/types.go` and
`apps/worker/internal/job/types.go`; keep them in sync.

### Storage layout

```txt
raw-videos/{videoId}/original.mp4                      # mediaflow-raw (private)
processed-videos/{videoId}/master.m3u8                 # mediaflow-processed
processed-videos/{videoId}/{quality}/index.m3u8
processed-videos/{videoId}/{quality}/segment_NNN.ts
thumbnails/{videoId}/default.jpg                       # mediaflow-thumbnails
```

Deterministic paths so retries can safely overwrite partial output. Original
filenames live only in PostgreSQL, never in object keys.

### Renditions

| Quality | Resolution | Approx bitrate |
| --- | --- | --- |
| 720p | 1280x720 | 2800k |
| 480p | 854x480 | 1400k |
| 360p | 640x360 | 800k |

Skip variants above source height. H.264 + AAC, 4s MPEG-TS segments, VOD playlists.

## Known Weaknesses Driving Phase 2

These exist in the code today. Each is the seed of a milestone.

1. **Dual-write problem** — `Service.Upload` (`apps/api/internal/videos/service.go`)
   does MinIO write → DB insert → RabbitMQ publish with no transaction spanning
   them. A failed publish strands a `queued` video forever. → M4 outbox.
2. **Stuck-job deadlock** — if a worker dies mid-transcode, RabbitMQ redelivers,
   but `ClaimJob` sees the job already claimed and skips it. The video is stuck
   in `processing` with no recovery path. → M4 leases + reaper.
3. **No retries** — `video_jobs.attempts` is never incremented; failures Nack
   without requeue; there is no DLQ. → M4 retry/backoff/DLQ.
4. **Ingest bottleneck** — the API proxies entire uploads (up to 500MB) through
   its own memory/network; uploads are not resumable. → M5 presigned multipart.
5. **Monolithic transcode** — one worker transcodes all renditions of a video
   serially in a single job; horizontal scaling is coarse. → M6 fan-out.
6. **Fake playback security** — only `master.m3u8` is presigned; variant
   playlists and segments are served from anonymous-download buckets. There is
   no caching tier. → M7 signed manifests + edge cache.
7. **Blind pipeline** — no traces, no metrics, no dashboards; Redis is
   provisioned and entirely unused. → M7/M8.
8. **No evidence** — no load tests, no chaos testing, no stated SLOs. → M9.

---

## Milestone 4: Correctness Under Failure

The pipeline must survive any single component dying at any moment, with every
video eventually `ready` or `failed` — never stuck.

### 4.1 Transactional outbox

- New `outbox_messages` table. `Upload` writes video + job + outbox row in one
  DB transaction and never talks to RabbitMQ directly.
- A relay loop in the API process polls the outbox (`FOR UPDATE SKIP LOCKED`),
  publishes with publisher confirms, marks rows sent. Delivery is at-least-once;
  consumers must be idempotent (they already partially are).

```sql
CREATE TABLE outbox_messages (
  id UUID PRIMARY KEY,
  exchange TEXT NOT NULL,
  routing_key TEXT NOT NULL,
  payload_json JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ
);
CREATE INDEX idx_outbox_unpublished ON outbox_messages(created_at) WHERE published_at IS NULL;
```

### 4.2 Leases, heartbeats, reaper

- `video_jobs` gains `claimed_by TEXT` and `lease_expires_at TIMESTAMPTZ`.
- Claiming a job sets both (lease ~2 minutes) and increments `attempts`.
- The worker heartbeats every ~30s to extend the lease while FFmpeg runs.
- A reaper (ticker in the worker process, or `cmd/reaper`) scans for expired
  leases: below max attempts → reset to `queued` and re-enqueue via the outbox;
  at max attempts → mark job and video `failed`.

### 4.3 Retries, backoff, DLQ

- Max 3 attempts per job.
- Transient failure below max attempts → publish to `video.transcode.retry`
  with per-message TTL (30s · 2^attempts); that queue's dead-letter exchange
  routes the message back to `video.transcode`.
- Exhausted or poison messages → `video.transcode.dlq`, video marked `failed`.
- Distinguish permanent failures (corrupt input, no video stream) and skip
  retries for them.

### 4.4 Idempotency and clean shutdown

- Upload accepts an `Idempotency-Key` header; replays return the original
  response instead of creating a duplicate video.
- Worker retry hygiene: skip if video already `ready`; clear stale
  `video_variants`; deterministic output keys overwrite partial output.
- Graceful shutdown: on SIGTERM stop consuming, finish the in-flight job within
  a deadline, then exit. `kill -9` is what the reaper is for.

### Done when

- `kill -9` on the worker mid-transcode: the lease expires, the reaper
  requeues, another run completes, the video becomes `ready`.
- RabbitMQ stopped during an upload: the upload still succeeds and the outbox
  drains after RabbitMQ returns.
- A poison message lands in the DLQ without wedging the consumer.
- Every status transition is visible in `video_events`.

## Milestone 5: Scalable Ingest

The API becomes a control plane; video bytes flow directly between the browser
and object storage via presigned multipart uploads, resumable across reloads.

### Design

- New `upload_sessions` table: id, title/description, object key, MinIO
  multipart `upload_id`, part size, declared total size, optional SHA-256,
  status (`pending | uploading | completed | aborted | expired`), `expires_at`.
- Endpoints:

```txt
POST   /uploads                      # create session, initiate multipart
GET    /uploads/:id                  # session status + already-uploaded parts (resume)
GET    /uploads/:id/parts/:n/url     # presigned URL for part n
POST   /uploads/:id/complete         # complete multipart, validate, enqueue via outbox
DELETE /uploads/:id                  # abort multipart
```

- Completion validates declared size and part checksums (ETags), then creates
  video + job + outbox row in one transaction — reusing the M4 machinery.
- A cleanup loop aborts expired sessions and their MinIO multipart uploads.
- Size limits enforced at session creation (declared) and completion (actual).
- Web uploader: slice the file, upload parts with bounded concurrency, retry
  individual parts, persist the session id so a page reload resumes from
  `GET /uploads/:id`, show real progress.
- The legacy proxy endpoint `POST /videos/upload` is removed (or kept briefly
  behind a flag for comparison benchmarks in M9).

### Done when

- A 500MB upload never transits the API process.
- Killing the tab mid-upload and reloading resumes from the completed parts.
- A tampered part (checksum mismatch) fails completion cleanly.

## Milestone 6: Distributed Transcoding

Replace the monolithic transcode job with a fan-out/aggregate pipeline so N
workers share one video's work — the map-reduce shape real video platforms use.

### Design

- Job hierarchy in `video_jobs`: `parent_job_id`, `job_type` of
  `plan | rendition | finalize`.
- **Planner** consumes `video.transcode`: downloads source, runs ffprobe, makes
  the thumbnail, decides target renditions, creates per-rendition job rows, and
  fans out messages to `video.rendition` via the outbox.
- **Rendition workers** consume `video.rendition`: each transcodes exactly one
  quality (per-variant playlist + segments) and uploads it. Leases, heartbeats,
  retries from M4 apply per rendition — one quality failing and retrying does
  not redo the others.
- **Aggregation**: completion of each rendition atomically decrements a pending
  counter (`UPDATE ... RETURNING` in Postgres). Whoever completes the last
  rendition triggers finalize: write `master.m3u8` referencing the finished
  variants, insert `video_variants`, mark the video `ready`.
- Partial failure: a rendition exhausting retries fails the whole video and
  cleans up; already-finished sibling renditions are noted in `video_events`.
- Run multiple workers (`docker compose up --scale worker=3` or N processes)
  and demonstrate parallel speedup on a single video.
- Autoscaling experiment: poll queue depth via the RabbitMQ management API and
  scale workers; record the results (KEDA is the k8s version of this later).
- Stretch goal: segment-level parallelism — split the source into time chunks,
  transcode chunks of one rendition across workers, stitch the playlists.

### Done when

- Three workers make one video ready measurably faster than one worker.
- Two renditions finishing simultaneously do not double-finalize (the counter
  race is tested).
- A rendition failure mid-fan-out converges to `failed`, never to a stuck state.

## Milestone 7: Serving At Scale

Answer the playback question properly: no anonymous buckets, short-lived signed
URLs for every object, and a caching tier in front of storage.

### Design

- Remove `mc anonymous set download` from the MinIO setup; all buckets private.
- **Manifest rewriting**: the API serves playlists and signs segment URLs:

```txt
GET /videos/:id/hls/master.m3u8            # variant URIs point back at the API
GET /videos/:id/hls/:quality/index.m3u8    # segment URIs point at nginx, HMAC-signed
```

  Segment URLs carry an expiry and an HMAC over (path, expiry) using a shared
  secret (`PLAYBACK_HMAC_SECRET`).
- **nginx edge cache** (CDN stand-in) in Docker Compose: validates signatures
  with `secure_link`, proxies misses to MinIO, caches segments aggressively
  (immutable, long TTL) and playlists briefly. This is where cache hit ratio,
  keys, and invalidation get real.
- **Redis finally earns its keep**: cache rewritten manifests (TTL shorter than
  token expiry), token-bucket rate limiting on playback endpoints, view
  counters via `INCR` flushed periodically to Postgres.
- The web player switches to the manifest endpoint; quality selection keeps
  working because variant playlists still flow through hls.js.

### Done when

- Anonymous fetch of any segment fails; a signed URL works until expiry and
  401s after.
- Repeat playback of the same video shows nginx cache hits, not MinIO reads.
- A rate-limited client gets 429s while others stream unaffected.

## Milestone 8: Observability

One trace from upload to `ready`, metrics for every stage, dashboards that make
queue lag and failure visible at a glance.

### Design

- OpenTelemetry tracing in the API (Gin middleware) and worker. Trace context
  is injected into AMQP message headers by the outbox relay and extracted by
  consumers, so a single trace spans HTTP → queue → planner → renditions →
  finalize. Spans per worker stage: download, probe, thumbnail, transcode,
  upload, finalize.
- Prometheus metrics: HTTP latency/RPS/error rate (API); job duration per
  stage and per rendition, success/failure/retry counters (worker); queue depth
  and consumer counts (RabbitMQ prometheus plugin); cache hit ratio (nginx).
- Docker Compose grows `jaeger`, `prometheus`, `grafana` services with
  provisioned dashboards: pipeline health (queue depth, in-flight jobs, time-
  to-ready p50/p95, failure rate) and API health.
- Structured logging (`slog`) in both Go apps with trace/correlation IDs on
  every line; `video_events` rows carry the trace id in `metadata_json`.
- Draft alert rules: queue lag, error rate, jobs stuck in `processing`.

### Done when

- Jaeger shows one trace covering an upload through `ready`, including the
  queue hop and parallel rendition spans.
- The Grafana pipeline dashboard makes a killed worker or a queue backlog
  visible without reading logs.

## Milestone 9: Proof — SLOs, Load, Chaos

"Hardcore" means demonstrated, not claimed. State objectives, push the system
until they break, break the system on purpose, and write down what happened.

### Design

- **SLOs** (tune to local hardware, but state them in `docs/SLOS.md`), e.g.:
  - p95 upload-session creation < 100ms
  - p95 time-to-ready for a 60s 720p source < 90s with 3 workers
  - p95 playback manifest latency < 50ms warm
  - 0 videos stuck in a non-terminal state after any chaos scenario
- **k6 load tests** under `tests/load/`: upload throughput (multipart flow),
  playback concurrency against the edge cache, and a sustained soak of
  continuous uploads. Results recorded against the SLOs.
- **Chaos suite** under `tests/chaos/` — scripted scenarios, each with a
  postmortem in `docs/postmortems/` (timeline, observed behavior, gaps found,
  fixes filed):
  1. `kill -9` a rendition worker mid-transcode
  2. Restart RabbitMQ under load
  3. MinIO unavailable during transcode
  4. `WORK_DIR` disk full
  5. Postgres restart under load
- **E2E smoke script** that runs the full upload → ready → playback path
  against a fresh compose stack.
- **Docs**: architecture diagram of the final system and short ADRs in
  `docs/adr/` for the major decisions (outbox, leases, fan-out, manifest
  signing).

### Done when

- Every chaos scenario converges with zero stuck videos and has a postmortem.
- Load test results vs SLOs are committed.
- A newcomer can understand the system from the diagram and ADRs alone.

---

## Build Order

Strictly in milestone order — correctness before scale, scale before polish:

1. M4: outbox → leases/reaper → retries/DLQ → idempotency → shutdown → chaos checks.
2. M5: sessions table → API endpoints → web uploader → remove proxy path.
3. M6: job hierarchy → planner → rendition workers → aggregation → multi-worker runs.
4. M7: private buckets → manifest endpoints → signing → nginx → Redis.
5. M8: tracing → metrics → compose services → dashboards.
6. M9: SLOs → load tests → chaos suite → postmortems → final docs.

Each milestone lands with tests, a `PROGRESS.md` update, and migrations under
`infrastructure/migrations/` (numbered `000002_...` onward).

## New Environment Variables (introduced per milestone)

```txt
M4: OUTBOX_POLL_INTERVAL, JOB_LEASE_SECONDS, JOB_MAX_ATTEMPTS, WORKER_ID
M5: UPLOAD_SESSION_TTL, UPLOAD_PART_SIZE
M7: PLAYBACK_HMAC_SECRET, EDGE_BASE_URL, REDIS_ADDR (finally used)
M8: OTEL_EXPORTER_OTLP_ENDPOINT, METRICS_ADDR
```

Keep `.env.example` files current when these land.

## Key Engineering Rules

- Do not transcode inside any HTTP request path.
- Every queue consumer must be idempotent; delivery is at-least-once everywhere.
- No video may ever be stuck: every state must have a path to `ready` or `failed` driven by a timeout, retry, or reaper.
- All DB-then-publish sequences go through the outbox; never dual-write.
- Deterministic object keys so retries overwrite rather than duplicate.
- Each milestone is proven by a failure drill or measurement, not just tests passing.
- Keep contracts (queue payloads, object key layout, API JSON) in sync across `apps/api`, `apps/worker`, and `apps/web/lib/api.ts`.

## Resolved Decisions

- Playback strategy: API manifest rewriting + HMAC-signed segment URLs behind an nginx edge cache (was an open question).
- SQL access: stay on `database/sql` + pgx; no ORM.
- Publish strategy: transactional outbox, at-least-once delivery.
- Retry transport: RabbitMQ TTL retry queue + dead-letter exchange.

## Open Decisions

- Segment-level parallel transcoding (M6 stretch): worth the stitching complexity locally, or document the design and skip?
- CMAF/fMP4 segments instead of MPEG-TS: revisit if/when LL-HLS becomes interesting.
- Kubernetes deployment with KEDA autoscaling: only after M9; local compose autoscaling experiment comes first.
- Auth/multi-tenancy: deliberately out of scope for Phase 2; the `users` table stays dormant.
