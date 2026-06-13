# MediaFlow Implementation Plan

## Goal

Build a hardcore, portfolio-grade distributed video platform in phases.

Phase 1 (complete) was the narrow MVP pipeline:

```txt
Upload MP4 -> Queue Job -> FFmpeg creates HLS -> hls.js plays adaptive stream
```

Phase 2 (current) turns MediaFlow into a serious system design project. Each milestone takes a real weakness that exists in the Phase 1 code today, fixes it with a canonical distributed-systems pattern, and then proves the fix under failure and load. Phase 3 layers product-grade full-stack systems (analytics, auth, fairness) on the proven foundation. The point is not more features; it is correctness, scalability, observability, and evidence.

```txt
Phase 2 — correctness, scale, proof
  M4  CI and integration test harness  -> GitHub Actions, testcontainers
  M5  Correctness under failure        -> outbox, leases, retries, DLQ, idempotency
  M6  Scalable ingest                  -> presigned multipart direct-to-storage uploads
  M7  Distributed transcoding          -> fan-out renditions, aggregation, parallel workers
  M8  Serving at scale                 -> signed manifests, edge cache, Redis, SSE status push
  M9  Observability                    -> traces across the queue, metrics, dashboards
  M10 Proof                            -> SLOs, load tests, chaos drills, DR drill, postmortems

Phase 3 — product-grade full stack
  M11 Analytics and player intelligence -> watch-time pipeline, retention, storyboards
  M12 Auth, quotas, and fairness        -> JWT auth, per-user quotas, fair scheduling

Phase 4 — capstone (choose after M10)
  Live streaming (RTMP -> live HLS) and/or Kubernetes + real cloud deploy
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

Target (Phase 2/3):

```txt
Frontend
  -> Upload API: creates upload session, issues presigned multipart URLs
  -> Browser uploads parts directly to MinIO
  -> API completes session: video + job + outbox row in one DB transaction
  -> Outbox relay publishes to RabbitMQ (at-least-once, publisher confirms)
  -> Planner worker: probe + thumbnail + storyboard + fan out per-rendition jobs
  -> N rendition workers transcode in parallel (leases, heartbeats, retries)
  -> Finalizer assembles master manifest when the last rendition completes
  -> Status changes pushed to the browser over SSE (Redis pub/sub fan-out)
  -> Playback: API rewrites manifests with HMAC-signed segment URLs
  -> nginx edge cache (CDN stand-in) serves segments, validates signatures
  -> Player heartbeats -> ingest -> Redis Streams -> watch-time aggregates
  -> Redis: manifest cache, rate limiting, counters, pub/sub, streams
  -> OpenTelemetry trace spans the whole pipeline; Prometheus + Grafana watch it
  -> All of it gated by CI running integration tests against real dependencies
```

## Stack

| Area | Choice |
| --- | --- |
| Frontend | Next.js, hls.js |
| Backend API | Go + Gin |
| Queue | RabbitMQ (TTL retry queues + DLX) |
| Database | PostgreSQL (`database/sql` + pgx) |
| Cache / counters / rate limiting / pub-sub / streams | Redis |
| Object storage | MinIO locally, S3-compatible later |
| Video processing | FFmpeg |
| Edge cache (CDN stand-in) | nginx (`proxy_cache` + `secure_link`) |
| Realtime status | Server-Sent Events over Redis pub/sub |
| Tracing | OpenTelemetry + Jaeger |
| Metrics | Prometheus + Grafana |
| CI | GitHub Actions + testcontainers-go |
| Load testing | k6 |
| Local infrastructure | Docker Compose |
| Production later | Kubernetes, real CDN |

## Phase 1 Record (Shipped)

Milestones 0–3 are complete; the code is the source of truth. The contracts below
are what later phases build on and must not be broken accidentally.

### Video lifecycle

```txt
uploading -> uploaded -> queued -> processing -> ready
any non-terminal state -> failed (error_message explains why)
```

Each meaningful transition should write a `video_events` row.

### API surface

```txt
POST   /videos/upload            # multipart proxy upload (replaced in M6)
GET    /videos
GET    /videos/:id
GET    /videos/:id/playback      # presigned master.m3u8 (replaced in M8)
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

1. **No CI** — nothing runs the tests automatically, and the existing tests use
   fakes only; the DB/queue/storage integration code has no automated coverage.
   → M4 CI + testcontainers.
2. **Dual-write problem** — `Service.Upload` (`apps/api/internal/videos/service.go`)
   does MinIO write → DB insert → RabbitMQ publish with no transaction spanning
   them. A failed publish strands a `queued` video forever. → M5 outbox.
3. **Stuck-job deadlock** — if a worker dies mid-transcode, RabbitMQ redelivers,
   but `ClaimJob` sees the job already claimed and skips it. The video is stuck
   in `processing` with no recovery path. → M5 leases + reaper.
4. **No retries** — `video_jobs.attempts` is never incremented; failures Nack
   without requeue; there is no DLQ. → M5 retry/backoff/DLQ.
5. **Ingest bottleneck** — the API proxies entire uploads (up to 500MB) through
   its own memory/network; uploads are not resumable. → M6 presigned multipart.
6. **Monolithic transcode** — one worker transcodes all renditions of a video
   serially in a single job; horizontal scaling is coarse. → M7 fan-out.
7. **Fake playback security** — only `master.m3u8` is presigned; variant
   playlists and segments are served from anonymous-download buckets. There is
   no caching tier. Status updates rely on 2s polling. → M8 signed manifests +
   edge cache + SSE.
8. **Blind pipeline** — no traces, no metrics, no dashboards; Redis is
   provisioned and entirely unused. → M8/M9.
9. **No evidence** — no load tests, no chaos testing, no backup story, no
   stated SLOs. → M10.

---

## Milestone 4: CI and Integration Test Harness

Everything after this milestone is judged by a pipeline, not by "works on my
machine". Built first because every later milestone (outbox, reaper, fan-out)
is exactly the kind of infrastructure-coupled code that unit tests with fakes
cannot validate.

### Design

- GitHub Actions workflow on every push/PR:
  - `apps/api`: `gofmt` check, `go vet`, `go test ./...`
  - `apps/worker`: same, with `ffmpeg`/`ffprobe` installed in the runner
  - `apps/web`: `npm run lint`, `npm run build`
- Integration test suite (build tag `integration`) using `testcontainers-go`
  to start real Postgres, RabbitMQ, and MinIO per run. First targets:
  - repository code against real Postgres (migrations applied)
  - publish/consume round-trip against real RabbitMQ
  - upload → store → queue → worker-process flow with a generated fixture MP4
    (`ffmpeg -f lavfi -i testsrc ...` — never commit media files)
- Local entry point mirrors CI: `go test -tags integration ./...` per app.
- Dependency caching (Go modules, npm) to keep runs fast.

### Done when

- CI is green on the main branch and required for PRs.
- An integration test exercises the full upload→ready flow against real
  dependencies in CI.
- A deliberately introduced dual-write bug (the M5 target) is the kind of thing
  the harness *could* catch — the suite touches real Postgres and RabbitMQ.

## Milestone 5: Correctness Under Failure

The pipeline must survive any single component dying at any moment, with every
video eventually `ready` or `failed` — never stuck.

### 5.1 Transactional outbox

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

### 5.2 Leases, heartbeats, reaper

- `video_jobs` gains `claimed_by TEXT` and `lease_expires_at TIMESTAMPTZ`.
- Claiming a job sets both (lease ~2 minutes) and increments `attempts`.
- The worker heartbeats every ~30s to extend the lease while FFmpeg runs.
- A reaper (ticker in the worker process, or `cmd/reaper`) scans for expired
  leases: below max attempts → reset to `queued` and re-enqueue via the outbox;
  at max attempts → mark job and video `failed`.

### 5.3 Retries, backoff, DLQ

- Max 3 attempts per job.
- Transient failure below max attempts → publish to `video.transcode.retry`
  with per-message TTL (30s · 2^attempts); that queue's dead-letter exchange
  routes the message back to `video.transcode`.
- Exhausted or poison messages → `video.transcode.dlq`, video marked `failed`.
- Distinguish permanent failures (corrupt input, no video stream) and skip
  retries for them.

### 5.4 Idempotency and clean shutdown

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
- Integration tests cover outbox relay, lease expiry, and retry routing.

## Milestone 6: Scalable Ingest

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
  video + job + outbox row in one transaction — reusing the M5 machinery.
- A cleanup loop aborts expired sessions and their MinIO multipart uploads.
- Size limits enforced at session creation (declared) and completion (actual).
- Web uploader: slice the file, upload parts with bounded concurrency, retry
  individual parts, persist the session id so a page reload resumes from
  `GET /uploads/:id`, show real progress.
- The legacy proxy endpoint `POST /videos/upload` is removed (or kept briefly
  behind a flag for comparison benchmarks in M10).

### Done when

- A 500MB upload never transits the API process.
- Killing the tab mid-upload and reloading resumes from the completed parts.
- A tampered part (checksum mismatch) fails completion cleanly.

## Milestone 7: Distributed Transcoding

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
  retries from M5 apply per rendition — one quality failing and retrying does
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

## Milestone 8: Serving At Scale

Answer the playback question properly: no anonymous buckets, short-lived signed
URLs for every object, a caching tier in front of storage — and replace status
polling with real-time push.

### Design — signed playback behind an edge cache

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
- **Redis**: cache rewritten manifests (TTL shorter than token expiry),
  token-bucket rate limiting on playback endpoints, view counters via `INCR`
  flushed periodically to Postgres.
- The web player switches to the manifest endpoint; quality selection keeps
  working because variant playlists still flow through hls.js.

### Design — real-time status over SSE

- Workers publish status transitions to a Redis pub/sub channel (in addition
  to the DB write, which remains the source of truth).
- The API exposes `GET /videos/:id/events` as a Server-Sent Events stream:
  subscribes to Redis, forwards transitions, supports `Last-Event-ID` by
  replaying missed transitions from `video_events` on reconnect.
- The web status page consumes SSE and falls back to polling if the stream
  errors. This exercises connection lifecycle, fan-out to many subscribers,
  and recovery when the API restarts mid-stream.

### Done when

- Anonymous fetch of any segment fails; a signed URL works until expiry and
  401s after.
- Repeat playback of the same video shows nginx cache hits, not MinIO reads.
- A rate-limited client gets 429s while others stream unaffected.
- The status page updates with no polling; restarting the API mid-stream
  reconnects and replays missed events.

## Milestone 9: Observability

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
  and consumer counts (RabbitMQ prometheus plugin); cache hit ratio (nginx);
  SSE subscriber count.
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

## Milestone 10: Proof — SLOs, Load, Chaos, DR

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
- **Disaster recovery drill**: Postgres WAL archiving with point-in-time
  recovery; a scripted "restore the database to 5 minutes ago" drill, run for
  real, with its own postmortem. Backup stories are claimed everywhere and
  tested almost nowhere — test it.
- **E2E smoke script** that runs the full upload → ready → playback path
  against a fresh compose stack.
- **Docs**: architecture diagram of the final system and short ADRs in
  `docs/adr/` for the major decisions (outbox, leases, fan-out, manifest
  signing, SSE).

### Done when

- Every chaos scenario converges with zero stuck videos and has a postmortem.
- The PITR restore drill has been run successfully and documented.
- Load test results vs SLOs are committed.
- A newcomer can understand the system from the diagram and ADRs alone.

---

## Milestone 11: Analytics and Player Intelligence (Phase 3)

The classic write-heavy design problem, plus the player features that make the
product feel real.

### Design — watch-time pipeline

- The player emits heartbeat events every ~10s while playing (video id,
  session id, position, selected quality, buffering state), with
  `navigator.sendBeacon` on page unload.
- `POST /analytics/events` accepts small batches, validates, and appends to a
  Redis Stream — the ingest path does no aggregation and stays fast.
- An aggregator service (or goroutine) consumes the stream via a consumer
  group and maintains:
  - total watch time per video (flushed to Postgres)
  - audience retention curve (watch counts per 10s bucket of the video)
  - concurrent viewers per video (gauge with TTL)
  - unique views via Redis HyperLogLog (`PFADD`/`PFCOUNT`)
- New analytics tables (migration), and a per-video analytics dashboard page
  in the web app: watch time, retention curve chart, live concurrent viewers.
- This is the place to discuss (in the ADR) why the ingest path is a stream
  and not direct DB writes, delivery semantics, and what Kafka would change.

### Design — storyboards (seek preview)

- During transcode, the worker also generates a storyboard: a sprite sheet of
  small frames (e.g. one per 2s) plus a WebVTT file mapping time ranges to
  sprite coordinates, uploaded under `processed-videos/{videoId}/storyboard/`.
- The player shows the frame preview when hovering/scrubbing the seek bar —
  the YouTube interaction, built from your own pipeline.

### Done when

- Watching a video produces a retention curve and watch-time numbers that
  survive an aggregator restart (consumer group resumes, no double counting
  beyond at-least-once tolerance).
- Concurrent-viewer count is live during playback and decays when tabs close.
- Scrubbing the seek bar shows frame previews.

## Milestone 12: Auth, Quotas, and Fair Scheduling (Phase 3)

Multi-tenancy turns every earlier system into a harder version of itself.

### Design

- Activate the dormant `users` table: register/login with hashed passwords
  (bcrypt/argon2), JWT access tokens, ownership column enforced on uploads and
  mutating endpoints. Public playback stays public; "my videos" is scoped.
- **Quotas**: per-user total storage bytes and uploads/day, enforced at upload
  session creation (declared size) and completion (actual size).
- **Rate limiting**: per-user token buckets in Redis on the API surface
  (extending the M8 per-IP playback limiter).
- **Fair scheduling** — the hardcore part: one user uploading 50 videos must
  not starve everyone else. Replace naive FIFO dispatch with per-tenant
  queues and a round-robin (optionally weighted) dispatcher that feeds
  `video.transcode`, so each active tenant gets a fair share of worker
  capacity. Document the alternatives considered (RabbitMQ priorities,
  multiple queues, dispatcher service) in an ADR.
- Web: register/login UI, session handling, "my videos" page, quota usage
  display.

### Done when

- A drill proves fairness: tenant A enqueues 50 videos, tenant B enqueues 1,
  and B's video completes in roughly single-tenant time, not behind all 50.
- Quota and rate-limit violations fail with clear, tested error responses.
- All mutating endpoints require auth; ownership is enforced and tested.

---

## Milestone 13: Video Understanding Layer (Phase 3)

Turn the transcode fan-out into an *enrichment* fan-out: every upload also
becomes searchable, chaptered, and captioned. The point is twofold — it makes
the product feel like 2025 instead of 2010, and it proves the M7 job system is
**general** (ML work is just more job types, not a special case). Depends on M7
(fan-out) and reuses M11's player surface.

### Design — enrichment as fan-out jobs

- Extend the M7 job hierarchy with two new `job_type`s alongside `rendition`:
  `transcript` and `embedding`. The **planner** fans these out via the outbox at
  the same time as renditions.
- **Non-blocking by design**: a video still reaches `ready` when *transcoding*
  finishes — enrichment runs in parallel and populates search/chapters/captions
  when done. A separate `enrichment_status` (`pending → indexing → indexed`)
  tracks it. **ML failure degrades, never blocks**: a failed transcript means no
  captions for that video, not a `failed` video. (Engineering rule: no video is
  ever stuck — enrichment is off the critical path.)
- **Transcript job**: extract audio, run Whisper (whisper.cpp locally, or a
  hosted endpoint). Output timestamped segments → a WebVTT file under
  `processed-videos/{videoId}/subtitles/` (wired as an HLS `EXT-X-MEDIA` subtitle
  track — subsumes the optional "subtitles via Whisper" extension) plus rows in
  `video_transcripts(video_id, start_s, end_s, text)`.
- **Auto-chapters**: feed the transcript to an LLM (Claude) to segment it into
  titled chapters; store in `video_chapters(video_id, start_s, title)`. The
  player renders chapter markers on the seek bar (alongside M11 storyboards).
- **Embedding job**: sample frames (~1 per 2s, drop near-duplicates), embed them
  (CLIP or a hosted multimodal model); also embed each transcript segment. Store
  vectors in **pgvector** — keeps everything in Postgres, no new datastore — in
  `video_embeddings(video_id, kind, ref_s, embedding)`.
- **Auto-thumbnail upgrade**: pick the most representative frame by embedding
  centrality instead of the fixed frame-at-1s.
- **Semantic search**: `GET /search?q=...` embeds the query, runs vector
  similarity over transcript + frame embeddings, returns ranked hits as
  `{videoId, timestamp, snippet}`. Web: a search box; a result deep-links to
  `/watch/:id?t=SECONDS` and seeks to the moment.
- New migration `000005_*` (or per build order): the four tables above + the
  `pgvector` extension + enrichment job rows. Env: model endpoints/keys.
- ADR: ML-as-a-job-type; pgvector vs a dedicated vector DB; why enrichment is
  non-blocking and degradation-tolerant.

### Done when

- Upload a video → shortly after `ready` it has captions, titled chapters, and
  is searchable, with a representative auto-thumbnail.
- A natural-language query (`"the part about the outage"`) jumps to the exact
  second, across the whole library.
- Killing the embedding model mid-run leaves the video playable with degraded
  features — never stuck, never `failed` for an ML reason.

## Milestone 14: Mission Control — Live Fleet Dashboard + Chaos Controls (Showcase)

The demo centerpiece. Make the distributed system **visible and pokeable**: a
live view of every worker and what it's processing, plus a button to *kill nodes
from the UI* and watch the system recover without losing a single video — the
"go ahead, kill it, I don't care" moment. This milestone mostly **surfaces infra
you already built** (M5 leases/reaper, M7 fan-out, M8 SSE, M9 heartbeats/metrics)
— which is exactly why it's a credible showcase: the resilience is real, not
staged. Build it after those dependencies land.

### Design — fleet state

- Each worker registers and heartbeats its live state (extends the M5 lease
  heartbeat and M9 metrics): `workers(worker_id, host, pid, status, current_job,
  current_video, stage, progress_pct, started_at, last_heartbeat_at)`, mirrored
  into a Redis hash for fast reads. `status` ∈ `idle | processing | draining`;
  `stage` ∈ `download | probe | transcode | upload | finalize`. The M5 reaper
  already declares a worker dead when its heartbeat/lease goes stale.
- **Fleet API**: `GET /admin/fleet` (live workers + per-worker current work +
  queue depth + in-flight count + time-to-ready stats) and
  `GET /admin/fleet/events` (SSE stream of fleet changes — reuses M8's SSE
  machinery and `video_events`/Redis pub-sub).

### Design — the chaos control plane (the kill switch)

- `POST /admin/workers/:id/{pause|resume}` → control message on a Redis channel
  the workers subscribe to (graceful drain — safe).
- `POST /admin/workers/:id/kill` → a **hard** `kill -9`-equivalent for the demo:
  a thin supervisor invokes the container runtime (`docker kill` via the Docker
  socket locally; the pod-delete API under the k8s capstone) to terminate that
  worker outright. This is the honest version — no in-process "pretend crash".
- **Gated**: the entire chaos plane requires `CHAOS_MODE=true` **and** admin auth
  (M12). It is demo-only and must be impossible to trigger in a real deployment.
- Optional flourish: **chaos roulette** — a toggle that kills a random worker
  every N seconds for a hands-free resilience loop.

### Design — the visualization (web `/mission-control`)

- A grid of **worker cards**: each shows the worker, its current video + stage,
  a live progress bar, pulsing while active; a red **KILL** button per card.
- Live **queue-depth gauge**, in-flight count, and a rolling time-to-ready stat.
- A **per-video pipeline view**: the M7 fan-out renditions as parallel lanes
  filling up, so you can see one video's work spread across the fleet.
- The payoff interaction: click **KILL** on a worker mid-transcode → its card
  drops out → the UI then *animates the recovery* — lease expires, reaper
  requeues the orphaned rendition, a surviving worker picks it up, the video
  still reaches `ready`. A scoreboard tallies "workers killed" vs "videos
  completed: 100%".
- ADR: control-plane safety (`CHAOS_MODE` gating + authz), how hard-kill is
  implemented per environment (Docker socket / k8s API), and DB-vs-Redis for
  fleet state.

### Done when

- The dashboard shows every live worker, its stage, and live progress, updating
  over SSE with no polling.
- Clicking KILL hard-kills a worker mid-job; the UI shows lease expiry → reaper
  requeue → resume on another worker → `ready`, with **zero stuck videos**.
- Chaos roulette can run for several minutes while 100% of in-flight videos
  still complete — the resilience claim, demonstrated live.

---

## Milestone 15: Content-Aware Encoding + Quality/Cost Proof (Showcase)

Stop shipping a fixed bitrate ladder. Analyze each source, encode the *optimal*
ladder for that content, and **prove it with numbers** — VMAF perceptual quality
and a real cost meter. This is the "we understand the economics of video, not
just how to play it" milestone, and the one that signals depth to anyone who has
operated video at scale. Builds on M7 (the planner already decides the ladder).

### Design

- **Complexity analysis** in the planner stage: estimate how hard the source is
  to compress (a fast constant-quality probe encode, or spatial/temporal energy
  from ffprobe/`signalstats`). Talking-head footage is cheap; high-motion sport
  is expensive — and the ladder should reflect that.
- **Per-title bitrate ladder**: derive the rendition set and target bitrates from
  complexity instead of the fixed 360/480/720 @ fixed bitrates (still capped by
  source resolution). Simple content → fewer/leaner rungs; complex content →
  richer ladder. Reference: Netflix per-title / per-shot encoding and the
  convex-hull bitrate–resolution selection — document the rigorous version,
  implement a pragmatic one.
- **VMAF scoring**: after each rendition, compute VMAF (an ffmpeg built with
  `libvmaf`) of the rendition against the source. Store per-rendition VMAF and
  measured bitrate on `video_variants` (new columns) — the quality-vs-bitrate
  datapoint.
- **Cost meter**: track transcode CPU-seconds per video and per rendition, times
  a configurable `COST_PER_CPU_HOUR`; store and expose. Dashboard tiles: `$ / 1000
  videos`, `bytes / minute of video`, and a CAE-vs-fixed-ladder comparison.
- **The proof**: a measurement (under `tests/encoding/`) that runs the fixed
  ladder vs CAE over the same corpus and reports bytes saved at equal VMAF and
  the resulting cost delta. That single number is the headline.
- Migration: VMAF + bitrate + cost columns on variants (and/or a
  `video_encode_stats` table). ADR: per-title approach chosen, VMAF as the
  quality gate, and the cost-model assumptions.

### Done when

- For a representative corpus, CAE ships measurably fewer bytes than the fixed
  ladder at equal-or-better VMAF, with the savings and cost delta committed
  (e.g. "~35% fewer bytes at VMAF ≥ 93, ~Y% cheaper per 1000 videos").
- Every rendition has a stored VMAF score and bitrate; the dashboard renders the
  quality-vs-bitrate curve.
- A high-motion source gets a richer ladder and a simple one gets a leaner
  ladder — the decision is content-driven and visible, not hardcoded.

## Milestone 16: Auto-Trailer / Highlight Reel (Showcase)

Turn the understanding layer into a *creative output*: auto-generate a short,
shareable trailer from any upload. The most screenshot-able feature on the
roadmap, and nearly free once M13 exists — it's another non-blocking enrichment
job. Depends on M13 (transcript + embeddings).

### Design

- New `highlight` enrichment `job_type` (M7/M13 fan-out), non-blocking like the
  rest of M13 — trailer generation never gates `ready`.
- **Moment scoring** from signals you already produce: an LLM (Claude) picks the
  most informative/quotable transcript lines with timestamps; audio energy peaks;
  scene-change density; frame-embedding salience and visual diversity. Combine
  into a score per time window.
- **Selection + assembly**: choose the top K non-overlapping windows totaling
  ~20–30s, kept in chronological order; FFmpeg trims and concatenates them into a
  trailer, which is then run through the **same HLS pipeline** (a trailer is just
  another short video — this proves the pipeline composes). Output under
  `processed-videos/{videoId}/trailer/`.
- **Hover previews**: also emit an animated preview (GIF/WebP) and reuse the M13
  representative thumbnail; the web grid plays the trailer/preview on hover
  (the YouTube/Netflix interaction), with a "trailer" badge and a share button.
- ADR: heuristic vs LLM-driven moment selection; non-blocking enrichment; reusing
  the transcode pipeline for a generated artifact.

### Done when

- Upload a multi-minute video → shortly after `ready`, a ~30s auto-trailer exists
  and plays on hover in the grid.
- The trailer spans visually and topically distinct moments (driven by transcript
  + embedding signals), not just the first 30 seconds.
- Trailer generation failing degrades gracefully (no trailer) and never blocks
  `ready` or wedges the pipeline.

---

## Phase 4: Capstone Options

Commit to these only after M10 proves the foundation. Each is a project-sized
effort; pick by interest.

- **Live streaming** — RTMP ingest (OBS → nginx-rtmp or MediaMTX), near-real-
  time HLS packaging, live-to-VOD archiving through the existing pipeline.
  The most iconic capstone; touches latency budgets, sliding-window playlists,
  and stream key auth.
- **Kubernetes + real cloud** — kind/k3s locally with Helm charts, KEDA
  autoscaling workers on queue depth (the production version of the M7
  experiment), then Terraform to real S3 + CloudFront. Turns "ran locally"
  into "deployed".

## Optional Extensions (pick by interest, any time after M10)

- **Subtitles via Whisper** — now folded into M13 (the transcript job produces
  the WebVTT subtitle track); listed here only for historical context.
- **HLS AES-128 encryption** — segment encryption with an auth-gated key
  endpoint (`EXT-X-KEY`); natural extension of M8's signing work.
- **ABR visualizer** (player extension, after M8) — a network-throttle slider on
  the watch page with a live bandwidth/buffer graph, showing hls.js switching
  renditions in real time. Makes adaptive streaming visible and pokeable for
  almost no effort.
- **Clip-it sharing** (player extension, after M8) — drag-select a time range and
  get a shareable URL that plays just that span via a generated sub-playlist over
  existing segments (no re-encode), with its own thumbnail. The Twitch/YouTube
  "clip" feature, built from your own primitives and M8's signing.

Deliberately out of scope: recommendations/ML, microservice splitting for its
own sake, GraphQL, comments/likes (CRUD, not system design).

---

## Build Order

Strictly in milestone order — CI first, correctness before scale, scale before
polish, proof before product features:

1. M4: GitHub Actions → testcontainers harness → integration upload-flow test.
2. M5: outbox → leases/reaper → retries/DLQ → idempotency → shutdown → failure drills.
3. M6: sessions table → API endpoints → web uploader → remove proxy path.
4. M7: job hierarchy → planner → rendition workers → aggregation → multi-worker runs.
5. M8: private buckets → manifest endpoints → signing → nginx → Redis → SSE.
6. M9: tracing → metrics → compose services → dashboards.
7. M10: SLOs → load tests → chaos suite → DR drill → postmortems → final docs.
8. M11: heartbeat ingest → stream aggregator → analytics UI → storyboards.
9. M12: auth → quotas → rate limits → fair scheduling drill.
10. M13: enrichment job types → transcript/embedding workers → pgvector → search UI (after M7; uses M11 player).
11. M14: worker heartbeat state → fleet API + SSE → chaos control plane → Mission Control UI (after M5/M7/M8/M9).
12. M15: complexity analysis → per-title ladder → VMAF scoring → cost meter → fixed-vs-CAE measurement (after M7).
13. M16: highlight-scoring job → trailer assembly through the HLS pipeline → hover previews (after M13).

Each milestone lands with tests, a `PROGRESS.md` update, an ADR when it makes
an architectural decision, and migrations under `infrastructure/migrations/`
(numbered `000002_...` onward). Write the milestone's docs/ADR while building
it, not after — the writeups are half the portfolio value.

## New Environment Variables (introduced per milestone)

```txt
M5:  OUTBOX_POLL_INTERVAL, JOB_LEASE_SECONDS, JOB_MAX_ATTEMPTS, WORKER_ID
M6:  UPLOAD_SESSION_TTL, UPLOAD_PART_SIZE
M8:  PLAYBACK_HMAC_SECRET, EDGE_BASE_URL, REDIS_ADDR (finally used)
M9:  OTEL_EXPORTER_OTLP_ENDPOINT, METRICS_ADDR
M11: ANALYTICS_STREAM_NAME, ANALYTICS_FLUSH_INTERVAL
M12: JWT_SECRET, ACCESS_TOKEN_TTL
M13: WHISPER_ENDPOINT, EMBEDDING_ENDPOINT, ANTHROPIC_API_KEY (auto-chapters), PGVECTOR enabled
M14: CHAOS_MODE, DOCKER_HOST (supervisor access to the container runtime)
M15: COST_PER_CPU_HOUR (cost meter); requires an ffmpeg built with libvmaf
M16: TRAILER_TARGET_SECONDS
```

Keep `.env.example` files current when these land.

## Key Engineering Rules

- Do not transcode inside any HTTP request path.
- Every queue consumer must be idempotent; delivery is at-least-once everywhere.
- No video may ever be stuck: every state must have a path to `ready` or `failed` driven by a timeout, retry, or reaper.
- All DB-then-publish sequences go through the outbox; never dual-write.
- Deterministic object keys so retries overwrite rather than duplicate.
- The DB is the source of truth; Redis pub/sub, streams, and caches are derived and rebuildable.
- Each milestone is proven by a failure drill or measurement, not just tests passing.
- CI must stay green; integration tests run against real dependencies, not fakes.
- Keep contracts (queue payloads, object key layout, API JSON) in sync across `apps/api`, `apps/worker`, and `apps/web/lib/api.ts`.

## Resolved Decisions

- CI: GitHub Actions with testcontainers-go integration tests against real Postgres/RabbitMQ/MinIO.
- Playback strategy: API manifest rewriting + HMAC-signed segment URLs behind an nginx edge cache (was an open question).
- Realtime status: SSE backed by Redis pub/sub, with `video_events` replay on reconnect; polling remains the fallback.
- Analytics ingest: Redis Streams with consumer groups (Kafka documented as the scale-up path, not adopted locally).
- SQL access: stay on `database/sql` + pgx; no ORM.
- Publish strategy: transactional outbox, at-least-once delivery.
- Retry transport: RabbitMQ TTL retry queue + dead-letter exchange.

## Open Decisions

- Segment-level parallel transcoding (M7 stretch): worth the stitching complexity locally, or document the design and skip?
- CMAF/fMP4 segments instead of MPEG-TS: revisit if/when LL-HLS or the live-streaming capstone becomes interesting.
- Phase 4 pick: live streaming vs Kubernetes/cloud first (or both, in which order).
- Fair-scheduling mechanism detail (M12): dispatcher service vs RabbitMQ priority queues — decide with an ADR when starting M12.
