# MediaFlow Progress Tracker

Last updated: 2026-06-21

## Overall Status

Status: Phase 1 (MVP, Milestones 0–3) complete. Phase 2 (Milestones 4–10) in progress — Milestones 4, 5, and 6 done; Milestone 7 in progress (slice A done). Phase 3 (Milestones 11–12) and Phase 4 capstones follow.

Current focus:

```txt
Milestone 6: Scalable Ingest — COMPLETE (ADRs 0007–0010)
Milestone 7: Distributed Transcoding — IN PROGRESS
  slice A (fan-out engine) done (ADR 0011): the monolithic transcode is replaced
    by plan -> N renditions -> finalize. Planner (video.transcode) probes +
    thumbnails + fans out one rendition job per quality via the outbox; rendition
    workers (video.rendition) each transcode one quality and atomically decrement
    the plan's pending counter (UPDATE ... RETURNING); whoever hits zero enqueues
    finalize (video.finalize), which assembles master.m3u8 and marks the video
    ready. Migration 000006 adds parent_job_id / pending_renditions /
    rendition_spec. Reaper is job-type-aware (requeues to the right queue,
    rebuilding rendition msgs from rendition_spec). Idempotent at every stage;
    aggregation race-safe via the plan row's update lock. Proven by
    TestFanOutProducesAllRenditions (720p -> 3 renditions -> ready, 3-stream
    master). Child-stage failures recover via the reaper (per-stage backoff is
    slice B).
  next: slice B (per-rendition retry/DLQ, partial-failure cleanup, aggregation-
    race + kill-drill proofs), then slice C (3-worker speedup measurement).
```

See `MEDIAFLOW_PLAN.md` for the design behind each milestone.

## Milestones

| Milestone | Status | Notes |
| --- | --- | --- |
| 0. Repo and Infra | Done | Scaffold, Compose file, env examples, migration, README, and live dependency startup verified. |
| 1. API Upload Path | Done | Upload path, DB writes, MinIO storage, RabbitMQ publishing, list/detail/playback endpoints, migration command, and API tests are working. |
| 2. Worker Transcoding Path | Done | Worker consumes jobs, runs FFmpeg/ffprobe, creates thumbnail and HLS variants, uploads outputs, and marks videos ready. |
| 3. Web Playback Path | Done | Next.js app supports upload, video list, status polling, HLS watch page, manual quality selection, and local smoke checks. |
| 4. CI and Integration Test Harness | Done | GitHub Actions + testcontainers-go integration tests (Postgres/RabbitMQ/MinIO, full upload→ready flow). CI green on PR #1 and required via a ruleset on a public, protected `main`. ADR: `docs/adr/0001-ci-and-integration-harness.md`. |
| 5. Correctness Under Failure | Done | Slice A (transactional outbox) done: video+job+outbox in one tx, no direct publish, relay loop with confirms (ADR `0002`). Slice B (leases+reaper) done: claims carry `claimed_by`+`lease_expires_at`, workers heartbeat while FFmpeg runs, reaper requeues expired leases via the outbox below `JOB_MAX_ATTEMPTS` / fails them at max (ADR `0003`). Slice C (retries/DLQ) done: transient failures publish to `video.transcode.retry` (per-message TTL `base·2^attempts`, DLX back to main), poison/exhausted/permanent → `video.transcode.dlq`, permanent failures (corrupt input / no video stream) skip retries; publish-first ordering keeps the reaper as backstop (ADR `0004`). Slice D (idempotency+shutdown) done: `Idempotency-Key` on upload (partial unique index, replay returns original with 200, race recovers via 23505), graceful SIGTERM (drain in-flight job within `WORKER_SHUTDOWN_GRACE`, reaper covers overruns), retry hygiene verified (skip-if-ready, variant clear, deterministic keys) (ADR `docs/adr/0005-idempotency-and-graceful-shutdown.md`). RabbitMQ-down drill done: upload stays 201 with the broker down (request path is broker-independent); the drill exposed a relay that held one connection for life and never recovered, fixed with a lazy-reconnecting publisher so the outbox drains automatically on broker return — no API restart (ADR `docs/adr/0006-relay-broker-reconnect.md`). |
| 6. Scalable Ingest | Done | Slice A (sessions + presigned parts) done: `upload_sessions` table (migration `000005`, status enum `pending\|uploading\|completed\|aborted\|expired`), control-plane endpoints (`POST /uploads`, `GET /uploads/:id`, `GET /uploads/:id/parts/:n/url`, `DELETE /uploads/:id`), MinIO multipart via `minio.Core`, size/part validation at creation, resume by listing parts from object storage (ADR `0007`). Slice B (completion+validation+enqueue) done: `POST /uploads/:id/complete` validates declared ETags + assembled size against what object storage holds (tampered/short/oversize → clean `422`), finalizes multipart with stored ETags, then creates video+job+events+outbox in one tx (reuses M5) and links the session; completion is idempotent (replay returns the same video, concurrent completions race-safe via a guarded UPDATE). Live drill: a 7MiB MP4 uploaded as two presigned parts straight to MinIO (PUTs hit `:9000`, never the API on `:8080`), completed, and the worker drove it to `ready` with 2 variants; outbox drained 1/1 (ADR `docs/adr/0008-upload-completion-validation.md`). Slice C (expiry sweep) done: a background sweeper (mirrors the outbox relay; `UPLOAD_SWEEP_INTERVAL` default 5m) expires `pending`/`uploading` sessions past `expires_at` via a guarded claim-first `UPDATE` (race-safe against a concurrent completion) and aborts their orphaned MinIO multipart uploads; abort failures are logged and leave the row `expired` (MinIO lifecycle is the backstop). No schema change — the `expired` enum + `expires_at` index shipped in `000005`. Integration `TestUploadSweepExpiresAbandonedSession` proves against real MinIO that the staged part is gone after the sweep (ADR `docs/adr/0009-upload-session-expiry-sweep.md`). Slice D (web uploader + retire legacy proxy) done: browser chunked uploader (`lib/uploads.ts`) slices the file and PUTs parts directly to MinIO with 4-lane bounded concurrency, per-part presigned URLs, 4-attempt backoff retry, and a shared `AbortController` for cancel; real byte-level progress via XHR upload events; resume-after-reload keyed on a `localStorage` file-identity record (re-select the same file → only the missing parts upload). Legacy `POST /videos/upload` is now gated behind `ENABLE_LEGACY_UPLOAD` (default off; handler kept for M10 proxy-vs-direct benchmarks) (ADR `docs/adr/0010-web-chunked-uploader.md`). |
| 7. Distributed Transcoding | In progress | Slice A (fan-out engine) done: plan→rendition→finalize across `video.transcode`/`video.rendition`/`video.finalize`; migration `000006` (`parent_job_id`/`pending_renditions`/`rendition_spec`); atomic `UPDATE ... RETURNING` aggregation counter; job-type-aware reaper; idempotent + race-safe; child-stage failures recover via the reaper (ADR `0011`). Remaining: per-rendition retry/DLQ + partial-failure cleanup (slice B), 3-worker speedup measurement (slice C). |
| 8. Serving At Scale | Not started | Private buckets, manifest rewriting with HMAC-signed segment URLs, nginx edge cache, Redis, SSE status push. |
| 9. Observability | Not started | OpenTelemetry traces across the queue, Prometheus metrics, Jaeger + Grafana dashboards. |
| 10. Proof: SLOs, Load, Chaos, DR | Not started | Stated SLOs, k6 load tests, scripted chaos scenarios, PITR restore drill, postmortems, ADRs. |
| 11. Analytics and Player Intelligence | Not started | Watch-time heartbeat pipeline via Redis Streams, retention curves, HLL unique views, storyboard seek previews. |
| 12. Auth, Quotas, Fair Scheduling | Not started | JWT auth, per-user quotas and rate limits, per-tenant fair dispatch. |
| 13. Video Understanding Layer | Not started | Enrichment fan-out: Whisper transcripts/captions, LLM auto-chapters, frame+text embeddings in pgvector, semantic search. Non-blocking; depends on M7. |
| 14. Mission Control (Showcase) | Not started | Live worker-fleet dashboard over SSE + chaos kill controls (kill nodes from the UI, watch recovery). Surfaces M5/M7/M8/M9. |
| 15. Content-Aware Encoding (Showcase) | Not started | Per-title bitrate ladder from complexity analysis, VMAF scoring, cost meter; proves fewer bytes at equal quality. Depends on M7. |
| 16. Auto-Trailer (Showcase) | Not started | Non-blocking `highlight` enrichment job: score moments, assemble a ~30s trailer through the HLS pipeline, hover previews. Depends on M13. |
| Phase 4 Capstone | Not started | Live streaming and/or Kubernetes + cloud — pick after Milestone 10. |

## Detailed Checklist

### Milestone 0: Repo and Infra (Done)

- [x] Create `apps/api`
- [x] Create `apps/worker`
- [x] Create `apps/web`
- [x] Create `packages/shared`
- [x] Create `infrastructure/migrations`
- [x] Add Docker Compose for PostgreSQL
- [x] Add Docker Compose for RabbitMQ
- [x] Add Docker Compose for Redis
- [x] Add Docker Compose for MinIO
- [x] Add MinIO bucket setup for `mediaflow-raw`
- [x] Add MinIO bucket setup for `mediaflow-processed`
- [x] Add MinIO bucket setup for `mediaflow-thumbnails`
- [x] Add initial database migration
- [x] Add local environment examples
- [x] Add root README with startup instructions
- [x] Verify Docker Compose dependencies start locally

### Milestone 1: API Upload Path (Done)

- [x] Initialize Go API app
- [x] Add health endpoint
- [x] Add database connection
- [x] Add migration runner or migration command
- [x] Add MinIO client
- [x] Add RabbitMQ publisher
- [x] Add upload request validation
- [x] Store original MP4 in MinIO
- [x] Create `videos` row
- [x] Create `video_jobs` row
- [x] Publish `video.transcode` job
- [x] Add `GET /videos`
- [x] Add `GET /videos/:id`
- [x] Add `GET /videos/:id/playback`
- [x] Add API tests

### Milestone 2: Worker Transcoding Path (Done)

- [x] Initialize worker app
- [x] Add RabbitMQ consumer
- [x] Add database connection
- [x] Add MinIO client
- [x] Claim queued job safely
- [x] Update video status to `processing`
- [x] Download raw video to temp directory
- [x] Run `ffprobe`
- [x] Save duration and metadata
- [x] Generate thumbnail with FFmpeg
- [x] Generate HLS master manifest
- [x] Generate 720p variant
- [x] Generate 480p variant
- [x] Generate 360p variant if source allows
- [x] Upload HLS output to MinIO
- [x] Upload thumbnail to MinIO
- [x] Insert `video_variants`
- [x] Update video status to `ready`
- [x] Mark job `completed`
- [x] Handle failures and update status to `failed`
- [x] Add worker tests

### Milestone 3: Web Playback Path (Done)

- [x] Initialize Next.js app
- [x] Add API client
- [x] Build upload page
- [x] Add upload validation UI
- [x] Build video list page
- [x] Build processing status page
- [x] Add status polling
- [x] Build watch page
- [x] Integrate `hls.js`
- [x] Show playback errors clearly
- [x] Add frontend tests or smoke checks

### Milestone 4: CI and Integration Test Harness

- [x] GitHub Actions workflow: API `gofmt` check, `go vet`, `go test ./...`
- [x] GitHub Actions workflow: worker tests with ffmpeg/ffprobe installed in runner
- [x] GitHub Actions workflow: web `npm run lint` and `npm run build`
- [x] Go module and npm dependency caching in CI (`setup-go`/`setup-node` cache)
- [x] Integration test suite behind `integration` build tag using testcontainers-go
- [x] Integration: repository tests against real Postgres with migrations applied
- [x] Integration: publish/consume round-trip against real RabbitMQ
- [x] Integration: upload → store → queue → process flow with generated fixture MP4
- [x] Fixture MP4 generated via ffmpeg in tests (never committed)
- [x] Local command mirrors CI (`go test -tags integration ./...`)
- [ ] CI required for PRs; main branch green (needs first push + branch-protection rule on GitHub)

### Milestone 5: Correctness Under Failure

- [x] Add migration `000002`: `outbox_messages` table
- [x] Add migration `000003`: `video_jobs.claimed_by` and `video_jobs.lease_expires_at` (slice B)
- [x] Write video + job + outbox row in one DB transaction in `Upload`
- [x] Remove direct RabbitMQ publish from the upload request path
- [x] Add outbox relay loop in API (`FOR UPDATE SKIP LOCKED`, publisher confirms)
- [x] Increment `video_jobs.attempts` on every claim
- [x] Add worker heartbeat that extends the lease during processing
- [x] Add reaper: expired lease below max attempts → requeue via outbox
- [x] Add reaper: expired lease at max attempts → mark job and video `failed`
- [x] Declare `video.transcode.retry` queue with per-message TTL and DLX back to `video.transcode`
- [x] Declare `video.transcode.dlq` and route exhausted/poison messages there
- [x] Classify permanent vs transient failures (no retry for corrupt input)
- [x] Add `Idempotency-Key` support on upload
- [x] Make worker retries overwrite-safe (clear stale variants, deterministic keys)
- [x] Add graceful worker shutdown (finish in-flight job on SIGTERM)
- [x] Write `video_events` row on every status transition
- [x] Tests (incl. integration): outbox relay, claim/lease, retry routing, reaper, idempotency key
- [x] Failure drill: `kill -9` worker mid-job → reaper recovers → video `ready` (2026-06-13: two-worker run, killed the claimer mid-download with the lease held; the survivor's reaper logged `requeued=1` ~16s later, reclaimed as attempt 2, drove the video to `ready`; `video.job.requeued` event recorded and the requeue flowed through the outbox)
- [x] Failure drill: RabbitMQ down during upload → outbox drains after restart (2026-06-17: with the broker stopped, `POST /videos/upload` still returned 201 and the outbox row persisted; the drill exposed a publisher that dialed once at startup and never reconnected — the relay looped on a dead channel even after the broker came back. Fixed with a lazy-reconnecting publisher (`ensureChannel` redials on a closed conn/channel); re-ran the drill and the outbox drained on the next tick with no API restart. Regression guard: integration `TestRelayReconnectsAfterBrokerDrop`. ADR `docs/adr/0006-relay-broker-reconnect.md`)
- [x] Failure drill: poison message lands in DLQ without wedging the consumer (integration `TestPoisonMessageDeadLettered`: unparseable body → DLQ with `x-failure-reason`, then a valid job still reaches `ready` — real RabbitMQ via testcontainers)

### Milestone 6: Scalable Ingest

- [x] Add migration `000005`: `upload_sessions` table (plan said `000003`; renumbered — M5 added `000002`–`000004`)
- [x] `POST /uploads`: create session, initiate MinIO multipart upload
- [x] `GET /uploads/:id/parts/:n/url`: issue presigned part URL
- [x] `GET /uploads/:id`: report session status and uploaded parts for resume
- [x] `POST /uploads/:id/complete`: complete multipart, validate size and checksums, enqueue via outbox
- [x] `DELETE /uploads/:id`: abort multipart upload
- [x] Cleanup loop: abort expired sessions and orphaned multipart uploads (sweeper in `internal/uploads`, wired in `cmd/api`; `UPLOAD_SWEEP_INTERVAL`/`UPLOAD_SWEEP_BATCH_SIZE`; ADR `0009`)
- [x] Enforce size limits at session creation and at completion
- [x] Web: chunked uploader with bounded parallelism and per-part retry (`lib/uploads.ts`: 4 lanes draining a shared queue, presigned PUT per part via XHR for progress, 4-attempt backoff+jitter retry, shared `AbortController` cancel; ADR `0010`)
- [x] Web: resume upload after page reload (persist session id) (identity record in `localStorage`; re-select the same file → `GET /uploads/:id` returns stored parts, only missing parts upload)
- [x] Web: real upload progress UI (byte-level progress bar + parts counter, Cancel/Discard)
- [x] Remove (or flag-gate) legacy `POST /videos/upload` proxy endpoint (gated behind `ENABLE_LEGACY_UPLOAD`, default off; route unregistered by default, handler kept for M10 benchmarks)
- [x] Tests: session lifecycle, resume, checksum mismatch, oversize rejection
- [x] Verify: 500MB upload never transits the API process (live drill 2026-06-17: 7MiB MP4 uploaded as two presigned parts directly to MinIO — PUTs hit `:9000`, not the API on `:8080` — completed via the API, worker drove it to `ready`; the path is size-independent so it holds for 500MB)
- [x] Verify: browser CORS contract for direct-to-MinIO PUT (live drill 2026-06-19: OPTIONS preflight from `Origin: http://localhost:3000` returns `Access-Control-Allow-Methods: PUT`; the actual PUT exposes `Etag` via `Access-Control-Expose-Headers` so `xhr.getResponseHeader("ETag")` works; `GET /uploads/:id` then surfaced the staged part and completion enqueued the video `201`). Full UI click-through (tab-kill + reload resume) still to be exercised in a browser.

### Milestone 7: Distributed Transcoding

- [x] Add migration `000006` (plan said `000004`; renumbered — M5/M6 took `000002`–`000005`): `video_jobs.parent_job_id`, `pending_renditions`, `rendition_spec`; job types `plan | rendition | finalize` (free-text `job_type`, no DDL)
- [x] Split worker into planner, rendition, and finalize consumers (one process consumes all three queues)
- [x] Planner: probe, thumbnail, plan renditions, fan out `video.rendition` jobs via the outbox (one tx)
- [x] Rendition worker: transcode exactly one quality, upload its playlist and segments
- [x] Apply M5 leases per rendition (claim + heartbeat). Per-rendition *retry* (one rendition retries without redoing others) is slice B; slice A recovers child failures via the reaper
- [x] Atomic completion counter (`UPDATE ... RETURNING`); last rendition triggers finalize via the outbox
- [x] Finalizer: write `master.m3u8` from recorded variants, mark video `ready` (variants are inserted per-rendition on completion, upsert-safe)
- [ ] Partial failure: exhausted rendition fails the video and cleans up siblings (basic fail-via-reaper works; explicit sibling cleanup is slice B)
- [ ] Run and document `docker compose up --scale worker=3` (slice C)
- [ ] Measure parallel speedup: 3 workers vs 1 on the same source video (slice C)
- [ ] Autoscaling experiment: scale workers on queue depth, record results (slice C)
- [x] Tests: fan-out + aggregation (`TestFanOutProducesAllRenditions`); aggregation race + partial failure are slice B
- [ ] Stretch: segment-level parallel transcode and playlist stitching

### Milestone 8: Serving At Scale

- [ ] Remove anonymous download policy from processed/thumbnail buckets
- [ ] `GET /videos/:id/hls/master.m3u8`: rewrite variant URIs to API endpoints
- [ ] `GET /videos/:id/hls/:quality/index.m3u8`: rewrite segment URIs to signed edge URLs
- [ ] HMAC segment signing (path + expiry, `PLAYBACK_HMAC_SECRET`)
- [ ] Add nginx edge cache to Docker Compose with `secure_link` validation
- [ ] Cache policy: long-TTL immutable segments, short-TTL playlists
- [ ] Redis: cache rewritten manifests with TTL below token expiry
- [ ] Redis: token-bucket rate limiting on playback endpoints
- [ ] Redis: view counters with periodic flush to Postgres
- [ ] Web player switches to the manifest endpoint (quality selection still works)
- [ ] Workers publish status transitions to Redis pub/sub
- [ ] SSE endpoint `GET /videos/:id/events` with `Last-Event-ID` replay from `video_events`
- [ ] Web status page consumes SSE with polling fallback
- [ ] Verify: anonymous segment fetch fails; expired signature returns 401/403
- [ ] Verify: repeat playback hits nginx cache, not MinIO
- [ ] Verify: API restart mid-stream → SSE reconnects and replays missed events
- [ ] Tests: signing, expiry, manifest rewriting, rate limiting, SSE replay

### Milestone 9: Observability

- [ ] OpenTelemetry tracing middleware in the API
- [ ] Inject trace context into AMQP headers in the outbox relay
- [ ] Extract trace context in consumers; spans per stage (download, probe, thumbnail, transcode, upload, finalize)
- [ ] Add Jaeger to Docker Compose
- [ ] Verify: single trace spans upload → queue → renditions → `ready`
- [ ] Prometheus metrics in API (latency, RPS, error rate)
- [ ] Prometheus metrics in worker (stage durations, success/failure/retry counters)
- [ ] Enable RabbitMQ prometheus plugin (queue depth, consumers)
- [ ] nginx cache hit ratio metrics; SSE subscriber gauge
- [ ] Add Prometheus and Grafana to Docker Compose with provisioned dashboards
- [ ] Pipeline health dashboard (queue depth, in-flight jobs, time-to-ready p50/p95, failure rate)
- [ ] Structured `slog` logging with trace/correlation IDs in both Go apps
- [ ] Store trace id in `video_events.metadata_json`
- [ ] Draft alert rules: queue lag, error rate, jobs stuck in `processing`

### Milestone 10: Proof — SLOs, Load, Chaos, DR

- [ ] Write `docs/SLOS.md` with stated, measurable objectives
- [ ] k6 upload load test (`tests/load/`)
- [ ] k6 playback concurrency test against the edge cache
- [ ] Soak test: sustained uploads, assert zero stuck videos
- [ ] Chaos: `kill -9` rendition worker mid-transcode + postmortem
- [ ] Chaos: RabbitMQ restart under load + postmortem
- [ ] Chaos: MinIO unavailable during transcode + postmortem
- [ ] Chaos: `WORK_DIR` disk full + postmortem
- [ ] Chaos: Postgres restart under load + postmortem
- [ ] DR: Postgres WAL archiving configured
- [ ] DR: scripted point-in-time-recovery restore drill, run for real + postmortem
- [ ] E2E smoke script: fresh compose stack → upload → ready → playback
- [ ] Architecture diagram of the final system
- [ ] ADRs in `docs/adr/` (outbox, leases, fan-out, manifest signing, SSE)
- [ ] Record load test results against SLOs
- [ ] Final README and docs update

### Milestone 11: Analytics and Player Intelligence

- [ ] Add migration `000005`: analytics tables (watch time aggregates, retention buckets)
- [ ] Player heartbeat emitter (~10s interval; position, quality, buffering; `sendBeacon` on unload)
- [ ] `POST /analytics/events`: batched, validated ingest appending to a Redis Stream
- [ ] Aggregator consuming the stream via consumer group
- [ ] Total watch time per video, flushed to Postgres
- [ ] Audience retention curve (per 10s bucket of the video)
- [ ] Concurrent viewers gauge with TTL decay
- [ ] Unique views via Redis HyperLogLog
- [ ] Per-video analytics dashboard page in the web app (watch time, retention chart, live viewers)
- [ ] Worker generates storyboard sprite sheet + WebVTT during transcode
- [ ] Player seek-bar hover shows frame previews
- [ ] Verify: aggregator restart resumes the consumer group without gross double counting
- [ ] ADR: why a stream for ingest; delivery semantics; the Kafka scale-up path
- [ ] Tests: ingest validation, aggregation, retention bucketing

### Milestone 12: Auth, Quotas, Fair Scheduling

- [ ] Add migration `000006`: password hash on `users`, ownership enforcement, quota tracking
- [ ] Register/login endpoints with bcrypt/argon2 and JWT access tokens
- [ ] Ownership enforced on uploads and all mutating endpoints
- [ ] Per-user quotas: total storage bytes and uploads/day (checked at session create and complete)
- [ ] Per-user token-bucket rate limiting in Redis
- [ ] Per-tenant fair dispatch feeding `video.transcode` (round-robin or weighted)
- [ ] ADR: fair-scheduling mechanism chosen and alternatives considered
- [ ] Web: register/login UI and session handling
- [ ] Web: "my videos" page and quota usage display
- [ ] Fairness drill: tenant A enqueues 50 videos, tenant B enqueues 1; B completes in ~single-tenant time
- [ ] Tests: authz, quota enforcement, rate limits, dispatcher fairness

### Milestone 13: Video Understanding Layer

- [ ] Add migration `000005+`: enable `pgvector`; `video_transcripts`, `video_chapters`, `video_embeddings` tables; `enrichment_status` + enrichment job rows
- [ ] Extend M7 job hierarchy with `transcript` and `embedding` job types; planner fans them out via the outbox
- [ ] Enrichment is non-blocking: video reaches `ready` on transcode completion; ML failure degrades, never fails/wedges the video
- [ ] Transcript job: audio extract → Whisper → timestamped segments → WebVTT subtitle track (`EXT-X-MEDIA`) + DB rows
- [ ] Auto-chapters: LLM (Claude) segments transcript into titled chapters; rendered as seek-bar markers
- [ ] Embedding job: sample/dedup frames + transcript segments → CLIP/multimodal embeddings → pgvector
- [ ] Auto-thumbnail upgrade: pick representative frame by embedding centrality
- [ ] `GET /search?q=...`: embed query → vector similarity → ranked `{videoId, timestamp, snippet}` hits
- [ ] Web: search box; result deep-links to `/watch/:id?t=SECONDS` and seeks
- [ ] ADR: ML-as-a-job-type; pgvector vs dedicated vector DB; non-blocking/degradation design
- [ ] Tests: enrichment job parsing, search ranking, degradation when a model is unavailable
- [ ] Verify: upload → captions + chapters + searchable; NL query jumps to the right second

### Milestone 14: Mission Control — Live Fleet Dashboard + Chaos Controls

- [ ] Worker heartbeat state: `workers` table (+ Redis mirror) with status, current job/video, stage, progress, last heartbeat
- [ ] `GET /admin/fleet`: live workers + current work + queue depth + in-flight + time-to-ready stats
- [ ] `GET /admin/fleet/events`: SSE stream of fleet changes (reuses M8 SSE)
- [ ] `POST /admin/workers/:id/pause|resume`: graceful drain via Redis control channel
- [ ] `POST /admin/workers/:id/kill`: hard kill via container runtime (`docker kill` / k8s pod delete) through a thin supervisor
- [ ] Gate the entire chaos plane behind `CHAOS_MODE=true` + admin auth (M12); impossible in real deployments
- [ ] Optional: chaos roulette — kill a random worker every N seconds
- [ ] Web `/mission-control`: worker cards with live stage + progress + per-card KILL button
- [ ] Web: queue-depth gauge, in-flight count, rolling time-to-ready, per-video fan-out lanes
- [ ] Web: animate recovery after a kill (lease expiry → reaper requeue → resume → ready); killed-vs-completed scoreboard
- [ ] ADR: control-plane safety/gating; hard-kill implementation per environment; DB-vs-Redis fleet state
- [ ] Verify: KILL mid-job → UI shows full recovery, zero stuck videos
- [ ] Verify: chaos roulette runs for minutes with 100% of in-flight videos still completing

### Milestone 15: Content-Aware Encoding + Quality/Cost Proof

- [ ] Complexity analysis in the planner (fast CQ probe encode or `signalstats` energy)
- [ ] Per-title bitrate ladder derived from complexity (capped by source resolution), replacing the fixed ladder
- [ ] VMAF scoring per rendition (ffmpeg + libvmaf) of rendition vs source
- [ ] Add migration: VMAF + measured-bitrate + cost columns on `video_variants` (and/or `video_encode_stats`)
- [ ] Cost meter: track transcode CPU-seconds per video/rendition × `COST_PER_CPU_HOUR`
- [ ] Dashboard tiles: `$ / 1000 videos`, `bytes / minute`, CAE-vs-fixed comparison, quality-vs-bitrate curve
- [ ] Measurement under `tests/encoding/`: fixed ladder vs CAE on a corpus → bytes saved at equal VMAF + cost delta
- [ ] ADR: per-title approach, VMAF quality gate, cost-model assumptions
- [ ] Verify: CAE ships fewer bytes at equal-or-better VMAF; high-motion gets richer ladder, simple gets leaner

### Milestone 16: Auto-Trailer / Highlight Reel

- [ ] New non-blocking `highlight` enrichment job type (M7/M13 fan-out)
- [ ] Moment scoring: LLM transcript keypoints + audio energy + scene changes + frame-embedding salience/diversity
- [ ] Selection: top K non-overlapping ~20–30s windows in chronological order
- [ ] Assembly: FFmpeg trim+concat → run the trailer back through the HLS pipeline (`processed-videos/{videoId}/trailer/`)
- [ ] Animated hover preview (GIF/WebP) + representative thumbnail reuse
- [ ] Web: grid plays trailer/preview on hover, "trailer" badge, share button
- [ ] ADR: heuristic vs LLM moment selection; non-blocking enrichment; reusing the transcode pipeline
- [ ] Tests: moment selection, graceful degradation when generation fails
- [ ] Verify: multi-minute upload → ~30s trailer spanning distinct moments; failure never blocks `ready`

### Phase 4 Capstone (pick after Milestone 10)

- [ ] Decide: live streaming vs Kubernetes/cloud first (ADR)
- [ ] Scope the chosen capstone into its own milestone plan in `MEDIAFLOW_PLAN.md`

## Current Decisions

| Topic | Decision |
| --- | --- |
| Product name | MediaFlow |
| First protocol | HLS |
| First storage | MinIO locally |
| First queue | RabbitMQ |
| First DB | PostgreSQL |
| First backend language | Go |
| First frontend framework | Next.js |
| CI | GitHub Actions + testcontainers-go integration tests against real dependencies |
| Publish strategy | Transactional outbox, at-least-once delivery |
| Stuck-job recovery | Leases with heartbeats + reaper |
| Retry strategy | TTL retry queue + DLX, max 3 attempts, then DLQ |
| Upload path (M6) | Presigned multipart direct-to-MinIO; API is control plane only |
| Transcode topology (M7) | Planner fan-out of per-rendition jobs + atomic aggregation |
| Playback (M8) | API manifest rewriting + HMAC-signed segment URLs behind nginx edge cache |
| Realtime status (M8) | SSE over Redis pub/sub with `video_events` replay; polling fallback |
| Analytics ingest (M11) | Redis Streams + consumer groups; Kafka documented as scale-up path |
| Video understanding (M13) | Enrichment as extra fan-out job types; vectors in pgvector (no new datastore); non-blocking so ML failures degrade, never wedge a video |
| Chaos controls (M14) | Hard kill via container runtime through a supervisor; entire chaos plane gated behind `CHAOS_MODE` + admin auth |
| Encoding (M15) | Per-title (content-aware) bitrate ladder; VMAF as the quality gate; savings proven by a fixed-vs-CAE measurement, not claimed |
| Auto-trailer (M16) | A `highlight` enrichment job; trailer is assembled then re-run through the existing HLS pipeline (proves the pipeline composes) |
| SQL access | `database/sql` + pgx, no ORM |

## Open Questions

- Segment-level parallel transcoding (M7 stretch): implement locally or document the design and skip?
- CMAF/fMP4 segments instead of MPEG-TS: revisit for LL-HLS or the live-streaming capstone.
- Phase 4 pick: live streaming vs Kubernetes/cloud first.
- Fair-scheduling mechanism detail (M12): dispatcher service vs RabbitMQ priority queues — decide with an ADR.

## Update Rules

- Update this file after each completed task or milestone.
- Keep statuses simple: `Not started`, `In progress`, `Blocked`, `Done`.
- Add short notes when a task changes architecture, schema, queue contracts, or environment variables.
- Do not mark a milestone `Done` until its checklist is complete and its failure drills / verification steps have actually been run.
- Write each milestone's ADR and docs while building it, not after.
