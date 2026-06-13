# MediaFlow Progress Tracker

Last updated: 2026-06-13

## Overall Status

Status: Phase 1 (MVP, Milestones 0–3) complete. Phase 2 (Milestones 4–10) in progress — Milestone 4 harness landed and verified locally. Phase 3 (Milestones 11–12) and Phase 4 capstones follow.

Current focus:

```txt
Milestone 4: CI and Integration Test Harness (verified locally; pending first green run + branch protection on GitHub)
→ next: Milestone 5: Correctness Under Failure
```

See `MEDIAFLOW_PLAN.md` for the design behind each milestone.

## Milestones

| Milestone | Status | Notes |
| --- | --- | --- |
| 0. Repo and Infra | Done | Scaffold, Compose file, env examples, migration, README, and live dependency startup verified. |
| 1. API Upload Path | Done | Upload path, DB writes, MinIO storage, RabbitMQ publishing, list/detail/playback endpoints, migration command, and API tests are working. |
| 2. Worker Transcoding Path | Done | Worker consumes jobs, runs FFmpeg/ffprobe, creates thumbnail and HLS variants, uploads outputs, and marks videos ready. |
| 3. Web Playback Path | Done | Next.js app supports upload, video list, status polling, HLS watch page, manual quality selection, and local smoke checks. |
| 4. CI and Integration Test Harness | In progress | GitHub Actions workflow + testcontainers-go integration tests (Postgres/RabbitMQ/MinIO, full upload→ready flow) written and verified locally. Remaining: first green run on GitHub + mark CI required for PRs (branch protection). ADR: `docs/adr/0001-ci-and-integration-harness.md`. |
| 5. Correctness Under Failure | Not started | Transactional outbox, job leases + reaper, retries/backoff/DLQ, idempotency, graceful shutdown. |
| 6. Scalable Ingest | Not started | Presigned multipart direct-to-MinIO uploads, resumable, checksummed. API becomes control plane only. |
| 7. Distributed Transcoding | Not started | Planner fan-out of per-rendition jobs, atomic aggregation, finalize step, parallel workers. |
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

- [ ] Add migration `000002`: `outbox_messages` table
- [ ] Add migration `000002`: `video_jobs.claimed_by` and `video_jobs.lease_expires_at`
- [ ] Write video + job + outbox row in one DB transaction in `Upload`
- [ ] Remove direct RabbitMQ publish from the upload request path
- [ ] Add outbox relay loop in API (`FOR UPDATE SKIP LOCKED`, publisher confirms)
- [ ] Increment `video_jobs.attempts` on every claim
- [ ] Add worker heartbeat that extends the lease during processing
- [ ] Add reaper: expired lease below max attempts → requeue via outbox
- [ ] Add reaper: expired lease at max attempts → mark job and video `failed`
- [ ] Declare `video.transcode.retry` queue with per-message TTL and DLX back to `video.transcode`
- [ ] Declare `video.transcode.dlq` and route exhausted/poison messages there
- [ ] Classify permanent vs transient failures (no retry for corrupt input)
- [ ] Add `Idempotency-Key` support on upload
- [ ] Make worker retries overwrite-safe (clear stale variants, deterministic keys)
- [ ] Add graceful worker shutdown (finish in-flight job on SIGTERM)
- [ ] Write `video_events` row on every status transition
- [ ] Tests (incl. integration): outbox relay, claim/lease, retry routing, reaper, idempotency key
- [ ] Failure drill: `kill -9` worker mid-job → reaper recovers → video `ready`
- [ ] Failure drill: RabbitMQ down during upload → outbox drains after restart
- [ ] Failure drill: poison message lands in DLQ without wedging the consumer

### Milestone 6: Scalable Ingest

- [ ] Add migration `000003`: `upload_sessions` table
- [ ] `POST /uploads`: create session, initiate MinIO multipart upload
- [ ] `GET /uploads/:id/parts/:n/url`: issue presigned part URL
- [ ] `GET /uploads/:id`: report session status and uploaded parts for resume
- [ ] `POST /uploads/:id/complete`: complete multipart, validate size and checksums, enqueue via outbox
- [ ] `DELETE /uploads/:id`: abort multipart upload
- [ ] Cleanup loop: abort expired sessions and orphaned multipart uploads
- [ ] Enforce size limits at session creation and at completion
- [ ] Web: chunked uploader with bounded parallelism and per-part retry
- [ ] Web: resume upload after page reload (persist session id)
- [ ] Web: real upload progress UI
- [ ] Remove (or flag-gate) legacy `POST /videos/upload` proxy endpoint
- [ ] Tests: session lifecycle, resume, checksum mismatch, oversize rejection
- [ ] Verify: 500MB upload never transits the API process

### Milestone 7: Distributed Transcoding

- [ ] Add migration `000004`: `video_jobs.parent_job_id`, job types `plan | rendition | finalize`, pending-rendition tracking
- [ ] Split worker into planner and rendition consumers
- [ ] Planner: probe, thumbnail, plan renditions, fan out `video.rendition` jobs via outbox
- [ ] Rendition worker: transcode exactly one quality, upload its playlist and segments
- [ ] Apply M5 leases/retries per rendition (one rendition retries without redoing others)
- [ ] Atomic completion counter (`UPDATE ... RETURNING`); last rendition triggers finalize
- [ ] Finalizer: write `master.m3u8`, insert `video_variants`, mark video `ready`
- [ ] Partial failure: exhausted rendition fails the video and cleans up
- [ ] Run and document `docker compose up --scale worker=3`
- [ ] Measure parallel speedup: 3 workers vs 1 on the same source video
- [ ] Autoscaling experiment: scale workers on queue depth, record results
- [ ] Tests: fan-out, aggregation race (two renditions finish simultaneously), partial failure
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
