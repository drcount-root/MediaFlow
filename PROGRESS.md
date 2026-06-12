# MediaFlow Progress Tracker

Last updated: 2026-06-12

## Overall Status

Status: Phase 1 (MVP, Milestones 0–3) complete. Phase 2 (hardcore system design, Milestones 4–9) not started.

Current focus:

```txt
Milestone 4: Correctness Under Failure
```

See `MEDIAFLOW_PLAN.md` for the design behind each Phase 2 milestone.

## Milestones

| Milestone | Status | Notes |
| --- | --- | --- |
| 0. Repo and Infra | Done | Scaffold, Compose file, env examples, migration, README, and live dependency startup verified. |
| 1. API Upload Path | Done | Upload path, DB writes, MinIO storage, RabbitMQ publishing, list/detail/playback endpoints, migration command, and API tests are working. |
| 2. Worker Transcoding Path | Done | Worker consumes jobs, runs FFmpeg/ffprobe, creates thumbnail and HLS variants, uploads outputs, and marks videos ready. |
| 3. Web Playback Path | Done | Next.js app supports upload, video list, status polling, HLS watch page, manual quality selection, and local smoke checks. |
| 4. Correctness Under Failure | Not started | Transactional outbox, job leases + reaper, retries/backoff/DLQ, idempotency, graceful shutdown. |
| 5. Scalable Ingest | Not started | Presigned multipart direct-to-MinIO uploads, resumable, checksummed. API becomes control plane only. |
| 6. Distributed Transcoding | Not started | Planner fan-out of per-rendition jobs, atomic aggregation, finalize step, parallel workers. |
| 7. Serving At Scale | Not started | Private buckets, manifest rewriting with HMAC-signed segment URLs, nginx edge cache, Redis caching/rate limiting/counters. |
| 8. Observability | Not started | OpenTelemetry traces across the queue, Prometheus metrics, Jaeger + Grafana dashboards. |
| 9. Proof: SLOs, Load, Chaos | Not started | Stated SLOs, k6 load tests, scripted chaos scenarios with postmortems, ADRs. |

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

### Milestone 4: Correctness Under Failure

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
- [ ] Tests: outbox relay, claim/lease, retry routing, reaper, idempotency key
- [ ] Failure drill: `kill -9` worker mid-job → reaper recovers → video `ready`
- [ ] Failure drill: RabbitMQ down during upload → outbox drains after restart
- [ ] Failure drill: poison message lands in DLQ without wedging the consumer

### Milestone 5: Scalable Ingest

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

### Milestone 6: Distributed Transcoding

- [ ] Add migration `000004`: `video_jobs.parent_job_id`, job types `plan | rendition | finalize`, pending-rendition tracking
- [ ] Split worker into planner and rendition consumers
- [ ] Planner: probe, thumbnail, plan renditions, fan out `video.rendition` jobs via outbox
- [ ] Rendition worker: transcode exactly one quality, upload its playlist and segments
- [ ] Apply M4 leases/retries per rendition (one rendition retries without redoing others)
- [ ] Atomic completion counter (`UPDATE ... RETURNING`); last rendition triggers finalize
- [ ] Finalizer: write `master.m3u8`, insert `video_variants`, mark video `ready`
- [ ] Partial failure: exhausted rendition fails the video and cleans up
- [ ] Run and document `docker compose up --scale worker=3`
- [ ] Measure parallel speedup: 3 workers vs 1 on the same source video
- [ ] Autoscaling experiment: scale workers on queue depth, record results
- [ ] Tests: fan-out, aggregation race (two renditions finish simultaneously), partial failure
- [ ] Stretch: segment-level parallel transcode and playlist stitching

### Milestone 7: Serving At Scale

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
- [ ] Verify: anonymous segment fetch fails; expired signature returns 401/403
- [ ] Verify: repeat playback hits nginx cache, not MinIO
- [ ] Tests: signing, expiry, manifest rewriting, rate limiting

### Milestone 8: Observability

- [ ] OpenTelemetry tracing middleware in the API
- [ ] Inject trace context into AMQP headers in the outbox relay
- [ ] Extract trace context in consumers; spans per stage (download, probe, thumbnail, transcode, upload, finalize)
- [ ] Add Jaeger to Docker Compose
- [ ] Verify: single trace spans upload → queue → renditions → `ready`
- [ ] Prometheus metrics in API (latency, RPS, error rate)
- [ ] Prometheus metrics in worker (stage durations, success/failure/retry counters)
- [ ] Enable RabbitMQ prometheus plugin (queue depth, consumers)
- [ ] nginx cache hit ratio metrics
- [ ] Add Prometheus and Grafana to Docker Compose with provisioned dashboards
- [ ] Pipeline health dashboard (queue depth, in-flight jobs, time-to-ready p50/p95, failure rate)
- [ ] Structured `slog` logging with trace/correlation IDs in both Go apps
- [ ] Store trace id in `video_events.metadata_json`
- [ ] Draft alert rules: queue lag, error rate, jobs stuck in `processing`

### Milestone 9: Proof — SLOs, Load, Chaos

- [ ] Write `docs/SLOS.md` with stated, measurable objectives
- [ ] k6 upload load test (`tests/load/`)
- [ ] k6 playback concurrency test against the edge cache
- [ ] Soak test: sustained uploads, assert zero stuck videos
- [ ] Chaos: `kill -9` rendition worker mid-transcode + postmortem
- [ ] Chaos: RabbitMQ restart under load + postmortem
- [ ] Chaos: MinIO unavailable during transcode + postmortem
- [ ] Chaos: `WORK_DIR` disk full + postmortem
- [ ] Chaos: Postgres restart under load + postmortem
- [ ] E2E smoke script: fresh compose stack → upload → ready → playback
- [ ] Architecture diagram of the final system
- [ ] ADRs in `docs/adr/` (outbox, leases, fan-out, manifest signing)
- [ ] Record load test results against SLOs
- [ ] Final README and docs update

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
| Publish strategy | Transactional outbox, at-least-once delivery |
| Stuck-job recovery | Leases with heartbeats + reaper |
| Retry strategy | TTL retry queue + DLX, max 3 attempts, then DLQ |
| Upload path (Phase 2) | Presigned multipart direct-to-MinIO; API is control plane only |
| Transcode topology (Phase 2) | Planner fan-out of per-rendition jobs + atomic aggregation |
| Playback (Phase 2) | API manifest rewriting + HMAC-signed segment URLs behind nginx edge cache |
| SQL access | `database/sql` + pgx, no ORM |

## Open Questions

- Segment-level parallel transcoding (M6 stretch): implement locally or document the design and skip?
- CMAF/fMP4 segments instead of MPEG-TS: revisit if LL-HLS becomes interesting.
- Kubernetes + KEDA deployment: only after M9; the compose autoscaling experiment comes first.

## Update Rules

- Update this file after each completed task or milestone.
- Keep statuses simple: `Not started`, `In progress`, `Blocked`, `Done`.
- Add short notes when a task changes architecture, schema, queue contracts, or environment variables.
- Do not mark a milestone `Done` until its checklist is complete and its failure drills / verification steps have actually been run.
