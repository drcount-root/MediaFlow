# MediaFlow Progress Tracker

Last updated: 2026-05-30

## Overall Status

Status: Milestone 3 complete. Ready for Milestone 4.

Current focus:

```txt
Milestone 4: Hardening
```

## Milestones

| Milestone | Status | Notes |
| --- | --- | --- |
| 0. Repo and Infra | Done | Scaffold, Compose file, env examples, migration, README, and live dependency startup verified. |
| 1. API Upload Path | Done | Upload path, DB writes, MinIO storage, RabbitMQ publishing, list/detail/playback endpoints, migration command, and API tests are working. |
| 2. Worker Transcoding Path | Done | Worker consumes jobs, runs FFmpeg/ffprobe, creates thumbnail and HLS variants, uploads outputs, and marks videos ready. |
| 3. Web Playback Path | Done | Next.js app supports upload, video list, status polling, HLS watch page, manual quality selection, and local smoke checks. |
| 4. Hardening | Not started | Retries, errors, logs, validation, docs. |

## Detailed Checklist

### Milestone 0: Repo and Infra

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

### Milestone 1: API Upload Path

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

### Milestone 2: Worker Transcoding Path

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

### Milestone 3: Web Playback Path

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

### Milestone 4: Hardening

- [ ] Add structured logging
- [ ] Add DB event logging
- [ ] Add retry attempt tracking
- [ ] Add max retry handling
- [ ] Add dead-letter queue plan or implementation
- [ ] Add large file rejection
- [ ] Add unsupported file type rejection
- [ ] Add no-audio video handling
- [ ] Add low-resolution source handling
- [ ] Add cleanup for worker temp directories
- [ ] Add end-to-end smoke test instructions
- [ ] Update `AGENTS.md` with final commands
- [ ] Update `MEDIAFLOW_PLAN.md` if architecture changes

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
| First worker strategy | Single worker, queue-backed, FFmpeg-based |

## Open Questions

- Should playback use presigned MinIO URLs long-term, API-proxied URLs, or manifest rewriting with signed variant URLs?
- Should SQL access use `database/sql`, `sqlc`, or an ORM?

## Update Rules

- Update this file after each completed task or milestone.
- Keep statuses simple: `Not started`, `In progress`, `Blocked`, `Done`.
- Add short notes when a task changes architecture, schema, queue contracts, or environment variables.
- Do not mark a milestone `Done` until its checklist is complete and manually verified.
