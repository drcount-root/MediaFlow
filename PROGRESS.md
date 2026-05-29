# MediaFlow Progress Tracker

Last updated: 2026-05-29

## Overall Status

Status: Planning complete, implementation not started.

Current focus:

```txt
Milestone 0: Repository and local infrastructure scaffold
```

## Milestones

| Milestone | Status | Notes |
| --- | --- | --- |
| 0. Repo and Infra | Not started | Create folders, Docker Compose, migrations, local README. |
| 1. API Upload Path | Not started | Upload MP4, store raw file, save metadata, publish queue job. |
| 2. Worker Transcoding Path | Not started | Consume job, run FFmpeg, create HLS, upload processed outputs. |
| 3. Web Playback Path | Not started | Upload UI, status page, HLS watch page. |
| 4. Hardening | Not started | Retries, errors, logs, validation, docs. |

## Detailed Checklist

### Milestone 0: Repo and Infra

- [ ] Create `apps/api`
- [ ] Create `apps/worker`
- [ ] Create `apps/web`
- [ ] Create `packages/shared`
- [ ] Create `infrastructure/migrations`
- [ ] Add Docker Compose for PostgreSQL
- [ ] Add Docker Compose for RabbitMQ
- [ ] Add Docker Compose for Redis
- [ ] Add Docker Compose for MinIO
- [ ] Add MinIO bucket setup for `mediaflow-raw`
- [ ] Add MinIO bucket setup for `mediaflow-processed`
- [ ] Add MinIO bucket setup for `mediaflow-thumbnails`
- [ ] Add initial database migration
- [ ] Add local environment examples
- [ ] Add root README with startup instructions

### Milestone 1: API Upload Path

- [ ] Initialize Go API app
- [ ] Add health endpoint
- [ ] Add database connection
- [ ] Add migration runner or migration command
- [ ] Add MinIO client
- [ ] Add RabbitMQ publisher
- [ ] Add upload request validation
- [ ] Store original MP4 in MinIO
- [ ] Create `videos` row
- [ ] Create `video_jobs` row
- [ ] Publish `video.transcode` job
- [ ] Add `GET /videos`
- [ ] Add `GET /videos/:id`
- [ ] Add `GET /videos/:id/playback`
- [ ] Add API tests

### Milestone 2: Worker Transcoding Path

- [ ] Initialize worker app
- [ ] Add RabbitMQ consumer
- [ ] Add database connection
- [ ] Add MinIO client
- [ ] Claim queued job safely
- [ ] Update video status to `processing`
- [ ] Download raw video to temp directory
- [ ] Run `ffprobe`
- [ ] Save duration and metadata
- [ ] Generate thumbnail with FFmpeg
- [ ] Generate HLS master manifest
- [ ] Generate 720p variant
- [ ] Generate 480p variant
- [ ] Generate 360p variant if source allows
- [ ] Upload HLS output to MinIO
- [ ] Upload thumbnail to MinIO
- [ ] Insert `video_variants`
- [ ] Update video status to `ready`
- [ ] Mark job `completed`
- [ ] Handle failures and update status to `failed`
- [ ] Add worker tests

### Milestone 3: Web Playback Path

- [ ] Initialize Next.js app
- [ ] Add API client
- [ ] Build upload page
- [ ] Add upload validation UI
- [ ] Build video list page
- [ ] Build processing status page
- [ ] Add status polling
- [ ] Build watch page
- [ ] Integrate `hls.js`
- [ ] Show playback errors clearly
- [ ] Add frontend tests or smoke checks

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

- Should the worker be Go from day one, or Python for faster FFmpeg scripting?
- Should playback use presigned MinIO URLs or API-proxied URLs in the MVP?
- Which migration tool should be used: Goose, golang-migrate, or another tool?
- Should SQL access use `database/sql`, `sqlc`, or an ORM?

## Update Rules

- Update this file after each completed task or milestone.
- Keep statuses simple: `Not started`, `In progress`, `Blocked`, `Done`.
- Add short notes when a task changes architecture, schema, queue contracts, or environment variables.
- Do not mark a milestone `Done` until its checklist is complete and manually verified.
