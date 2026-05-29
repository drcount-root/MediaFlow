# MediaFlow Implementation Plan

## Goal

Build a portfolio-grade YouTube-like VOD platform that supports video uploads, asynchronous processing, FFmpeg transcoding, HLS adaptive streaming, thumbnail generation, and playback through a web UI.

The first milestone is intentionally narrow:

```txt
Upload MP4 -> Queue Job -> FFmpeg creates HLS -> hls.js plays adaptive stream
```

## Core Architecture

```txt
Frontend
  -> Upload API
  -> Object Storage: raw video
  -> Queue: video processing job
  -> Worker: FFmpeg transcode + thumbnails
  -> Object Storage: HLS output
  -> DB status update
  -> Frontend player loads master.m3u8
```

## Recommended Stack

| Area | Choice |
| --- | --- |
| Frontend | Next.js, hls.js |
| Backend API | Go + Gin |
| Queue | RabbitMQ |
| Database | PostgreSQL |
| Cache/status | Redis |
| Local object storage | MinIO |
| Cloud object storage later | AWS S3 |
| Video processing | FFmpeg |
| Local infrastructure | Docker Compose |
| Production infrastructure later | Kubernetes, CDN |

## Local Development Assumptions

These assumptions keep the first implementation practical:

- The first version is local-only.
- One developer runs all services through Docker Compose plus local app commands.
- Authentication is skipped initially. Use a nullable `user_id` or a seeded demo user.
- Uploaded files are accepted through the API server first. Direct browser-to-MinIO uploads are a later optimization.
- Max upload size can start at `500MB` locally, then be made configurable.
- Supported upload format for MVP: `video/mp4`.
- Output protocol for MVP: HLS only.
- Output container for HLS segments: MPEG-TS initially, CMAF/fMP4 later if needed.
- Playback URLs can be presigned MinIO URLs or proxied API URLs. Prefer presigned URLs for simpler streaming.
- The first worker can process one video at a time. Parallel worker scaling comes later.

## Environment Variables

Use explicit env vars per app so Docker/local runs are predictable.

### API

```txt
APP_ENV=local
HTTP_ADDR=:8080
DATABASE_URL=postgres://mediaflow:mediaflow@localhost:5432/mediaflow?sslmode=disable
RABBITMQ_URL=amqp://mediaflow:mediaflow@localhost:5672/
REDIS_ADDR=localhost:6379
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=mediaflow
MINIO_SECRET_KEY=mediaflow-secret
MINIO_USE_SSL=false
MINIO_RAW_BUCKET=mediaflow-raw
MINIO_PROCESSED_BUCKET=mediaflow-processed
MINIO_THUMBNAIL_BUCKET=mediaflow-thumbnails
MAX_UPLOAD_BYTES=524288000
```

### Worker

```txt
APP_ENV=local
DATABASE_URL=postgres://mediaflow:mediaflow@localhost:5432/mediaflow?sslmode=disable
RABBITMQ_URL=amqp://mediaflow:mediaflow@localhost:5672/
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=mediaflow
MINIO_SECRET_KEY=mediaflow-secret
MINIO_USE_SSL=false
MINIO_RAW_BUCKET=mediaflow-raw
MINIO_PROCESSED_BUCKET=mediaflow-processed
MINIO_THUMBNAIL_BUCKET=mediaflow-thumbnails
WORKER_CONCURRENCY=1
WORK_DIR=/tmp/mediaflow-worker
FFMPEG_PATH=ffmpeg
FFPROBE_PATH=ffprobe
```

### Web

```txt
NEXT_PUBLIC_API_BASE_URL=http://localhost:8080
```

## Repository Shape

Start simple. Avoid premature microservice sprawl.

```txt
/apps
  /api                 # Go API: upload, metadata, playback URLs
  /worker              # Go or Python worker: FFmpeg processing pipeline
  /web                 # Next.js frontend

/packages
  /shared              # Shared contracts/types/config if needed later

/infrastructure
  docker-compose.yml   # Postgres, RabbitMQ, Redis, MinIO
  /migrations          # Database migrations
```

Later, `api` can be split into upload, metadata, streaming, analytics, and notification services if the project needs that scale.

## Detailed Service Responsibilities

### Web App

Responsibilities:

- Render upload UI.
- Send multipart upload request to the API.
- Show video list and video processing state.
- Poll video status until it becomes `ready` or `failed`.
- Render HLS playback with `hls.js`.
- Handle basic playback errors, such as missing manifest or video not ready.

Non-responsibilities for MVP:

- No auth.
- No direct MinIO credentials in the browser.
- No transcoding logic.

### API App

Responsibilities:

- Validate upload requests.
- Create video IDs.
- Store original video in MinIO.
- Write metadata rows to PostgreSQL.
- Create a `video_jobs` row.
- Publish a RabbitMQ message.
- Return quickly after queueing.
- Expose video status, metadata, playback URL, and event logs.
- Ensure bucket existence on startup or through a setup command.

Non-responsibilities for MVP:

- No FFmpeg processing.
- No long-running job execution inside request handlers.
- No CDN behavior.

### Worker App

Responsibilities:

- Consume `video.transcode` messages.
- Claim the corresponding job.
- Set video status to `processing`.
- Download raw video from MinIO.
- Run `ffprobe` to get metadata.
- Run FFmpeg to generate HLS renditions.
- Generate thumbnail.
- Upload processed outputs to MinIO.
- Insert `video_variants`.
- Set video status to `ready`.
- Ack successful messages.
- Mark failed jobs/videos and nack or dead-letter unrecoverable messages.

Non-responsibilities for MVP:

- No user-facing API.
- No recommendation logic.
- No distributed chunk-level processing.

## MVP Scope

Phase 1 must include:

1. Upload video from UI.
2. Store original file in MinIO.
3. Save video metadata in PostgreSQL with status `uploaded`.
4. Publish RabbitMQ job.
5. Worker consumes job.
6. FFmpeg generates HLS outputs for `720p`, `480p`, and optionally `360p`.
7. Worker generates thumbnail.
8. Upload processed HLS files and thumbnail to MinIO.
9. Update DB status to `ready`.
10. Playback page loads `master.m3u8` with `hls.js`.

## Deferred Scope

Do not build these until the core pipeline works:

- Auth
- Likes/comments
- Recommendations
- AI moderation
- AI subtitles
- Livestreaming
- Shorts/reels
- WebRTC low latency
- CDN integration
- Kubernetes deployment
- Resumable uploads
- Distributed workers

## Video Lifecycle

```txt
uploading
  -> uploaded
  -> queued
  -> processing
  -> ready
```

Failure path:

```txt
uploading/uploaded/queued/processing
  -> failed
```

State transition rules:

- `uploading`: optional temporary state while metadata row exists before object upload finishes.
- `uploaded`: original object exists in MinIO.
- `queued`: RabbitMQ message was published and DB job exists.
- `processing`: worker has claimed the job.
- `ready`: HLS master manifest, variants, and thumbnail are available.
- `failed`: pipeline failed. `error_message` should explain the failure.

Avoid silent transitions. Each meaningful transition should write a `video_events` row.

## Initial Database Draft

### `users`

Can be skipped in the first local MVP if auth is deferred, but keep the design ready.

```txt
id
email
display_name
created_at
updated_at
```

### `videos`

```txt
id
user_id
title
description
status                    # uploading | uploaded | queued | processing | ready | failed
raw_object_key
hls_master_key
thumbnail_key
duration_seconds
original_filename
content_type
size_bytes
error_message
created_at
updated_at
```

### `video_variants`

```txt
id
video_id
quality                   # 720p | 480p | 360p
width
height
bitrate
codec
playlist_key
created_at
```

### `video_jobs`

```txt
id
video_id
job_type                  # transcode
status                    # queued | processing | completed | failed
attempts
last_error
created_at
updated_at
```

### `video_events`

Useful for debugging the pipeline.

```txt
id
video_id
event_type
message
metadata_json
created_at
```

## Suggested SQL Schema

This is not final migration syntax, but it is close enough to implement from.

```sql
CREATE TYPE video_status AS ENUM (
  'uploading',
  'uploaded',
  'queued',
  'processing',
  'ready',
  'failed'
);

CREATE TYPE job_status AS ENUM (
  'queued',
  'processing',
  'completed',
  'failed'
);

CREATE TABLE users (
  id UUID PRIMARY KEY,
  email TEXT UNIQUE,
  display_name TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE videos (
  id UUID PRIMARY KEY,
  user_id UUID REFERENCES users(id),
  title TEXT NOT NULL,
  description TEXT,
  status video_status NOT NULL,
  raw_object_key TEXT,
  hls_master_key TEXT,
  thumbnail_key TEXT,
  duration_seconds NUMERIC,
  original_filename TEXT,
  content_type TEXT,
  size_bytes BIGINT,
  error_message TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_videos_status ON videos(status);
CREATE INDEX idx_videos_created_at ON videos(created_at DESC);

CREATE TABLE video_variants (
  id UUID PRIMARY KEY,
  video_id UUID NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
  quality TEXT NOT NULL,
  width INT NOT NULL,
  height INT NOT NULL,
  bitrate INT NOT NULL,
  codec TEXT,
  playlist_key TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(video_id, quality)
);

CREATE TABLE video_jobs (
  id UUID PRIMARY KEY,
  video_id UUID NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
  job_type TEXT NOT NULL,
  status job_status NOT NULL,
  attempts INT NOT NULL DEFAULT 0,
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_video_jobs_status ON video_jobs(status);
CREATE INDEX idx_video_jobs_video_id ON video_jobs(video_id);

CREATE TABLE video_events (
  id UUID PRIMARY KEY,
  video_id UUID NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
  event_type TEXT NOT NULL,
  message TEXT NOT NULL,
  metadata_json JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_video_events_video_id_created_at ON video_events(video_id, created_at DESC);
```

## Queue Design

Start with one queue:

```txt
video.transcode
```

Initial job payload:

```json
{
  "jobId": "uuid",
  "videoId": "uuid",
  "rawBucket": "mediaflow-raw",
  "rawObjectKey": "raw-videos/video-id/original.mp4",
  "requestedAt": "2026-05-29T00:00:00Z"
}
```

RabbitMQ settings for MVP:

```txt
exchange: mediaflow.video
exchange_type: direct
queue: video.transcode
routing_key: video.transcode
durable: true
message_delivery_mode: persistent
prefetch_count: 1
```

Retry strategy for MVP:

- On transient failure, increment `attempts`.
- Retry up to `3` attempts.
- After max attempts, mark job and video as `failed`.
- Add dead-letter queue later: `video.transcode.dlq`.

Later queues:

```txt
video.thumbnail
video.completed
video.failed
video.ai_moderation
video.subtitle
```

## Storage Layout

Raw uploads:

```txt
raw-videos/{videoId}/original.mp4
```

Processed HLS:

```txt
processed-videos/{videoId}/master.m3u8
processed-videos/{videoId}/720p/index.m3u8
processed-videos/{videoId}/720p/segment_000.ts
processed-videos/{videoId}/480p/index.m3u8
processed-videos/{videoId}/480p/segment_000.ts
processed-videos/{videoId}/360p/index.m3u8
processed-videos/{videoId}/360p/segment_000.ts
```

Thumbnails:

```txt
thumbnails/{videoId}/default.jpg
```

Bucket policy:

- Raw bucket should remain private.
- Processed and thumbnail buckets can remain private with presigned URLs.
- Public bucket access can be considered only for local convenience.

Path conventions:

- Always include `videoId` in object keys.
- Avoid using original filenames in object keys.
- Store original filename only as metadata in PostgreSQL.
- Use deterministic paths so retrying a job can overwrite partial output safely after cleanup.

## FFmpeg Processing Plan

Worker responsibilities:

1. Download original video from MinIO.
2. Probe metadata with `ffprobe`.
3. Generate thumbnail.
4. Generate HLS variants.
5. Upload generated files to MinIO.
6. Update `videos` and `video_variants`.
7. Mark job completed or failed.

Initial qualities:

| Quality | Resolution | Approx Bitrate |
| --- | --- | --- |
| 720p | 1280x720 | 2800k |
| 480p | 854x480 | 1400k |
| 360p | 640x360 | 800k |

### Metadata Probe

Use `ffprobe` before transcoding:

```bash
ffprobe -v error \
  -show_entries format=duration,size,bit_rate \
  -show_entries stream=codec_type,codec_name,width,height \
  -of json \
  input.mp4
```

Use this to:

- Save duration.
- Validate that a video stream exists.
- Skip variants larger than the source resolution.
- Decide whether audio exists.

### Thumbnail Command

Generate one thumbnail around the first few seconds:

```bash
ffmpeg -y \
  -ss 00:00:03 \
  -i input.mp4 \
  -frames:v 1 \
  -vf "scale=640:-1" \
  thumbnail.jpg
```

If the video is shorter than 3 seconds, fall back to `00:00:00`.

### HLS Command Direction

The final command may be adjusted during implementation, but the goal is:

- Generate a `master.m3u8`.
- Generate one playlist per quality.
- Generate 4 to 6 second segments.
- Use H.264 video and AAC audio for broad compatibility.

Example direction:

```bash
ffmpeg -y -i input.mp4 \
  -filter_complex \
  "[0:v]split=3[v720][v480][v360]; \
   [v720]scale=w=1280:h=720:force_original_aspect_ratio=decrease[v720out]; \
   [v480]scale=w=854:h=480:force_original_aspect_ratio=decrease[v480out]; \
   [v360]scale=w=640:h=360:force_original_aspect_ratio=decrease[v360out]" \
  -map "[v720out]" -map 0:a? -c:v:0 libx264 -b:v:0 2800k -maxrate:v:0 2996k -bufsize:v:0 4200k \
  -map "[v480out]" -map 0:a? -c:v:1 libx264 -b:v:1 1400k -maxrate:v:1 1498k -bufsize:v:1 2100k \
  -map "[v360out]" -map 0:a? -c:v:2 libx264 -b:v:2 800k -maxrate:v:2 856k -bufsize:v:2 1200k \
  -c:a aac -ar 48000 -b:a 128k \
  -preset veryfast \
  -g 48 -keyint_min 48 -sc_threshold 0 \
  -hls_time 4 \
  -hls_playlist_type vod \
  -hls_segment_filename "out/%v/segment_%03d.ts" \
  -master_pl_name master.m3u8 \
  -var_stream_map "v:0,a:0 v:1,a:1 v:2,a:2" \
  out/%v/index.m3u8
```

Implementation note:

- The `var_stream_map` needs care when input has no audio.
- Start with a known sample video that has audio.
- Then add no-audio handling.
- If the source is lower than 720p, generate only variants at or below source height.

### Worker Temporary Directory

For each job:

```txt
/tmp/mediaflow-worker/{jobId}/
  input.mp4
  thumbnail.jpg
  hls/
    master.m3u8
    0/
    1/
    2/
```

Cleanup rules:

- Remove job temp directory after success.
- Remove job temp directory after failure if logs are enough.
- Keep local temp files only while debugging.

### Idempotency

The worker should be safe to retry.

Before processing a retry:

- Check if video is already `ready`; if yes, ack and skip.
- Clear stale `video_variants` for that video.
- Overwrite processed object keys for that video.
- Keep raw upload untouched.

## Frontend Pages

### Upload Page

Required behavior:

- Select MP4 file.
- Enter title and description.
- Upload to backend.
- Show upload/progress status if simple to implement.
- Redirect to video status page or watch page.

UX states:

- Empty form.
- File selected.
- Uploading.
- Queued.
- Upload failed.

Validation:

- Require title.
- Require file.
- Accept `video/mp4`.
- Show clear error when backend rejects file size or type.

### Video Status Page

Required behavior:

- Poll video status.
- Show processing state while worker runs.
- Link to playback when status is `ready`.
- Show error if status is `failed`.

Polling:

- Poll `GET /videos/:id` every 2 seconds while status is `queued` or `processing`.
- Stop polling when status is `ready` or `failed`.

### Watch Page

Required behavior:

- Load video metadata.
- Load HLS URL.
- Use `hls.js` for browsers without native HLS support.
- Support automatic adaptive bitrate switching through HLS.

Playback logic:

```txt
if video.canPlayType("application/vnd.apple.mpegurl"):
  video.src = hlsUrl
else if Hls.isSupported():
  const hls = new Hls()
  hls.loadSource(hlsUrl)
  hls.attachMedia(video)
else:
  show unsupported browser error
```

Optional but useful controls:

- Show title and thumbnail.
- Show processing status if user reaches watch page too early.
- Show available variants for debugging.

## Backend API Draft

```txt
POST   /videos/upload
GET    /videos
GET    /videos/:id
GET    /videos/:id/playback
GET    /health
```

### `POST /videos/upload`

Request:

```txt
Content-Type: multipart/form-data

fields:
  title: string
  description: string optional
  file: mp4 file
```

Response:

```json
{
  "id": "uuid",
  "title": "Demo video",
  "status": "queued",
  "createdAt": "2026-05-29T00:00:00Z"
}
```

Important behavior:

- Validate file type and size.
- Create video row.
- Upload raw object.
- Create job row.
- Publish RabbitMQ message.
- Return `202 Accepted` or `201 Created`.

### `GET /videos`

Response:

```json
{
  "items": [
    {
      "id": "uuid",
      "title": "Demo video",
      "status": "ready",
      "thumbnailUrl": "http://...",
      "createdAt": "2026-05-29T00:00:00Z"
    }
  ]
}
```

### `GET /videos/:id`

Response:

```json
{
  "id": "uuid",
  "title": "Demo video",
  "description": "Optional text",
  "status": "processing",
  "durationSeconds": 123.45,
  "thumbnailUrl": null,
  "errorMessage": null,
  "variants": [
    {
      "quality": "720p",
      "width": 1280,
      "height": 720,
      "bitrate": 2800000
    }
  ],
  "createdAt": "2026-05-29T00:00:00Z",
  "updatedAt": "2026-05-29T00:00:00Z"
}
```

### `GET /videos/:id/playback`

Response when ready:

```json
{
  "videoId": "uuid",
  "hlsUrl": "http://localhost:9000/mediaflow-processed/processed-videos/uuid/master.m3u8?...",
  "expiresAt": "2026-05-29T01:00:00Z"
}
```

Response when not ready:

```json
{
  "error": "video_not_ready",
  "status": "processing"
}
```

Possible later APIs:

```txt
POST   /videos/:id/retry
DELETE /videos/:id
GET    /videos/:id/events
```

## API Error Shape

Use one predictable JSON error response:

```json
{
  "error": {
    "code": "invalid_file_type",
    "message": "Only MP4 uploads are supported in the MVP."
  }
}
```

Recommended HTTP status mapping:

| Case | Status |
| --- | --- |
| Invalid input | 400 |
| File too large | 413 |
| Unsupported file type | 415 |
| Video not found | 404 |
| Video not ready | 409 |
| Internal error | 500 |
| Dependency unavailable | 503 |

## Docker Compose Services

Required local services:

```txt
postgres
  port: 5432
  database: mediaflow
  user: mediaflow
  password: mediaflow

rabbitmq
  ports: 5672, 15672
  management UI: http://localhost:15672
  user: mediaflow
  password: mediaflow

redis
  port: 6379

minio
  ports: 9000, 9001
  console: http://localhost:9001
  access key: mediaflow
  secret key: mediaflow-secret
```

Buckets to create:

```txt
mediaflow-raw
mediaflow-processed
mediaflow-thumbnails
```

## Observability And Debugging

Minimum useful logs:

- API startup config summary with secrets redacted.
- Upload request started.
- Upload stored to MinIO.
- DB video row created.
- Queue job published.
- Worker message received.
- Worker ffprobe result summary.
- Worker FFmpeg command started.
- Worker HLS output uploaded.
- Worker job completed.
- Worker job failed with error.

Minimum useful events in DB:

```txt
video.upload.started
video.upload.completed
video.job.queued
video.processing.started
video.probe.completed
video.thumbnail.generated
video.hls.generated
video.processing.completed
video.processing.failed
```

## Testing Plan

### Manual Smoke Test

1. Start Docker Compose.
2. Start API.
3. Start worker.
4. Start web app.
5. Upload a small MP4.
6. Confirm DB row is `queued`.
7. Confirm RabbitMQ message is consumed.
8. Confirm status changes to `processing`.
9. Confirm processed HLS files exist in MinIO.
10. Confirm status changes to `ready`.
11. Open watch page.
12. Confirm playback works.
13. Open browser devtools network tab.
14. Confirm `.m3u8` and `.ts` files are being loaded.

### API Tests

Test cases:

- Health check returns OK.
- Upload rejects missing file.
- Upload rejects unsupported content type.
- Upload creates video row.
- Upload publishes queue message.
- `GET /videos/:id` returns status.
- Playback endpoint rejects non-ready videos.

### Worker Tests

Test cases:

- Worker can parse a valid job payload.
- Worker marks job failed for missing raw object.
- Worker runs ffprobe on sample input.
- Worker generates thumbnail.
- Worker generates HLS output.
- Worker writes variants.
- Worker is idempotent when video is already `ready`.

### End-To-End Test

Use a tiny sample MP4 committed under a test fixture folder only if licensing is safe. Otherwise document how to generate one:

```bash
ffmpeg -f lavfi -i testsrc=size=1280x720:rate=30 \
  -f lavfi -i sine=frequency=1000:sample_rate=48000 \
  -t 10 \
  -c:v libx264 \
  -c:a aac \
  sample.mp4
```

## Build Order

1. Initialize repo structure.
2. Add Docker Compose for PostgreSQL, RabbitMQ, Redis, and MinIO.
3. Add database migrations.
4. Build Go API skeleton with health check.
5. Add MinIO client and upload endpoint.
6. Save video metadata to PostgreSQL.
7. Publish RabbitMQ transcode job.
8. Build worker skeleton that consumes jobs.
9. Add FFmpeg/ffprobe integration.
10. Generate HLS files locally.
11. Upload processed files to MinIO.
12. Update DB status and variants.
13. Build Next.js upload page.
14. Build processing status page.
15. Build watch page with `hls.js`.
16. Add error handling, retries, and job logs.

## Milestone Breakdown

### Milestone 0: Repo And Infra

Done when:

- Repo folders exist.
- Docker Compose starts Postgres, RabbitMQ, Redis, and MinIO.
- Buckets are created.
- Migrations run.
- README has local startup commands.

### Milestone 1: API Upload Path

Done when:

- `POST /videos/upload` accepts MP4.
- Raw object appears in MinIO.
- Video row appears in DB.
- Job row appears in DB.
- RabbitMQ message is published.

### Milestone 2: Worker Transcoding Path

Done when:

- Worker consumes the job.
- FFprobe metadata is saved.
- HLS output is generated.
- Thumbnail is generated.
- Processed files are uploaded.
- Video status becomes `ready`.

### Milestone 3: Web Playback Path

Done when:

- Web app uploads video.
- Status page shows progress.
- Watch page plays HLS stream.
- Browser network panel shows manifest and segment requests.

### Milestone 4: Hardening

Done when:

- Failed transcodes show useful errors.
- Retry behavior is defined.
- Logs are readable.
- Large files are rejected cleanly.
- No transcoding happens inside API request path.

## Success Criteria For MVP

- A local user can upload an MP4.
- The API returns quickly after upload and does not transcode synchronously.
- RabbitMQ contains and dispatches the processing job.
- Worker generates HLS output with multiple qualities.
- `master.m3u8` is playable from the frontend.
- Player automatically switches quality using HLS adaptive bitrate behavior.
- Video status is visible as `queued`, `processing`, `ready`, or `failed`.

## Acceptance Checklist

- [ ] `docker compose up` starts required dependencies.
- [ ] Migrations create required tables.
- [ ] API health check works.
- [ ] Upload endpoint stores raw MP4 in MinIO.
- [ ] Upload endpoint publishes `video.transcode`.
- [ ] Worker receives and acknowledges jobs.
- [ ] Worker updates status to `processing`.
- [ ] Worker creates thumbnail.
- [ ] Worker creates `master.m3u8`.
- [ ] Worker creates at least two quality variants.
- [ ] Worker uploads HLS output to MinIO.
- [ ] Worker updates status to `ready`.
- [ ] Playback endpoint returns HLS URL.
- [ ] Frontend upload page works.
- [ ] Frontend status page works.
- [ ] Frontend watch page works.
- [ ] Failed processing updates DB with an error message.
- [ ] README explains how to run the MVP locally.

## Key Engineering Rules

- Do not stream raw MP4 as the primary playback strategy.
- Do not transcode inside the upload request.
- Do not build production microservices before the local pipeline works.
- Keep the first version observable: clear logs, status fields, and failure messages.
- Prefer working end-to-end over building isolated pieces that cannot be tested together.

## Later Scaling Topics

- Resumable uploads with tus.io or multipart upload.
- CDN in front of processed HLS files.
- Worker autoscaling based on queue depth.
- Dead letter queues.
- Raw video lifecycle policies.
- GPU-accelerated transcoding.
- Kubernetes deployment.
- Analytics events.
- AI moderation and subtitles.
- Recommendation system.

## Production Evolution Path

After MVP, grow in this order:

1. Add auth and ownership.
2. Add resumable uploads.
3. Move from local MinIO to S3-compatible cloud storage.
4. Add CDN in front of processed HLS assets.
5. Add worker autoscaling.
6. Add dead-letter queues and retry backoff.
7. Add analytics events.
8. Add comments/likes.
9. Add AI subtitles and moderation.
10. Add recommendation service.
11. Add Kubernetes deployment.
12. Add livestreaming or low-latency streaming only if explicitly needed.

## Open Decisions

Resolve these during implementation:

- Worker language: Go keeps the stack consistent; Python may be faster for FFmpeg scripting. Default to Go unless FFmpeg orchestration becomes awkward.
- Playback URL strategy: presigned MinIO URLs are easiest locally. API proxy is useful if we want full access control later.
- Migration tool: choose a simple Go-friendly tool such as Goose or golang-migrate.
- SQL access: choose between `database/sql`, `sqlc`, or an ORM. Prefer `sqlc` or `database/sql` for clarity in a systems portfolio project.
- Monorepo tooling: keep minimal unless the frontend/backend integration needs shared generated types.
