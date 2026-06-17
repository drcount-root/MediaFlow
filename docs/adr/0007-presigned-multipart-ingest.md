# ADR 0007: Presigned Multipart Ingest (Control Plane)

- Status: Accepted
- Date: 2026-06-17
- Milestone: 6 (slice A — upload sessions + presigned parts)

## Context

Milestone 6 turns the API into a control plane: video bytes must flow directly
between the browser and object storage, never through the API process. Today's
`POST /videos/upload` streams the whole file through the API into MinIO — fine
for the MVP, but it caps upload size at the API's memory/timeouts, can't resume,
and makes the API a throughput bottleneck.

S3/MinIO multipart upload is the mechanism: the client splits the file into
parts, uploads each part directly to object storage, and the upload is finalized
by listing the parts. The API's job is to broker this safely — initiate the
upload, hand out short-lived presigned URLs, track progress for resume, and
(later slice) validate and enqueue on completion.

This ADR covers slice A: the session lifecycle and presigned part URLs.
Completion/validation and the expiry sweeper are separate slices.

## Decision

### `upload_sessions` table (migration 000005)

One row per upload, holding everything needed to issue part URLs, report
progress, and later validate completion: the MinIO `upload_id`, declared
`total_size`/`part_size`/`part_count`, content type, optional client SHA-256, a
`status` enum (`pending | uploading | completed | aborted | expired`),
`expires_at`, and a `video_id` (set on completion). The object the parts write to
is `raw-uploads/{sessionId}/original{ext}` — distinct from the MVP's
`raw-videos/{videoId}/...` because no video row exists yet; the eventual
transcode job carries this key explicitly, so the worker contract is unchanged.

### Endpoints

```txt
POST   /uploads                   # validate, initiate multipart, persist session (201)
GET    /uploads/:id               # session + parts object storage already has (resume)
GET    /uploads/:id/parts/:n/url  # presigned PUT URL for part n
DELETE /uploads/:id               # abort multipart, mark aborted (204)
```

- **Validation at creation.** Title and MP4 type required; `total_size` must be
  positive and within `MAX_UPLOAD_BYTES`; `part_size` positive. Part count is
  derived (`ceil(total/part)`) and bounded by S3's 10,000-part limit. A
  multi-part upload whose `part_size` is below the 5 MiB multipart minimum is
  rejected (the final part may be smaller, but interior parts may not).
- **Initiate-then-persist ordering.** The multipart upload is initiated in MinIO
  *before* the row is written, so a session always has a valid `upload_id`. If
  initiate fails, nothing is persisted. The reverse (orphaned MinIO multipart
  with no row) is what the expiry sweeper in a later slice cleans up.
- **Status transitions.** `pending` → `uploading` on the first issued part URL
  (best-effort; a status-write failure doesn't void the URL already returned) →
  `completed`/`aborted` later. Part URLs are refused once a session leaves the
  `pending`/`uploading` states (`409`).
- **Resume.** `GET /uploads/:id` lists the parts MinIO has actually received
  (number, ETag, size) so a reloaded client re-uploads only what's missing. The
  DB is not the source of truth for which parts landed — object storage is.
- **`upload_id` is server-only.** It is tracked on the session but tagged
  `json:"-"`; clients never need it because part URLs are presigned for it.

### Reuse

Multipart operations are added to the existing `MinIOStorage` via `minio.Core`
(`NewMultipartUpload`, `Presign`, `ListObjectParts`, `AbortMultipartUpload`).
The Postgres repository implements `uploads.Repository` alongside
`videos.Repository` — one repo, two consumer-defined interfaces.

## Consequences

- Uploads no longer transit the API; size is bounded by client/object-store
  limits, not API memory. (Proven end to end once completion lands in slice B.)
- Resume is a property of asking object storage what it has, not bookkeeping the
  API must keep perfectly in sync.
- The legacy `POST /videos/upload` proxy stays for now so existing flows/tests
  keep working; it is retired in the web-uploader slice.
- New knobs: `UPLOAD_SESSION_TTL` (how long a session stays resumable) and
  `UPLOAD_PART_URL_TTL` (presigned URL validity).

## Verification

- Unit (`internal/uploads`): part-count math, validation (empty title, bad
  media, oversize, undersized parts), initiate-fails-no-persist, pending→uploading
  on first part URL, out-of-range part rejection, conflict after abort,
  resume-parts population, idempotent abort, and that `upload_id` never appears
  in the JSON sent to clients.
- Integration (real MinIO + Postgres via testcontainers):
  `TestUploadSessionPresignedPartsRoundTrip` creates a session and PUTs two parts
  **directly to MinIO** via presigned URLs, then sees both reported for resume
  with correct sizes; `TestUploadSessionAbortReleasesMultipart` uploads a part,
  aborts, and confirms the session refuses further part URLs.
