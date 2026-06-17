# ADR 0008: Upload Completion — Validation and Enqueue

- Status: Accepted
- Date: 2026-06-17
- Milestone: 6 (slice B — completion + validation + enqueue)

## Context

Slice A (ADR 0007) gave clients a way to upload parts directly to object storage
via presigned multipart. This slice finalizes an upload: validate what landed,
assemble the object, and hand off to the existing transcode pipeline — without
ever streaming bytes through the API.

The two correctness questions completion must answer:

1. **Did the right bytes land?** A part could be truncated, re-uploaded with
   different content, or missing entirely. Completion must reject a bad upload
   cleanly (no video, no job) and leave the session resumable.
2. **Can completion be lost or duplicated?** The video/job/outbox rows and the
   session's completed-state must move together; a client retrying `complete`
   after a timeout must not create a second video.

## Decision

### `POST /uploads/:id/complete`

Request body is the client's part list: `{ "parts": [{ "partNumber", "etag" }] }`.

The service:

1. Loads the session; refuses anything not `pending`/`uploading` (a
   already-`completed` session is **replayed** — see idempotency below).
2. Lists the parts **object storage actually holds** (`ListObjectParts`).
3. Validates, for every expected part number `1..partCount`:
   - the part exists in storage (else `ErrIncompleteUpload`);
   - the client's declared ETag matches the stored ETag (else
     `ErrChecksumMismatch`). ETags are normalized (quotes stripped,
     lowercased) so a quoted PUT-response header compares equal to the
     unquoted list value.
4. Sums the **stored** part sizes and requires the total to equal the size the
   client declared at session creation (`ErrSizeMismatch`), and to stay within
   `MAX_UPLOAD_BYTES` (`ErrTooLarge`).
5. Finalizes the multipart upload using the **stored** ETags — never a
   client-supplied value — so a forged ETag can fail validation but can never be
   used to assemble the object.
6. Creates the video/job/events/outbox rows and marks the session
   `completed`/linked, all in one transaction.

Validation failures map to `422 Unprocessable Entity`; they finalize nothing and
leave the session resumable.

### One transaction for completion

`CompleteSession` does, in a single DB transaction: insert the `videos` row
(status `queued`), the `video_jobs` row, the lifecycle `video_events`, the
`outbox_messages` row (the M5 transcode enqueue — no dual-write to the broker),
and `UPDATE upload_sessions SET status='completed', video_id=…` guarded by
`WHERE status IN ('pending','uploading')`. Either a session becomes completed
*and* a job is enqueued, or neither happens.

### Idempotent completion

- If the session is already `completed`, the service returns the linked
  `video_id` with `created=false` (`200`); a fresh completion returns `201`.
- If two concurrent completions race, the guarded `UPDATE` lets exactly one win
  (it flips the row out of `pending`/`uploading`); the loser's `affected == 0`
  surfaces as a conflict and rolls back, so only one video is ever created.

### Object key and worker contract

The assembled object stays at `raw-uploads/{sessionId}/original{ext}`. The
transcode job carries that key explicitly (`RawObjectKey`), and the worker
downloads by the key in the message, so no worker change is needed — processed
and thumbnail keys are still derived from the new `videoId`.

## Consequences

- A completed upload flows into the exact same transcode path as the legacy
  proxy upload, reusing all of M5 (outbox, leases, retries, reaper).
- Bad uploads fail fast and cheaply, before any video exists.
- Completion is safe to retry.
- The legacy `POST /videos/upload` remains until the web-uploader slice.
- The race-path and any abandoned sessions leave staged parts in object storage;
  the expiry sweeper (slice C) reclaims them.

## Verification

- Unit (`internal/uploads`): finalize+enqueue happy path; replay returns the same
  video without re-finalizing; checksum mismatch, missing parts, and size
  mismatch each fail without finalizing.
- Integration (real MinIO + Postgres via testcontainers):
  `TestUploadSessionCompleteEnqueuesTranscode` uploads two parts via presigned
  URLs, completes, and asserts a queued video, exactly one outbox row, and a
  completed/linked session; `TestUploadSessionCompleteRejectsTamperedPart`
  forges a part ETag and asserts a clean `ErrChecksumMismatch` with zero videos
  and zero outbox rows, session left resumable.
- Live drill (local stack, 2026-06-17): a 7 MiB MP4 uploaded as two parts whose
  presigned PUTs targeted `localhost:9000` (MinIO) directly — **never the API on
  `:8080`** — then `complete` (`201`); the relay published the job and the worker
  downloaded `raw-uploads/{sessionId}/original.mp4`, transcoded it, and drove the
  video to `ready` with two variants. Outbox drained 1/1; session `completed` and
  linked.
