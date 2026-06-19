# ADR 0010: Web Chunked Uploader and Legacy Proxy Retirement

- Status: Accepted
- Date: 2026-06-19
- Milestone: 6 (slice D — web uploader + retire legacy proxy)

## Context

Slices A–C built the server side of presigned multipart ingest: create a
session, hand out per-part presigned URLs, validate and complete, and sweep
abandoned sessions (ADRs 0007–0009). Nothing on the client used it — the web app
still POSTed the whole file to the legacy `POST /videos/upload` proxy, which
streams every byte through the API process. This slice makes the browser the
data plane and removes the proxy from the default surface.

The plan's "done when" for M6 frames the requirements: a 500 MB upload must never
transit the API, killing the tab mid-upload and reloading must resume from the
completed parts, and a tampered part must still fail completion cleanly (already
covered by slice B).

## Decision

### Client upload engine (`apps/web/lib/uploads.ts`)

`uploadFile()` drives the full session lifecycle from the browser:

1. **Create** a session (`POST /uploads`) declaring filename, content type, total
   size, and a client-chosen `partSize`. `choosePartSize` uses 8 MiB parts,
   bumped up (rounded to whole MiB) only when 8 MiB would exceed the 10,000-part
   multipart cap — so part count stays bounded for arbitrarily large files. The
   server recomputes `partCount` from the same `partSize`/`totalSize`, so the two
   sides always agree.
2. **Upload parts** with **bounded concurrency** (default 4 lanes draining a
   shared work queue). Each part fetches a fresh presigned URL
   (`GET /uploads/:id/parts/:n/url`) and PUTs its slice **directly to object
   storage**.
3. **Complete** (`POST /uploads/:id/complete`) with the `(partNumber, etag)` list
   and navigate to the new video.

**Per-part retry**: each part retries up to 4 times with exponential backoff +
jitter, re-issuing the presigned URL each attempt (URLs can expire). A failed
part resets only its own progress counter; sibling lanes keep going. The first
unrecoverable part aborts an internal `AbortController` that the lanes share, so
one fatal error stops in-flight PUTs promptly instead of finishing doomed work.

**XHR, not fetch, for the PUT**: `fetch` exposes no upload-progress events, so
parts go over `XMLHttpRequest` to feed a smooth byte-level progress bar. The
ETag is read from the PUT response header — object storage must expose it via
CORS, which MinIO does by default.

### Resume after reload

The browser cannot persist a `File` across a reload, so resume is identity-based:
on session creation we store `{ sessionId, fileName, fileSize, lastModified,
title }` in `localStorage`. After a reload the form shows an "unfinished upload"
banner; when the user re-selects a file whose name/size/lastModified match the
record, the engine resumes that session — `GET /uploads/:id` returns the parts
object storage already holds, those parts are marked done, and only the missing
parts upload. A non-matching file discards the stale session
(`DELETE /uploads/:id`, best-effort) and starts fresh. Cancel aborts in-flight
PUTs but leaves the session resumable; "discard" aborts it server-side and clears
the record. On successful completion the record is cleared.

### Legacy proxy behind a flag (default off)

`POST /videos/upload` is no longer registered by default. It is gated behind
`ENABLE_LEGACY_UPLOAD` (default `false`); `videos.Handler.RegisterRoutes` takes
the flag and only wires the route when set. The handler/service code stays in the
tree so M10 can flip the flag on for proxy-vs-direct comparison benchmarks. The
web app no longer calls it at all (`uploadVideo` remains in `lib/api.ts` unused,
for the same benchmark purpose).

## Consequences

- Video bytes flow browser → object storage; the API only ever sees control JSON,
  satisfying the "500 MB never transits the API" goal at any size.
- An interrupted upload resumes from the server's view of stored parts — no
  re-uploading completed chunks, no client-side bookkeeping of which bytes landed.
- The retry/abort wiring keeps a flaky network from wedging an upload and keeps a
  cancel responsive.
- The default deployment has exactly one ingest path (presigned multipart); the
  proxy is opt-in and isolated to a benchmark scenario.
- Resume depends on `lastModified`; a file edited between attempts is correctly
  treated as different. CORS ETag exposure is a hard dependency on the object
  store's config (fine for MinIO; note for any future S3 bucket — set
  `ExposeHeaders: ETag`).

## Verification

- `npm run lint` and `npm run build` (type-check) pass.
- `go vet ./...` and `go test ./...` pass; the legacy-upload handler tests now
  register the route with the flag on.
- Live drill (local stack, 2026-06-19): see PROGRESS.md M6 — a multi-part MP4
  uploaded from the browser with PUTs hitting `:9000` (MinIO) directly, a reload
  mid-upload resumed from the completed parts, and completion drove the video to
  `ready`.
