# ADR 0005: Upload Idempotency and Graceful Shutdown

- Status: Accepted
- Date: 2026-06-16
- Milestone: 5 (slice D — 5.4)

## Context

Two loose ends remained for Milestone 5's "no stuck videos, no surprises"
guarantee:

1. **Duplicate uploads.** A client that retries `POST /videos/upload` after a
   network timeout (not knowing whether the first attempt landed) creates a
   second video, a second transcode job, and a second copy of the bytes. The
   upload needs to be idempotent on a client-supplied key.
2. **Worker shutdown.** On `SIGTERM` (a deploy, a scale-down) the worker passed
   the cancelled context straight into FFmpeg, so a rolling deploy *killed* every
   in-flight transcode. The crash path (`kill -9`) is covered by the reaper; the
   clean path should let the current job finish.

## Decision

### Idempotency-Key on upload

- Migration `000004` adds `videos.idempotency_key` with a **partial unique
  index** (`WHERE idempotency_key IS NOT NULL`).
- The handler reads the `Idempotency-Key` header and passes it through. The
  service:
  - **Fast path** — if a video already exists for the key, return it without
    re-uploading bytes or creating anything (`200 OK`).
  - **Create path** — store the raw object and insert the video/job/outbox row
    with the key (`201 Created`).
  - **Race path** — if two requests with the same key run concurrently, both may
    pass the fast-path check and try to insert; the unique index lets one win.
    The loser catches the unique violation (SQLSTATE `23505` →
    `ErrDuplicateKey`), looks the winner up, and returns it (`200`). The loser's
    already-uploaded raw object is orphaned — harmless, and rare.
- The service now returns a `created bool` so the handler can distinguish `201`
  (new) from `200` (replay).

### Graceful shutdown

`Worker.Run` now separates two contexts:

- The **shutdown signal** (`ctx`, cancelled on SIGTERM) — observed once, then a
  watcher goroutine cancels the AMQP consumer (`channel.Cancel`) so no new
  deliveries arrive. With prefetch = 1 there is at most one job in flight.
- A **job context** (`jobCtx`, derived from `context.Background()`) passed into
  `handleDelivery`/FFmpeg, so the in-flight job is *not* cancelled by SIGTERM.

The watcher then waits up to `WORKER_SHUTDOWN_GRACE` (default 30s): if the job
finishes first, the consume channel closes and `Run` returns cleanly; if the
grace elapses, the watcher cancels `jobCtx` (killing FFmpeg) and the reaper
recovers the job on a later run. The lease heartbeat keeps running during the
grace window, so the reaper won't reclaim a job that is still finishing.

### Worker retry hygiene (already in place)

The plan's third item was largely satisfied by earlier slices and is now covered
by tests: claiming refuses a video that is already `ready` (idempotent
re-delivery is a no-op), `CompleteJob` deletes stale `video_variants` before
re-inserting, and output object keys are deterministic
(`processed-videos/{videoId}/…`) so a retry overwrites partial output.

## Consequences

- Upload is safely retryable by clients; at-least-once delivery from clients no
  longer produces duplicate videos.
- Rolling deploys no longer abort transcodes within the grace window; only jobs
  that exceed the grace (or a `kill -9`) fall back to the reaper.
- `created`/`200`-vs-`201` gives clients a clear signal whether their key was a
  replay.
- The race-path orphaned raw object is accepted as negligible garbage; a future
  janitor (or the M6 multipart work) can sweep orphans.
- Shutdown grace is a tradeoff: long enough to finish typical jobs, short enough
  to respect an orchestrator's kill deadline. 30s default; a very long transcode
  will be aborted and retried rather than holding up the deploy.

## Verification

- Unit (API): `TestUploadReplaysIdempotencyKey` — same key twice → `201` then
  `200`, exactly one create, same video id.
- Integration (API, real Postgres unique index): `TestUploadIdempotencyKeyReplays`
  — replay returns the original, exactly one `videos` row.
- Integration (worker): `TestGracefulShutdownFinishesInFlightJob` — shutdown
  signalled mid-transcode, the job still reaches `ready` and the worker exits;
  `TestRedeliveryOfReadyVideoIsSkipped` — re-delivery for a `ready` video does no
  work and leaves variants untouched.
