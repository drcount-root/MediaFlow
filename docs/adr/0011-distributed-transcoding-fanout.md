# ADR 0011: Distributed Transcoding — Plan/Rendition/Finalize Fan-Out

- Status: Accepted
- Date: 2026-06-21
- Milestone: 7 (slice A — fan-out engine)

## Context

Through Milestone 6 a video was transcoded by a single monolithic job: one
worker downloaded the source, probed it, made the thumbnail, generated *every*
HLS rendition, wrote `master.m3u8`, and marked the video `ready`. That job is
indivisible — a second worker cannot help with one video, and a transient
failure in the third rendition redoes the first two. Milestone 7 replaces it
with the map-reduce shape real platforms use: split one video's work across N
workers and aggregate the results.

This slice (A) delivers the fan-out engine and proves it correct on a single
worker. Per-stage retry/backoff, the aggregation-race and kill-drill proofs
(slice B), and the multi-worker speedup measurement (slice C) follow.

## Decision

### Job hierarchy

`video_jobs` grows three columns (migration `000006`):

- `parent_job_id` — renditions and the finalize job point back at their plan job.
- `pending_renditions` — the aggregation counter, stamped on the plan job.
- `rendition_spec` (JSONB) — the single quality a rendition must produce.

`job_type` (free-text, no enum) now takes `plan | rendition | finalize`. Ingest
stamps the job it enqueues as `plan` (`videos.JobTypePlan`); legacy `transcode`
rows are untouched and the reaper treats them as plan jobs.

Three queues bound on the existing `mediaflow.video` exchange:
`video.transcode` (plan, unchanged routing key), `video.rendition`,
`video.finalize`. A single worker process consumes all three.

### The three stages

1. **Planner** (`video.transcode`): claims the plan job (M5 lease + heartbeat),
   downloads the source, probes it, generates and uploads the thumbnail, picks
   the rendition ladder from the source height, then **fans out** — in one
   transaction it inserts one `rendition` job + one outbox message per quality,
   stamps `pending_renditions = N` on the plan job, and marks the plan job
   `completed`. It does not transcode.

2. **Rendition worker** (`video.rendition`): claims one rendition job, downloads
   the source, transcodes exactly one quality, uploads its
   `{quality}/index.m3u8` + segments, then in one transaction upserts its
   `video_variants` row and **atomically decrements** the plan's counter
   (`UPDATE ... RETURNING pending_renditions`). The worker that drives the
   counter to zero inserts the `finalize` job + outbox message in that same
   transaction.

3. **Finalizer** (`video.finalize`): reads the recorded variants, builds
   `master.m3u8` referencing them (highest bitrate first), uploads it, and marks
   the video `ready`.

### Everything goes through the outbox

The planner's fan-out, the rendition→finalize hand-off, and the reaper's
requeues all write `outbox_messages` rows inside the same transaction as their
state change — never a direct publish. The existing API relay (content-agnostic:
it publishes whatever `exchange`/`routing_key`/`payload_json` it finds) drains
them. This preserves the M5 invariant: no dual-write of DB-then-broker.

### Idempotency and the aggregation race

At-least-once delivery means every stage must tolerate redelivery:

- **Fan-out** is guarded by `WHERE id = $plan AND status = 'processing'`. A
  redelivered plan message whose fan-out already committed claims nothing (the
  plan job is `completed`) and is skipped; a fan-out that lost its claim to the
  reaper rolls back wholesale, so rendition rows never escape without a counter.
- **Counter decrement** happens only when the rendition transition
  `processing → completed` actually affects a row. A duplicate rendition
  delivery re-runs the idempotent variant upsert but does **not** decrement
  again, so the counter cannot underflow and finalize fires exactly once.
- **Two renditions finishing simultaneously** serialize on the plan row's update
  lock: `2 → 1` then `1 → 0`. Only the transaction that reads `0` enqueues
  finalize. No double-finalize.

### Failure handling (slice A)

The plan stage keeps the M5 retry/DLQ machinery (transient → `video.transcode.retry`
with TTL backoff; permanent/exhausted → DLQ + video `failed`).

Rendition and finalize stages do **not** yet have their own retry queues. On an
explicit error the worker drops the delivery (Nack, no requeue) and leaves the
job `processing` with its lease; the **reaper** recovers it on the lease timeout
— requeuing to the correct queue (rebuilt from `rendition_spec`) below max
attempts, or failing the video at max. Dropping the delivery avoids a hot loop;
the reaper is the backstop. This satisfies the invariant that every video has a
path to `ready` or `failed`. Per-stage backoff retry and immediate partial-
failure cleanup are slice B.

The reaper is now job-type-aware: a plan requeue restarts the pipeline and
returns the video to `queued`; a rendition/finalize requeue leaves the video
`processing` (it is mid-fan-out).

## Consequences

- One video's renditions can now run on different workers in parallel — the
  point of the milestone. Each rendition independently downloads the source
  (re-probing locally for the audio flag) so it is self-contained from just the
  raw key + spec; the reaper can rebuild its message with no in-flight context.
  The cost is N source downloads per video, acceptable against object storage.
- The plan job becomes a pure planner + aggregation anchor; the heavy lifting is
  the rendition jobs.
- Graceful shutdown now commits the in-flight **stage** (the rendition finishes
  and records its variant) rather than the whole video; downstream stages resume
  via the queue/reaper.

## Alternatives considered

- **Inline finalize** (the last rendition writes the master playlist itself)
  instead of a separate `finalize` job. Rejected: a separate job gets its own
  lease/retry/reaper coverage, so a master-playlist write failure recovers
  independently without re-running renditions.
- **Counting rendition rows by status** instead of a counter column. Rejected:
  the `UPDATE ... RETURNING` counter is a single atomic row operation with an
  obvious race story; counting invites read-modify-write windows.
- **Carrying `HasAudio` in the rendition message / spec.** Rejected in favor of
  a local re-probe so the spec stays purely about the output and the reaper
  rebuild needs no source metadata.

## Verification

Integration tests against real Postgres/RabbitMQ/MinIO + ffmpeg (the suite runs
a test relay standing in for the API relay):

- `TestFanOutProducesAllRenditions` — a 720p source fans out to three rendition
  jobs, aggregates to zero, finalizes, and reaches `ready` with three variants
  and a 3-stream master playlist ordered by bitrate; the plan job ends
  `completed` with `pending_renditions = 0`.
- `TestPipelineProcessesUploadToReady` — the end-to-end happy path (single
  rendition source).
- `TestGracefulShutdownFinishesInFlightRendition` — SIGTERM mid-transcode lets
  the in-flight rendition finish and record its variant, then the worker exits
  cleanly.
- `TestPoisonMessageDeadLettered`, the lease/retry suites — unchanged plan-stage
  guarantees still hold under fan-out.

The live multi-worker speedup drill (`--scale worker=3`) and the per-rendition
retry / aggregation-race / kill-drill proofs land in slices B and C.
