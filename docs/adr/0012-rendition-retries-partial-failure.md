# ADR 0012: Per-Rendition Retries and Partial-Failure Cleanup

- Status: Accepted
- Date: 2026-06-22
- Milestone: 7 (slice B — per-stage retries, partial-failure cleanup, aggregation-race proof)

## Context

Slice A (ADR 0011) delivered the fan-out engine: a plan job probes the source and
fans out one `rendition` job per quality, each rendition decrements an atomic
counter on completion, and whoever drives the counter to zero enqueues a
`finalize` job that assembles `master.m3u8`. That slice deliberately deferred the
failure story for the child stages: a rendition or finalize error was left
`processing` with its lease and recovered only by the reaper on lease timeout.

That works, but it is coarse:

- A transient rendition error waited a full lease timeout (minutes) before the
  reaper retried it, with no per-attempt backoff and no DLQ for a poison body.
- A rendition that could never succeed (a permanent error, or repeated transient
  failures) had no path to fail the *video* promptly. Because a doomed rendition
  never decrements the counter, finalize never fires — so without explicit
  cleanup the video would sit `processing` until the reaper eventually exhausted
  the job's attempts. The user-visible video state lagged the reality.

Slice B closes this: the M5 retry/DLQ policy now applies per stage, and a
terminally-failed rendition immediately fails the whole video and cleans up its
siblings. It also proves the two correctness properties M7 calls out: the
aggregation counter never double-finalizes, and a rendition failure converges to
`failed` rather than a stuck state.

## Decision

### Per-stage retry queues

Each stage gets its own retry queue, all on the existing `mediaflow.video`
exchange:

| Stage     | Main queue        | Retry queue (TTL → dead-letter)                 |
| --------- | ----------------- | ------------------------------------------------ |
| plan      | `video.transcode` | `video.transcode.retry` → `video.transcode`      |
| rendition | `video.rendition` | `video.rendition.retry` → `video.rendition`      |
| finalize  | `video.finalize`  | `video.finalize.retry` → `video.finalize`        |

A retry queue has no consumer: a republished message waits out its per-message
TTL, then the broker dead-letters it back to that stage's *own* main queue. This
is the key property — a retrying rendition re-runs as a rendition and never
restarts the plan, so one quality retrying does not redo the others. Poison and
attempt-exhausted messages from every stage still go to the single shared
`video.transcode.dlq` for inspection.

### One failure path for all three stages

The plan-only `handleFailure` and the reaper-only `handleChildFailure` stub are
replaced by a single `retryOrFail(delivery, stage, err)`. A `stage` descriptor
carries the job/video ids, the stage's retry routing key, and (for renditions)
the parent plan job id. The policy is unchanged from M5:

- **Transient, below max attempts** → publish-first to the stage retry queue
  (confirmed publish) with exponential backoff, then release the DB claim
  (`MarkQueuedForRetry`) and ack. If the publish fails, the message is nacked and
  the reaper remains the backstop.
- **Permanent error or attempts exhausted** → dead-letter the message and fail
  terminally.

The only stage-specific branch is the terminal action: a rendition calls
`FailVideoFromRendition` (below); plan and finalize call the existing `FailJob`
(both already fail the whole video, which is the right outcome — a video with no
plan or no master cannot be served).

### Immediate partial-failure cleanup

`FailVideoFromRendition` runs in one transaction:

1. **Lock the parent plan row** (`SELECT … FOR UPDATE`). This serialises against
   `CompleteRendition`, which decrements the counter (and may enqueue finalize) by
   updating the same plan row. Failing and completing a video can therefore never
   interleave.
2. Mark this rendition job `failed`.
3. **Cancel siblings** still `queued` or `processing` (other renditions and any
   finalize job) by setting them `failed` with a "cancelled: sibling rendition
   failed" note and clearing their lease. A sibling mid-transcode on another
   worker then finds its job is no longer `processing`, so its `CompleteRendition`
   guard no-ops — it never decrements a counter for a doomed video, and the
   reaper ignores a `failed` job.
4. Mark the video `failed` (guarded `status <> 'failed'` for idempotence).
5. Record a `video.processing.failed` event whose metadata names the
   `failedQuality` and lists the `completedRenditions` that finished before the
   failure and are now discarded (their objects are left orphaned; cleanup is a
   later GC concern).

### Why no resurrection is possible

A rendition either *completes* (decrements the counter) or *fails* (never
decrements) — never both, even across retries (a retried rendition that later
succeeds decrements exactly once, guarded by the job's `processing` status). So
the counter only reaches zero when *every* rendition succeeded; finalize cannot
fire for a video that has a terminally-failed rendition. The plan-row lock and the
`status <> 'failed'` guard in `CompleteFinalize` are belt-and-suspenders on top of
this structural guarantee.

## Alternatives considered

- **Keep recovering child failures via the reaper only (slice A behaviour).**
  Rejected: minutes of latency before a retry, no backoff, no DLQ, and no prompt
  video-level failure. The reaper stays as the crash backstop (see the kill
  drill), not the primary error path.
- **A retry queue per stage that dead-letters back to a single shared queue.**
  Rejected: it would re-route a retrying rendition through the plan consumer.
  Per-stage dead-letter targets keep each stage's retries on its own lane.
- **Let every sibling finish, then fail the video at finalize.** Rejected: wastes
  work on a video that is already doomed and leaves the video `processing` far
  longer than necessary. Cancelling in-flight siblings surfaces the failure
  immediately.
- **A dedicated `cancelled` job status.** Rejected: the `job_status` enum already
  has `failed`; the `last_error` note distinguishes a cancellation from a genuine
  failure without a schema change.

## Verification

Automated (run in CI under the `integration` tag, testcontainers + ffmpeg):

- `TestAggregationRaceFinalizesOnce` — two renditions complete concurrently;
  exactly one observes the last decrement, exactly one finalize job and one
  finalize outbox message are written, and the counter lands on zero. This is the
  counter-race proof.
- `TestRenditionTerminalFailureCleansUp` — a doomed rendition fails the video,
  cancels the still-queued sibling, leaves the completed sibling intact, records
  the failed and completed qualities in the event, and enqueues no finalize.
- `TestRenditionExhaustsRetriesFailsVideo` — a rendition with a missing source
  driven through the real broker (max attempts = 1) converges the video to
  `failed` and parks the message in the DLQ tagged "attempts exhausted". This is
  the "converges to failed, never stuck" proof.
- `TestChildRetryQueuesDeadLetterBackToStage` — a message parked in the rendition
  and finalize retry queues dead-letters back to its own stage queue.

Manual kill drill (the crash backstop, distinct from the in-process error path):

```bash
# Terminal 1: bring up infra + two workers against a real source.
docker compose -f infrastructure/docker-compose.yml up -d
WORKER_ID=w1 go run ./cmd/worker &   # from apps/worker
WORKER_ID=w2 go run ./cmd/worker &

# Upload a video, then mid-transcode hard-kill the worker holding a rendition:
kill -9 <pid of the worker logging "rendition downloading raw video">
```

Expected: the killed rendition job stays `processing` until its lease expires;
the reaper requeues it on `video.rendition` (not `video.transcode`) rebuilding the
message from `rendition_spec`; the surviving worker picks it up; the video still
reaches `ready`. The kill path is recovery-by-reaper (the crashed process runs no
cleanup); the retry/DLQ path above handles in-process processing errors.

## Consequences

- A rendition failure now has two recovery mechanisms by design: in-process
  retries with backoff (transient processing errors) and the reaper (worker
  crashes). Both converge every video to `ready` or `failed`.
- Completed renditions of a failed video leave orphaned objects in the processed
  bucket. Acceptable for now; a storage GC pass is future work.
- Slice C remains: `--scale worker=3` parallel-speedup measurement and the
  queue-depth autoscaling experiment (ADR 0013).
