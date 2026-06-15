# ADR 0004: Retries, Backoff, and the Dead-Letter Queue

- Status: Accepted
- Date: 2026-06-15
- Milestone: 5 (slice C — 5.3)

## Context

Slices A and B guarantee a job is *delivered* and that a *crashed* worker's job
is recovered. But a job can also fail while the worker is perfectly healthy: a
transient blip (MinIO momentarily unreachable, a flaky network read) or a
permanent defect (the upload is corrupt, has no video stream). The MVP handled
every `Process` error the same way — mark the video `failed` and `Nack` without
requeue — so a one-second blip permanently failed a video, and a corrupt upload
looked identical to a recoverable error.

We need: bounded automatic retries with backoff for transient failures, a
terminal resting place for poison/exhausted messages, and a way to tell the two
kinds of failure apart.

## Decision

### Topology (declared by the worker)

- `video.transcode.retry` — a queue with **no consumer**, declared with
  `x-dead-letter-exchange = mediaflow.video` and
  `x-dead-letter-routing-key = video.transcode`. A message published here sits
  until its **per-message TTL** expires, at which point the broker dead-letters
  it back to the main transcode queue.
- `video.transcode.dlq` — terminal storage for poison and exhausted messages,
  bound to the shared exchange on `video.transcode.dlq`. Each message carries an
  `x-failure-reason` header for whoever inspects it.

### Classification

A `job.PermanentError` wrapper marks failures that retrying cannot fix. The
ffprobe stage wraps its two failure modes as permanent — ffprobe rejecting the
input (corrupt/unreadable) and "no video stream found". Everything else is
transient by default.

### Decision logic (pure, unit-tested — `classifyFailure`)

On a `Process` error the worker reads the job's current `attempts` and decides:

- **permanent, or `attempts >= JOB_MAX_ATTEMPTS`** → terminal. Mark job + video
  `failed` (`FailJob`), publish the message to the DLQ with a reason, `Ack`.
- **otherwise** → retry. Backoff TTL is `JOB_RETRY_BASE_DELAY * 2^attempts`
  (e.g. 60s after the first failure, 120s after the second).

### Ordering — keeping the no-stuck guarantee

The retry path is **publish-first**: the retry message is published *with a
publisher confirm* before the DB claim is released. Only after the broker acks do
we flip the job from `processing` to `queued` (re-claimable) via
`MarkQueuedForRetry`, leaving the video `processing` and recording a
`video.job.retry_scheduled` event. If the publish fails, we `Nack(requeue)` and
the job stays `processing` with its lease — so **slice B's reaper remains the
backstop** and nothing is stranded. The re-claim (not the retry bookkeeping)
increments `attempts`, so counting stays in one place.

No new migration: retry state reuses `queued`; terminal/DLQ reuses `failed`.

A separate publisher channel (with confirms) is used for retry/DLQ publishes so
it never contends with the consume channel.

## Consequences

- **Transient failures self-heal** with exponential backoff, bounded by
  `JOB_MAX_ATTEMPTS`; permanent failures fail fast without burning retries.
- **Poison messages can't wedge the consumer**: an unparseable body is acked
  straight to the DLQ, and the consumer keeps draining (covered by a test that
  processes a real job immediately after a poison message).
- **Two recovery mechanisms now coexist**, by design: the reaper handles *crash*
  recovery (immediate requeue via the outbox), while this path handles
  *application-level* failures (backed-off retry via the broker). They don't
  overlap — the reaper only touches `processing` jobs with expired leases; a
  retry-pending job sits in `queued`.
- **Per-message TTL has head-of-line blocking**: a single retry queue evaluates
  TTL only at its head, so a 120s message ahead of a 60s message delays the
  latter. Acceptable at this scale and with only two backoff tiers; the standard
  upgrade is per-tier retry queues (one fixed `x-message-ttl` each), noted as
  future work.
- The retry message is delivered at-least-once like everything else; the
  idempotent claim guard absorbs any duplicate that races the reaper.

### Bug found and fixed

Implementing the terminal path surfaced a latent bug: `FailJob` packed three
statements into a single `ExecContext`, which fails under pgx's extended protocol
(`cannot insert multiple commands into a prepared statement`). No prior test
exercised a terminal failure, so it had never run. Rewritten to a transaction
with separate statements.

## Verification

- Unit: `classifyFailure` backoff/exhaustion/permanent cases and `terminalReason`.
- Integration (testcontainers + ffmpeg):
  - `TestPermanentFailureDeadLettersAndFailsVideo` — bogus raw object → ffprobe
    permanent error → video `failed`, `attempts == 1`, DLQ message tagged
    `permanent failure`.
  - `TestPoisonMessageDeadLettered` — unparseable body → DLQ, and a subsequent
    valid job still reaches `ready` (consumer not wedged).
  - `TestRetryQueueDeadLettersBackToMain` — a message with a short TTL in the
    retry queue is routed back to the main queue by the DLX.
  - `TestMarkQueuedForRetryReleasesClaim` — retry transition resets job to
    `queued`, clears the lease, keeps the video `processing`, records the event.
