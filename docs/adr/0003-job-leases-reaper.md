# ADR 0003: Job Leases and the Reaper

- Status: Accepted
- Date: 2026-06-13
- Milestone: 5 (slice B — 5.2)

## Context

After slice A (the transactional outbox) a `TranscodeJob` is reliably delivered
to a worker, and the worker claims it with a guarded `UPDATE` so duplicate
deliveries are skipped. But a claim is permanent: once a job flips to
`processing`, nothing reverses it if the worker dies mid-transcode. A `kill -9`,
an OOM, or a hung FFmpeg leaves the job stuck in `processing` and the video
stuck out of `ready`/`failed` forever — exactly the "stuck video" the project's
engineering rules forbid (*every video state must have a path to `ready` or
`failed` via retry, timeout, or reaper*).

We need a liveness signal on an in-flight claim and a process that acts when the
signal stops.

## Decision

Add **time-bounded leases** to claims and a **reaper** that recovers expired
ones.

- Migration `000003` adds `claimed_by TEXT` and `lease_expires_at TIMESTAMPTZ`
  to `video_jobs`, plus a partial index `(lease_expires_at) WHERE status =
  'processing'` for the reaper scan.
- **Claim** (`ClaimJob`) now stamps `claimed_by = <worker id>` and
  `lease_expires_at = now() + lease` alongside the existing
  `status = 'processing'` / `attempts + 1`, still guarded to
  `status IN ('queued','failed')`. `WORKER_ID` defaults to `<hostname>-<pid>`.
- **Heartbeat** (`Heartbeat`): while FFmpeg runs, the worker extends its lease on
  a ticker. The `UPDATE` is guarded by `claimed_by = $worker AND status =
  'processing'`, so a worker that has lost its claim (the reaper already requeued
  it) extends zero rows and silently stops mattering. The heartbeat goroutine is
  tied to a context cancelled when `Process` returns.
- **Reaper** (`internal/reaper`): on a ticker, scans for
  `status = 'processing' AND lease_expires_at < now()` using
  `FOR UPDATE OF j SKIP LOCKED LIMIT n`, so reapers on every worker run
  concurrently without fighting over the same rows. For each expired job, in one
  transaction:
  - **below `JOB_MAX_ATTEMPTS`** → reset job + video to `queued`, clear
    `claimed_by`/`lease_expires_at`, write a `TranscodeJob` **outbox row** for the
    relay to publish (no direct broker write — the same rule as slice A), and
    record a `video.job.requeued` event. Attempts are *not* incremented here; the
    re-claim counts the next attempt.
  - **at `JOB_MAX_ATTEMPTS`** → mark job + video `failed` with an error message
    and a `video.processing.failed` event. No outbox row.
- The reaper runs as a goroutine in the worker process next to consumption and
  shuts down with it.
- Config: `JOB_LEASE_SECONDS` (default `120`), `JOB_HEARTBEAT_INTERVAL`
  (`30s`), `JOB_MAX_ATTEMPTS` (`3`), `REAPER_INTERVAL` (`30s`), `WORKER_ID`.

The lease duration must comfortably exceed the heartbeat interval (4× here) so a
brief GC pause or slow tick never lets a live worker's lease lapse.

## Consequences

- **Every in-flight job now has a path out.** A dead worker's lease expires and
  the reaper either re-drives the job (via the outbox, so it flows back through
  the normal consume path) or fails it after `JOB_MAX_ATTEMPTS`. No stuck videos.
- **Recovery composes with the outbox.** The reaper never publishes to RabbitMQ
  directly; it writes an outbox row and the existing relay handles delivery with
  confirms. One delivery mechanism, one set of guarantees.
- **At-least-once is still the contract.** A requeue can race a worker that was
  merely slow (lease lapsed but the process is alive). That worker's `Complete`
  still wins or loses cleanly: its heartbeat/complete `UPDATE`s are guarded on
  `claimed_by`, and the re-claim is guarded on status, so at most one outcome
  sticks. Consumers remain idempotent.
- **Reaper is horizontally safe.** `SKIP LOCKED` means N workers each run a
  reaper without coordination; the batch loop drains a backlog over successive
  ticks.
- Lease/heartbeat tuning is a latency-vs-recovery-time tradeoff: a longer lease
  tolerates longer pauses but delays recovery of a genuinely dead worker.

## Verification

- Integration tests (`apps/worker/integration/leases_test.go`):
  `TestClaimStampsLeaseAndAttempt`, `TestHeartbeatExtendsLease` (including the
  non-owning-worker no-op), `TestReaperRequeuesExpiredLeaseBelowMax` (asserts the
  outbox row, event, and cleared claim), `TestReaperFailsAtMaxAttempts` (asserts
  no outbox row).
- Failure drill: with infra up, upload a video, `kill -9` the worker mid-FFmpeg,
  and confirm the lease expires, the reaper requeues the job, and a freshly
  started worker drives the video to `ready`.
