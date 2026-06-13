# ADR 0002: Transactional Outbox for Job Enqueue

- Status: Accepted
- Date: 2026-06-13
- Milestone: 5 (slice A — 5.1)

## Context

The MVP upload path did a **dual write**: it committed the `videos`/`video_jobs`
rows to Postgres and then, as a separate step, published a `TranscodeJob` to
RabbitMQ. Those two operations are not atomic. If the process crashed (or
RabbitMQ was briefly unavailable) between the commit and the publish, the video
was stranded in `queued` forever with no message to drive it — a stuck video,
which the project's engineering rules forbid. Conversely, publishing before the
commit risks a message referencing a row that never persisted.

This is the canonical "you cannot atomically write to a database and a message
broker" problem, and it is the foundation the rest of Milestone 5 builds on.

## Decision

Adopt the **transactional outbox** pattern.

- New `outbox_messages` table (migration `000002`): `id`, `exchange`,
  `routing_key`, `payload_json`, `created_at`, `published_at`, with a partial
  index on the unpublished rows.
- `Upload` builds the `TranscodeJob`, marshals it, and the repository inserts the
  **video + job + lifecycle events + outbox row in a single transaction**. The
  request path no longer talks to RabbitMQ at all. Either everything commits or
  nothing does.
- A **relay loop** in the API process (`internal/outbox`) polls the table on a
  ticker:
  - `SELECT ... WHERE published_at IS NULL ORDER BY created_at FOR UPDATE SKIP
    LOCKED LIMIT n` — `SKIP LOCKED` lets multiple API instances relay
    concurrently without grabbing the same row.
  - publishes each row with **publisher confirms** (`PublishWithDeferredConfirm`
    + `WaitContext`), and marks it `published_at = now()` only after the broker
    acks — all inside the same transaction. A publish failure rolls the batch
    back, leaving the rows for the next tick.
  - drains full batches until empty so a backlog clears in one tick.
- The relay starts with the API and stops on shutdown (cancel + wait).
- Config: `OUTBOX_POLL_INTERVAL` (default `1s`), `OUTBOX_BATCH_SIZE` (default
  `100`).

## Consequences

- **Delivery is at-least-once.** A crash between the broker ack and the row
  update will republish the message later. Consumers must be idempotent — the
  worker already claims jobs with a guarded `UPDATE` and skips duplicates, and
  slice B (leases) hardens this further.
- The upload path is now decoupled from broker availability: an upload succeeds
  and is durably enqueued even if RabbitMQ is down; the relay drains once it
  returns. (The full "RabbitMQ down then up" restart is exercised as a chaos
  drill; here it is covered structurally — the request path performs zero broker
  I/O, verified by `TestUploadStoresAndWritesOutbox`.)
- Slight added latency (one poll interval) between commit and publish. Acceptable
  for this pipeline; the interval is tunable.
- A long-lived DB transaction spans the network publish within a batch. Fine at
  this scale and bounded by `OUTBOX_BATCH_SIZE`.

## Alternatives considered

- **Publish-then-commit / commit-then-publish (the dual write):** the bug we are
  removing.
- **Change Data Capture (Debezium reading the WAL):** the heavyweight,
  infra-correct version. Documented as the scale-up path; unjustified locally.
- **Listen/Notify to wake the relay instantly** instead of polling: a reasonable
  latency optimization, deferred — the ticker is simpler and sufficient.

## Tests

- Unit: the upload handler writes an outbox row carrying the transcode job
  (no direct publish).
- Integration (testcontainers): `TestUploadStoresAndWritesOutbox` (store + a
  single unpublished outbox row, no broker I/O) and `TestRelayDeliversOutboxToQueue`
  (relay publishes to the real queue with confirms and marks the row published).
