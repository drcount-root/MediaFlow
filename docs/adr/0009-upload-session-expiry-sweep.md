# ADR 0009: Upload Session Expiry Sweep

- Status: Accepted
- Date: 2026-06-18
- Milestone: 6 (slice C — expiry sweep)

## Context

Slices A and B (ADRs 0007, 0008) let a client create an upload session, PUT
parts directly to MinIO via presigned URLs, and complete the upload. But not
every session completes: a client can close the tab, lose the network, or simply
abandon the upload after staging some parts. Each abandoned session leaves two
pieces of garbage behind:

1. A row stuck in `pending`/`uploading` forever — `expires_at` already records a
   deadline (ADR 0007), but nothing acts on it.
2. An **open MinIO multipart upload** whose staged parts occupy storage
   indefinitely. Multipart parts are not visible as objects and are not reclaimed
   by ordinary object expiry — they linger until the multipart upload is aborted
   or completed.

The completion path (ADR 0008) also noted that its race window and tampered-part
rejections can leave staged parts behind, deferring their reclamation to this
slice.

## Decision

A background **sweeper** runs for the life of the API process (alongside the
outbox relay), on a ticker (`UPLOAD_SWEEP_INTERVAL`, default 5m). Each pass
drains in batches (`UPLOAD_SWEEP_BATCH_SIZE`, default 100) until no expired
sessions remain, mirroring the relay's shape.

### What counts as expired

A session is swept when `status IN ('pending','uploading') AND expires_at < now()`.
Terminal sessions (`completed`, `aborted`, already `expired`) are never touched.
The predicate is served by the existing `idx_upload_sessions_expires_at` index —
**no schema change** was needed; the `expired` enum value and the index already
shipped in migration `000005`.

### Claim-first ordering (safe against a concurrent completion)

For each expired candidate the sweeper:

1. **Claims** it with a guarded `UPDATE upload_sessions SET status='expired'
   WHERE id=$1 AND status IN ('pending','uploading')`. The bool return reports
   whether this call won the transition.
2. Only on winning does it call `AbortMultipart` to release the MinIO upload.

This is the same atomic-claim idiom used for job leases and `CompleteSession`.
If a completion lands between the list and the claim, the guarded `UPDATE`
affects zero rows, the sweeper skips the abort, and the completion owns the
(now finalized) multipart upload — the sweeper never aborts an upload another
path is using.

### Abort failures do not revert the claim

If `AbortMultipart` fails after a successful claim, the sweeper logs a warning
and moves on; it does **not** revert the row to `pending`. Leaving it `expired`
keeps the sweep from spinning on the same session every tick. The rare orphaned
multipart this can leave is the backstop's job: a MinIO incomplete-multipart
lifecycle rule (operational config) is the safety net for uploads the sweeper
can't reach. Keeping the abort error out of the `uploads` package's type system
also preserves the package's storage-agnostic boundary (no MinIO error codes
leak in).

## Consequences

- Abandoned uploads converge to `expired` with their staged parts released —
  storage no longer leaks on every abandoned upload.
- Completion stays the source of truth for sessions it owns; the sweeper is a
  best-effort reclaimer that can never race a live completion into a bad state.
- `SweepOnce(ctx)` is exported, so tests drive a single deterministic pass and a
  future ops endpoint could trigger an on-demand sweep.
- Truly orphaned multipart uploads with no session row (e.g. an `InitiateMultipart`
  that succeeded but whose `CreateSession` insert then failed) are out of scope
  here and left to the MinIO lifecycle rule.

## Verification

- Unit (`internal/uploads`): `TestSweepExpiresOpenSessionsAndAbortsMultipart`
  (expired pending+uploading sessions flip to `expired` and each multipart is
  aborted); `TestSweepLeavesUnexpiredAndTerminalSessions` (fresh and
  completed/aborted sessions untouched); `TestSweepSkipsAbortWhenSessionRacedAway`
  (a completion winning the claim race means no abort).
- Integration (real MinIO + Postgres via testcontainers):
  `TestUploadSweepExpiresAbandonedSession` stages a real part via a presigned
  URL, backdates `expires_at`, runs `SweepOnce`, and asserts the session is
  `expired`, the part listed before the sweep, the multipart upload gone after
  (its `ListObjectParts` now errors), and a second pass is a no-op.
