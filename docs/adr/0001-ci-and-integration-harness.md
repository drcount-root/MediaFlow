# ADR 0001: CI and Integration Test Harness

- Status: Accepted
- Date: 2026-06-13
- Milestone: 4

## Context

Everything from Milestone 5 onward (transactional outbox, job leases, reaper,
retry/DLQ routing, fan-out transcoding) is infrastructure-coupled code whose
correctness cannot be proven with unit tests against fakes. We need a pipeline
that exercises the real Postgres, RabbitMQ, and MinIO behaviour on every change,
and a single command developers can run locally that matches what CI runs.

The repo is a polyrepo-in-a-monorepo: `apps/api` and `apps/worker` are separate
Go modules and `apps/web` is a standalone npm package. There is no root build
system.

## Decision

**CI: GitHub Actions** (`.github/workflows/ci.yml`), one job per unit of work so
failures are isolated and runs parallelise:

- `api` — `gofmt -l` gate, `go vet ./...`, `go test ./...`.
- `worker` — same, with `ffmpeg`/`ffprobe` installed in the runner (the worker
  shells out to them).
- `web` — `npm ci`, `npm run lint`, `npm run build` (build also type-checks).
- `integration-api` and `integration-worker` — `go test -tags integration ./...`.

Dependency caching uses the built-in caches of `actions/setup-go` (keyed on each
module's `go.sum`) and `actions/setup-node` (keyed on `package-lock.json`). Go
versions come from each module's `go.mod` via `go-version-file`, so CI never
drifts from the toolchain the modules declare.

**Integration tests: testcontainers-go**, gated behind the `integration` build
tag so the default `go test ./...` stays hermetic and fast. Each module has an
`integration/` package whose `TestMain` starts real `postgres:16-alpine`,
`rabbitmq:3.13-management-alpine`, and `minio/minio:latest` containers once,
applies `infrastructure/migrations/*.sql`, and creates the three buckets — then
shares them across the package's tests, which reset only the state they touch.

Coverage targets:

- **Repository against real Postgres** (`apps/api`): `CreateQueuedVideo` commits
  the video + job + lifecycle events in one transaction; `GetVideo`/`ListVideos`/
  `GetVariants` round-trip.
- **Publish/consume round-trip against real RabbitMQ** (`apps/api`): the
  production `RabbitPublisher` publishes a `TranscodeJob`; a raw consumer reads it
  back and asserts the wire contract.
- **Upload → store → queue** (`apps/api`): the real `videos.Service` against live
  MinIO + Postgres + RabbitMQ — raw object stored, video queued, job published.
- **Queue → process → ready** (`apps/worker`): the headline end-to-end test. A
  fixture MP4 is generated on the fly with `ffmpeg -f lavfi -i testsrc ...`,
  uploaded as the raw object, the queued rows are seeded, a job is published, and
  the real `worker.Worker` consumes it, transcodes to HLS, and leaves the video
  `ready` with variant rows and the master playlist + thumbnail in MinIO.

## Consequences

- The suite touches real Postgres and RabbitMQ, so the M5 dual-write bug it is
  meant to guard against (DB write succeeds, publish fails, no outbox) is exactly
  the kind of failure these tests *can* observe — the design goal of this
  milestone.
- Media files are never committed; fixtures are produced in `t.TempDir()` and
  discarded. `.gitignore` already excludes `*.mp4`/`*.m3u8`/`*.ts`.
- Local command mirrors CI: `go test -tags integration ./...` per module. It
  requires a running Docker daemon and, for the worker suite, `ffmpeg`/`ffprobe`
  on `PATH`. The worker pipeline test skips (not fails) when ffmpeg is absent.
- testcontainers pulls a large transitive dependency tree (Docker client, etc.)
  into both Go modules. Accepted — it is test-only and the value is real
  end-to-end coverage.
- The full upload→ready flow is exercised across the module boundary by seeding
  the queued rows the API would have written, rather than importing the `api`
  module into the `worker` module (which would couple the two). The API's own
  store→queue half is covered separately in `apps/api/integration`.

## Open / follow-ups

- "CI required for PRs; main branch green" still needs the workflow to run on
  GitHub once and a branch-protection rule added — both are GitHub-side settings,
  not code.
