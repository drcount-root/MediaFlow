# ADR 0006: Outbox Relay Reconnects After a Broker Outage

- Status: Accepted
- Date: 2026-06-17
- Milestone: 5 (RabbitMQ-down-during-upload drill)

## Context

Milestone 5's guarantee is "no stuck videos": every video has a path to `ready`
or `failed`. The last M5 item was the **RabbitMQ-down-during-upload** failure
drill — prove an upload still succeeds while the broker is down, and that the
queued work is delivered once the broker returns.

The upload path already satisfies the first half: `POST /videos/upload` writes
the raw object plus the video/job/outbox rows in one Postgres transaction and
never publishes to RabbitMQ on the request path (ADR 0002). So an upload does not
depend on the broker being up.

Running the drill, however, exposed a real gap in the second half. The
`RabbitPublisher` dialed the broker exactly once at startup and held that single
connection/channel for the process lifetime. When RabbitMQ was stopped and
restarted, the relay kept retrying on the **same dead channel** forever:

```text
level=ERROR msg="outbox drain failed" error="channel/connection is not open"
level=ERROR msg="outbox drain failed" error="channel/connection is not open"
... (indefinitely, even after the broker was healthy again)
```

The outbox row never drained and the video stayed in `queued` with no recovery
path short of bouncing the API. The reaper does not help here — it recovers
*worker* crashes (expired job leases), not a wedged *relay* connection. So a
transient broker blip violated the M5 guarantee.

## Decision

Make the publisher reconnect lazily.

`RabbitPublisher` now keeps the broker URL and re-establishes the connection on
demand instead of only at construction:

- `connect()` dials, opens a confirm-mode channel, and declares the
  exchange/queue topology. It is also still called once from
  `NewRabbitPublisher` so a misconfigured URL fails fast at startup.
- `ensureChannel()` (called under a mutex at the top of every `Publish`) returns
  the current channel if both `conn` and `channel` are live
  (`!IsClosed()`); otherwise it tears the dead connection down and redials.
- On any publish/confirm error, `Publish` invalidates the connection so the next
  call is forced to reconnect.

A publish failure is still surfaced to the relay, which rolls the batch back and
retries on its next tick (at-least-once, unchanged). By the next tick
`ensureChannel` has redialed, so the backlog drains on its own once the broker is
reachable again. A `sync.Mutex` guards the connection fields; the relay is
single-goroutine today but the publisher no longer assumes that.

### Alternatives considered

- **Background reconnect loop with `NotifyClose`.** More machinery (a goroutine,
  a state channel) for no better outcome — the relay already polls on a ticker,
  so reconnect-on-next-publish recovers within one interval. Rejected as
  over-engineering for the current single-relay design.
- **Restart the API on broker loss.** Pushes recovery onto the orchestrator and
  still drops in-flight work; defeats the point of a durable outbox.

## Consequences

- A broker restart (deploy, crash, network blip) no longer wedges the relay: the
  outbox drains automatically once RabbitMQ is back, with no API restart.
- The publisher is now safe under the documented failure and is concurrency-safe
  for a future multi-relay setup.
- Recovery latency is bounded by `OUTBOX_POLL_INTERVAL` (1s default) plus the
  dial time — the relay redials each tick while the broker is down.
- The request path is unchanged and was already broker-independent.

## Verification

- Integration (real Postgres + RabbitMQ via testcontainers):
  `TestRelayReconnectsAfterBrokerDrop` — the relay delivers a message, the test
  force-closes every broker connection via the management API (simulating a
  broker restart), and a subsequent message is still delivered, proving the
  publisher redialed transparently.
- Live drill (local compose stack, 2026-06-17):
  1. API + relay running, all infra up.
  2. `docker compose stop rabbitmq`.
  3. `POST /videos/upload` → **201 Created**; video row `queued`, one outbox row
     unpublished. Relay logs `outbox drain failed` each tick.
  4. `docker compose start rabbitmq`; once healthy, the outbox drained on the
     next tick (unpublished → 0) with **no API restart**. The relay logs show it
     redialing each tick (incrementing client source ports) rather than
     repeating on a dead channel, and errors stop the moment the broker is back.

  Before the fix, step 4 never recovered: the relay repeated
  `channel/connection is not open` indefinitely and the row stayed unpublished.
