#!/usr/bin/env bash
# Autoscaling experiment (M7): poll the RabbitMQ management API for the depth of
# the rendition queue and scale the worker replica count to match — the manual,
# single-host stand-in for what KEDA does on Kubernetes later (M12 capstone).
#
#   ./infrastructure/scripts/autoscale-on-queue-depth.sh
#
# Tunables (env): MIN_WORKERS=1 MAX_WORKERS=4 TICKS=40 INTERVAL=3
# Each rendition worker handles one quality at a time (WORKER_CONCURRENCY=1), so
# the controller simply targets one worker per in-flight rendition message,
# clamped to [MIN_WORKERS, MAX_WORKERS].
set -euo pipefail

MIN="${MIN_WORKERS:-1}"
MAX="${MAX_WORKERS:-4}"
TICKS="${TICKS:-40}"
INTERVAL="${INTERVAL:-3}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE=(docker compose -f "$ROOT/infrastructure/docker-compose.yml" -f "$ROOT/infrastructure/docker-compose.app.yml")
RABBIT="http://mediaflow:mediaflow@localhost:15672/api/queues/%2F"

# messages = ready + unacked, i.e. queued plus currently being transcoded.
queue_depth() { curl -fsS "$RABBIT/$1" | jq -r '.messages // 0'; }
running_workers() { "${COMPOSE[@]}" ps --status running worker -q 2>/dev/null | grep -c . || true; }

clamp() { local v="$1"; (( v < MIN )) && v="$MIN"; (( v > MAX )) && v="$MAX"; echo "$v"; }

printf "%-5s %-10s %-9s %-9s %s\n" tick rend_depth workers desired action
for ((t = 1; t <= TICKS; t++)); do
  depth="$(queue_depth video.rendition)"
  have="$(running_workers)"
  want="$(clamp "$depth")"

  action="hold"
  if [ "$want" != "$have" ]; then
    "${COMPOSE[@]}" up -d --scale "worker=$want" >/dev/null 2>&1
    action="scale -> $want"
  fi
  printf "%-5s %-10s %-9s %-9s %s\n" "$t" "$depth" "$have" "$want" "$action"
  sleep "$INTERVAL"
done
