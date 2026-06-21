#!/usr/bin/env bash
# Measure the M7 fan-out speedup: time one source video from upload to `ready`
# with 1 worker vs N workers, against the real containerised stack.
#
#   ./infrastructure/scripts/measure-fanout-speedup.sh [N] [DURATION_SECONDS]
#
# Defaults: N=3, DURATION=60. Requires Docker, ffmpeg, curl, jq on the host.
set -euo pipefail

N="${1:-3}"
DURATION="${2:-60}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE=(docker compose -f "$ROOT/infrastructure/docker-compose.yml" -f "$ROOT/infrastructure/docker-compose.app.yml")
API="http://localhost:8080"
SRC="$(mktemp -t mediaflow-src-XXXX).mp4"
POLL_TIMEOUT=600

now_ms() { python3 -c 'import time;print(int(time.time()*1000))'; }

cleanup() { rm -f "$SRC"; }
trap cleanup EXIT

scale_to() {
  local count="$1"
  echo "==> scaling to $count worker(s)"
  "${COMPOSE[@]}" up -d --scale "worker=$count" >/dev/null
  # Give freshly started workers a moment to declare their consumers.
  sleep 5
  echo "    running workers: $("${COMPOSE[@]}" ps --status running worker -q | wc -l | tr -d ' ')"
}

wait_api() {
  echo "==> waiting for API /health"
  for _ in $(seq 1 60); do
    if curl -fsS "$API/health" >/dev/null 2>&1; then echo "    API up"; return 0; fi
    sleep 2
  done
  echo "API never became healthy" >&2; exit 1
}

# upload_and_time prints the elapsed milliseconds from upload to `ready`.
upload_and_time() {
  local start id status elapsed
  start="$(now_ms)"
  id="$(curl -fsS -X POST "$API/videos/upload" \
        -F "file=@${SRC};type=video/mp4" -F "title=speedup-${RANDOM}" | jq -r '.id')"
  if [ -z "$id" ] || [ "$id" = "null" ]; then echo "upload failed" >&2; exit 1; fi

  local deadline=$(( $(date +%s) + POLL_TIMEOUT ))
  while :; do
    status="$(curl -fsS "$API/videos/$id" | jq -r '.status')"
    case "$status" in
      ready) break ;;
      failed) echo "video $id FAILED" >&2; exit 1 ;;
    esac
    if [ "$(date +%s)" -ge "$deadline" ]; then echo "timed out waiting for $id (last=$status)" >&2; exit 1; fi
    sleep 1
  done
  elapsed=$(( $(now_ms) - start ))
  echo "    video $id ready in ${elapsed} ms (status=$status)"
  echo "$elapsed"
}

echo "==> generating ${DURATION}s 720p test source"
ffmpeg -hide_banner -loglevel error -y \
  -f lavfi -i "testsrc=size=1280x720:rate=30:duration=${DURATION}" \
  -f lavfi -i "sine=frequency=440:duration=${DURATION}" \
  -c:v libx264 -preset veryfast -pix_fmt yuv420p -c:a aac -shortest "$SRC"
echo "    source: $(du -h "$SRC" | cut -f1)"

echo "==> bringing up stack (build if needed)"
"${COMPOSE[@]}" up -d --build >/dev/null
wait_api

scale_to 1
T1="$(upload_and_time | tail -1)"

scale_to "$N"
TN="$(upload_and_time | tail -1)"

echo
echo "================ fan-out speedup ================"
printf "1 worker : %6d ms\n" "$T1"
printf "%d workers: %6d ms\n" "$N" "$TN"
python3 - "$T1" "$TN" "$N" <<'PY'
import sys
t1, tn, n = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
print(f"speedup  : {t1/tn:.2f}x  ({n} workers vs 1)")
PY
echo "================================================="
