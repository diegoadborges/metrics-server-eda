#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://localhost:8080/metrics}"
SLEEP_SECONDS="${SLEEP_SECONDS:-1}"

rand_float() {
  local seed="$1"
  local min="$2"
  local max="$3"
  awk -v seed="$seed" -v min="$min" -v max="$max" 'BEGIN { srand(seed); printf "%.1f", min + (max - min) * rand() }'
}

servers=(web-01 web-02 api-01 db-01 cache-01)

while true; do
  server_id="${servers[$((RANDOM % ${#servers[@]}))]}"
  cpu_seed="$((RANDOM * 1000 + RANDOM))"
  mem_seed="$((RANDOM * 1000 + RANDOM))"

  if (( RANDOM % 4 == 0 )); then
    cpu_usage="$(rand_float "$cpu_seed" 90 100)"
  else
    cpu_usage="$(rand_float "$cpu_seed" 0 100)"
  fi

  if (( RANDOM % 5 == 0 )); then
    memory_usage="$(rand_float "$mem_seed" 90 100)"
  else
    memory_usage="$(rand_float "$mem_seed" 0 100)"
  fi

  timestamp="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

  payload=$(printf '{"server_id":"%s","cpu_usage":%s,"memory_usage":%s,"timestamp":"%s"}' \
    "$server_id" "$cpu_usage" "$memory_usage" "$timestamp")

  curl -sS -X POST "$API_URL" \
    -H "Content-Type: application/json" \
    -d "$payload" >/dev/null

  echo "sent $payload"
  sleep "$SLEEP_SECONDS"
done

