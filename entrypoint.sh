#!/usr/bin/env bash
set -euo pipefail

# load .env if mounted at /app/.env
if [ -f /app/.env ]; then
  set -a
  . /app/.env
  set +a
fi

# required envs
REQUIRED=("WEBHOOK_URL" "DATABASE_HOST" "DATABASE_USER" "DATABASE_PASSWORD" "DATABASE_NAME" "DATABASE_PORT" "RAIDERIO_API_KEY" "KEYSTONE_GURU_USER" "KEYSTONE_GURU_PW")
missing=()
for v in "${REQUIRED[@]}"; do
  if [ -z "${!v:-}" ]; then
    missing+=("$v")
  fi
done

# check Blizzard client id/secret for configured regions
REGIONS="${REGIONS:-us,eu,kr,tw}"
IFS=',' read -r -a REGION_ARR <<< "$REGIONS"
for r in "${REGION_ARR[@]}"; do
  up=$(printf "%s" "$r" | awk '{print toupper($0)}')
  idvar="BLIZ_CLIENT_ID_${up}"
  secvar="BLIZ_CLIENT_SECRET_${up}"
  if [ -z "${!idvar:-}" ] || [ -z "${!secvar:-}" ]; then
    missing+=("$idvar" "$secvar")
  fi
done

if [ "${#missing[@]}" -ne 0 ]; then
  echo "ERROR: missing required env vars: ${missing[*]}" >&2
  if [ -n "${WEBHOOK_URL:-}" ]; then
    payload="{\"status\":\"crash\",\"message\":\"Missing env vars: ${missing[*]}\",\"container\":\"${HOSTNAME:-unknown}\"}"
    curl --max-time 5 -s -X POST -H "Content-Type: application/json" -d "$payload" "$WEBHOOK_URL" || true
  fi
  exit 2
fi

# required static files
REQUIRED_FILES=("/app/data/static/dungeons.json" "/app/data/static/specs.json" "/app/data/static/talents.json" "/app/data/static/classes.json")
missing_files=()
for f in "${REQUIRED_FILES[@]}"; do
  if [ ! -f "$f" ]; then
    missing_files+=("$f")
  fi
done

if [ "${#missing_files[@]}" -ne 0 ]; then
  echo "CRITICAL STARTUP ERROR: missing required static application files: ${missing_files[*]}" >&2
  
  if [ -n "${WEBHOOK_URL:-}" ]; then
    payload="{\"status\":\"crash\",\"message\":\"Missing required static files: ${missing_files[*]}\",\"container\":\"${HOSTNAME:-unknown}\"}"
    curl --max-time 5 -s -X POST -H "Content-Type: application/json" -d "$payload" "$WEBHOOK_URL" || true
  fi
  
  exit 3
fi

send_webhook(){
  payload="{\"status\":\"$1\",\"container\":\"${HOSTNAME:-unknown}\"}"
  # best-effort, don't exit on failure
  curl --max-time 5 -s -X POST -H "Content-Type: application/json" -d "$payload" "$WEBHOOK_URL" || true
}

send_webhook started

# ensure /data/runs exists (volume)
mkdir -p /data/runs || true

python -u /app/collectLeaderboardData.py &
APP_PID=$!

_term(){
  send_webhook stopping
  kill -TERM "$APP_PID" 2>/dev/null || true
  wait "$APP_PID" 2>/dev/null || true
  exit 0
}
trap _term SIGTERM SIGINT

# wait for collector to exit and then report
wait "$APP_PID"
EXIT_CODE=$?

send_webhook "exited:${EXIT_CODE}"

exit $EXIT_CODE
