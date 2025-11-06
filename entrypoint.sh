#!/usr/bin/env bash
set -euo pipefail

# load .env if mounted at /app/.env
if [ -f /app/.env ]; then
  set -a
  . /app/.env
  set +a
fi

# required envs
REQUIRED=("WEBHOOK_URL" "DATABASE_HOST" "DATABASE_USER" "DATABASE_PASSWORD" "DATABASE_NAME" "DATABASE_PORT" "RAIDERIO_API_KEY")
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
  exit 2
fi

send_webhook(){
  payload="{\"status\":\"$1\",\"container\":\"${HOSTNAME:-unknown}\"}"
  curl --max-time 5 -s -X POST -H "Content-Type: application/json" -d "$payload" "$WEBHOOK_URL"
}

send_webhook started

# ensure /data/runs exists (volume)
mkdir -p /data/runs || true

DB_ARGS=(
  --database_host "${DATABASE_HOST}"
  --database_user "${DATABASE_USER}"
  --database_password "${DATABASE_PASSWORD}"
  --database "${DATABASE_NAME}"
  --port "${DATABASE_PORT}"
)

# run app, log to file so we can include tail on failure
LOGFILE=/app/collector.log
: > "$LOGFILE"
python /app/collectLeaderboardData.py "${DB_ARGS[@]}" &
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

exit $EXIT_CODE
