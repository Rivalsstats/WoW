#!/usr/bin/env bash
set -euo pipefail

# load .env if mounted at /app/.env
if [ -f /app/.env ]; then
  set -a
  # shellcheck disable=SC1091
  . /app/.env
  set +a
fi

REQUIRED=("WEBHOOK_URL" "DATABASE_HOST" "DATABASE_USER" "DATABASE_PASSWORD" "DATABASE_NAME" "DATABASE_PORT" "RAIDERIO_API_KEY")
missing=()
for v in "${REQUIRED[@]}"; do
  if [ -z "${!v:-}" ]; then
    missing+=("$v")
  fi
done

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
  curl --max-time 5 -s -X POST -H "Content-Type: application/json" -d "$payload" "$WEBHOOK_URL" || true
}

send_webhook started

# ensure /data/runs exists (volume mount target)
mkdir -p /data/runs || true


DB_ARGS=(
  --database_host "${DATABASE_HOST}"
  --database_user "${DATABASE_USER}"
  --database_password "${DATABASE_PASSWORD}"
  --database "${DATABASE_NAME}"
  --port "${DATABASE_PORT}"
)

# start python app as child so we can trap signals and report webhooks
python /app/collectLeaderboardData.py "${DB_ARGS[@]}" &
APP_PID=$!

GIT_BRANCH="${GIT_BRANCH:-main}"
CHECK_INTERVAL="${CHECK_INTERVAL:-60}"

# watcher: fetch origin, compare origin/branch to local HEAD.
( while true; do
    git -C /opt/repo fetch origin "$GIT_BRANCH" >/dev/null 2>&1 || true
    REMOTE=$(git -C /opt/repo rev-parse "origin/${GIT_BRANCH}" 2>/dev/null || true)
    LOCAL=$(git -C /opt/repo rev-parse HEAD 2>/dev/null || true)
    if [ -n "$REMOTE" ] && [ "$REMOTE" != "$LOCAL" ]; then
       git -C /opt/repo reset --hard "$REMOTE" || git -C /opt/repo pull --ff-only || true
       cp -f /opt/repo/backend_scripts/collectLeaderboardData.py /app/ || true
       cp -f /opt/repo/backend_scripts/databaseConnector.py /app/ || true
       mkdir -p /app/data/static
       cp -f /opt/repo/data/static/dungeons.json /app/data/static/dungeons.json || true
       send_webhook updated
       # ask python pro
