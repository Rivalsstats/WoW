#!/usr/bin/env bash
set -euo pipefail

# load .env if mounted at /app/.env
if [ -f /app/.env ]; then
  set -a
  . /app/.env
  set +a
fi

# WEBHOOK_URL is a Discord webhook (see .envexample), and Discord rejects a body
# without content/embeds/file with {"message": "Cannot send an empty message",
# "code": 50006} -- so every alert has to carry a "content" field. json_escape
# keeps interpolated values (env var names, file paths) inside the JSON string.
json_escape(){
  printf '%s' "$1" | python -c 'import json,sys; print(json.dumps(sys.stdin.read())[1:-1], end="")'
}

post_alert(){
  # best-effort, never blocks startup or shutdown
  [ -n "${WEBHOOK_URL:-}" ] || return 0
  payload="{\"content\": \"$(json_escape "$1")\"}"
  curl --max-time 5 -s -o /dev/null -X POST -H "Content-Type: application/json" -d "$payload" "$WEBHOOK_URL" || true
}

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
  post_alert "**crash** \`${HOSTNAME:-unknown}\`: missing env vars: ${missing[*]}"
  exit 2
fi

# Everything the app imports or reads must be present before anything runs. This
# replaces a hardcoded list of required static files: verifyImageImports.py
# derives both sets from the source itself, so it can't fall behind the way a
# hand-maintained list does (it already covers every file that list named, plus
# the ones it had missed). Without it a gap in the Dockerfile COPY blocks only
# surfaces later as a crash-loop or, worse, as sims quietly running on empty
# lookup tables.
if ! python -u /app/verifyImageImports.py /app; then
  echo "CRITICAL STARTUP ERROR: the image is missing something it needs (see above)" >&2
  post_alert "**crash** \`${HOSTNAME:-unknown}\`: image is missing a module or static data file it needs - see container logs"
  exit 3
fi

send_webhook(){
  post_alert "collector \`${HOSTNAME:-unknown}\`: $1"
}

send_webhook started

# ensure /data/runs exists (volume)
mkdir -p /data/runs || true

# clear out any simc sibling container left running from a previous instance
# that was SIGKILLed before it could clean up after itself (e.g. watchtower's
# stop timeout expired before our own SIGTERM handler finished).
python -u -c "import sys; sys.path.insert(0, '/app'); import simcBis; simcBis.cleanup_orphaned_containers('startup')" 2>&1 || true

python -u /app/collectLeaderboardData.py &
APP_PID=$!

_term(){
  send_webhook stopping
  kill -TERM "$APP_PID" 2>/dev/null || true
  wait "$APP_PID" 2>/dev/null || true
  # simc sibling containers aren't children of this process and aren't tracked
  # by watchtower, so nothing else stops them when this container is replaced
  # (e.g. on a watchtower update). Clean them up by label, independent of
  # whatever state the python process exited in.
  python -u -c "import sys; sys.path.insert(0, '/app'); import simcBis; simcBis.cleanup_orphaned_containers('container stopping')" 2>&1 || true
  exit 0
}
trap _term SIGTERM SIGINT

# wait for collector to exit and then report
wait "$APP_PID"
EXIT_CODE=$?

send_webhook "exited:${EXIT_CODE}"

exit $EXIT_CODE
