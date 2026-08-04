#!/usr/bin/env bash
set -euo pipefail

# Load .env if mounted at /app/.env (compose also injects it via env_file, but
# re-sourcing here lets a Windows-edited file with CRLF still work).
if [ -f /app/.env ]; then
  set -a
  . /app/.env
  set +a
fi

json_escape(){
  printf '%s' "$1" | python -c 'import json,sys; print(json.dumps(sys.stdin.read())[1:-1], end="")'
}

post_alert(){
  [ -n "${WEBHOOK_URL:-}" ] || return 0
  payload="{\"content\": \"$(json_escape "$1")\"}"
  curl --max-time 5 -s -o /dev/null -X POST -H "Content-Type: application/json" -d "$payload" "$WEBHOOK_URL" || true
}

REQUIRED=("DISCORD_BOT_TOKEN" "DATABASE_HOST" "DATABASE_USER" "DATABASE_PASSWORD" "DATABASE_NAME" "DATABASE_PORT")
missing=()
for v in "${REQUIRED[@]}"; do
  if [ -z "${!v:-}" ]; then
    missing+=("$v")
  fi
done

if [ "${#missing[@]}" -ne 0 ]; then
  echo "ERROR: missing required env vars: ${missing[*]}" >&2
  post_alert "**bot crash** \`${HOSTNAME:-unknown}\`: missing env vars: ${missing[*]}"
  exit 2
fi

# Re-run the image-content guard as a startup preflight (matches the collector).
if ! python -u /app/verifyImageImports.py /app; then
  echo "CRITICAL STARTUP ERROR: the image is missing something it needs (see above)" >&2
  post_alert "**bot crash** \`${HOSTNAME:-unknown}\`: image missing a module or static data file"
  exit 3
fi

post_alert "bot \`${HOSTNAME:-unknown}\`: started"
mkdir -p /app/data/bot_cache/charts || true

python -u -m discord_bot &
APP_PID=$!

_term(){
  post_alert "bot \`${HOSTNAME:-unknown}\`: stopping"
  kill -TERM "$APP_PID" 2>/dev/null || true
  wait "$APP_PID" 2>/dev/null || true
  exit 0
}
trap _term SIGTERM SIGINT

wait "$APP_PID"
EXIT_CODE=$?
post_alert "bot \`${HOSTNAME:-unknown}\`: exited:${EXIT_CODE}"
exit $EXIT_CODE
