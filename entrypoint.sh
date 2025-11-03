#!/usr/bin/env bash
set -euo pipefail

MAIN_PID=$$

# load .env if mounted at /app/.env
if [ -f /app/.env ]; then
  set -a
  # shellcheck disable=SC1091
  . /app/.env
  set +a
fi

 # kill the process and throw an error if no webhook is set
if [ -z "${WEBHOOK_URL:-}" ]; then
    echo "ERROR: WEBHOOK_URL is not set in environment or .env file."
    exit 1
fi

send_webhook(){
  [ -z "${WEBHOOK_URL:-}" ] && return 0
  payload="{\"status\":\"$1\",\"container\":\"${HOSTNAME:-unknown}\"}"
  curl --max-time 5 -s -X POST -H "Content-Type: application/json" -d "$payload" "$WEBHOOK_URL" || true
}

send_webhook started

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
       cp -f /opt/repo/data/static/dungeons.json /app/ || true
       send_webhook updated
       # ask main process to terminate so Docker restarts the container with updated files
       kill -TERM "$MAIN_PID" 2>/dev/null || true
       exit 0
    fi
    sleep "$CHECK_INTERVAL"
done ) &

WATCHER_PID=$!

# run python app as child so we can trap signals and report webhook
python /app/collectLeaderboardData.py &
APP_PID=$!

_term(){
  send_webhook stopping
  # forward termination to python app
  kill -TERM "$APP_PID" 2>/dev/null || true
  wait "$APP_PID" 2>/dev/null || true
  # stop watcher
  kill "$WATCHER_PID" 2>/dev/null || true
  exit 0
}
trap _term SIGTERM SIGINT

wait "$APP_PID"
EXIT_CODE=$?

# normal exit
send_webhook exited
kill "$WATCHER_PID" 2>/dev/null || true
exit $EXIT_CODE
