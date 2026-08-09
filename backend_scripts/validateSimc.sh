#!/usr/bin/env bash
# Validate a simc binary by initializing every actor in one or more profiles.
#
#   validateSimc.sh <simc-binary> <profile.simc> [profile2.simc ...]
#
# simc aborts with a non-zero exit (code 50) the moment any actor references an
# action whose spell data is missing — e.g. the Unholy DK `army_ghoul` regression
# that failed run 31325084815. "Severe"/"Trivial" warnings (e.g. a spec that does
# not support the fight style) keep exit 0, so the exit code alone is the signal:
# a clean run means EVERY actor in EVERY profile initialized. Any failure rejects
# the whole build — we never ship a simc that can't init a spec we sim.
#
# One iteration / one second: create_actions() (where the missing-data abort
# fires) runs during init, before iteration 0, so this is a fast gate.
set -uo pipefail

simc="$1"; shift
if [ ! -f "$simc" ]; then
  echo "validateSimc: '$simc' does not exist" >&2
  exit 2
fi
if [ "$#" -eq 0 ]; then
  echo "validateSimc: no profiles given" >&2
  exit 2
fi

for prof in "$@"; do
  if [ ! -f "$prof" ]; then
    echo "validateSimc: profile '$prof' not found" >&2
    exit 2
  fi
  out=$("$simc" "$prof" iterations=1 max_time=1 2>&1)
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "validateSimc: FAILED on $prof (simc exit $rc)" >&2
    printf '%s\n' "$out" | grep -iE "could not find spell data|Error:|Unable to|Fatal" | tail -20 >&2
    exit "$rc"
  fi
done

echo "validateSimc: OK ($*)"
exit 0
