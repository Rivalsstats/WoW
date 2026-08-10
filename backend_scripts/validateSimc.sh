#!/usr/bin/env bash
# Validate a simc binary by actually SIMULATING every actor in one or more profiles.
#
#   validateSimc.sh <simc-binary> <profile.simc> [profile2.simc ...]
#
# simc aborts with a non-zero exit (code 50) the moment an actor references an
# action whose spell data is missing. An earlier version of this gate ran
# `iterations=1 max_time=1` on the theory that create_actions() runs before
# iteration 0, so a 1-second fight was enough. That is only true for STATIC
# actions. Actions on a dynamically-spawned pet are created when the pet is
# summoned, mid-combat — so the Unholy DK `army_ghoul` regression sailed through
# this gate (`validateSimc: OK` in run 31335443090) and then killed all 8 sim legs.
#
# So: run a real fight, at the two shapes the tierlist matrix actually uses
# (single-target 180s and 8-target 60s — some actions are AoE-gated). A handful of
# iterations of a short fight costs seconds per candidate and is the only thing
# that reaches a pet summon.
#
# "Severe"/"Trivial" warnings (e.g. a spec that does not support the fight style)
# keep exit 0, so the exit code alone is the signal: a clean run means EVERY actor
# in EVERY profile both initialized and simulated. Any failure rejects the whole
# build — we never ship a simc that can't sim a spec we rank.
#
# Env:
#   SIMC_VALIDATE_ITERATIONS  iterations per shape (default 5)
#   SIMC_VALIDATE_TIMEOUT     per-invocation wall clock, seconds (default 600)
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

ITERATIONS="${SIMC_VALIDATE_ITERATIONS:-5}"
TIMEOUT="${SIMC_VALIDATE_TIMEOUT:-600}"

# Mirrors the simulate matrix in buildPages.yml: 180s single-target, 60s multi.
SHAPES=(
  "desired_targets=1 max_time=180"
  "desired_targets=8 max_time=60"
)

for prof in "$@"; do
  if [ ! -f "$prof" ]; then
    echo "validateSimc: profile '$prof' not found" >&2
    exit 2
  fi
  for shape in "${SHAPES[@]}"; do
    # A broken actor does not always abort cleanly: in run 31335443090 five of
    # eight sim legs DEADLOCKED instead of exiting 50. Treat a hang as a failed
    # candidate (not an infra error) so the walk-back moves on instead of
    # burning the whole job on one bad commit. -k SIGKILLs if TERM is ignored.
    # shellcheck disable=SC2086
    out=$(timeout -k 30 "$TIMEOUT" "$simc" "$prof" iterations="$ITERATIONS" $shape 2>&1)
    rc=$?
    if [ "$rc" -eq 124 ] || [ "$rc" -eq 137 ]; then
      echo "validateSimc: FAILED on $prof [$shape] (hung >${TIMEOUT}s)" >&2
      printf '%s\n' "$out" | tail -5 >&2
      exit 1
    fi
    if [ "$rc" -ne 0 ]; then
      echo "validateSimc: FAILED on $prof [$shape] (simc exit $rc)" >&2
      printf '%s\n' "$out" | grep -iE "could not find spell data|Error:|Unable to|Fatal" | tail -20 >&2
      exit "$rc"
    fi
  done
done

echo "validateSimc: OK ($*) at iterations=$ITERATIONS across ${#SHAPES[@]} fight shapes"
exit 0
