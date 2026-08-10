#!/usr/bin/env bash
# Build a *validated* simc binary for the tierlist sims (buildPages.yml).
#
# Instead of trusting the absolute tip of simc's actively-developed `midnight`
# branch (which broke Unholy DK's `army_ghoul` and failed runs 31325084815 and
# 31335443090), walk back over recent commits and pick the first one that can
# actually SIMULATE every spec in the real generated gearset profiles. That keeps
# simc as fresh as possible while never shipping a build that can't sim a spec we
# rank — so no spec is dropped from the tierlist.
#
# Two things this gate learned the hard way (see validateSimc.sh):
#   * validation must run a real fight, not a 1-second init, or pet actions slip
#     through and every sim leg dies later;
#   * the search must reach ~30 commits back, so it samples geometrically rather
#     than scanning linearly (see simcWalkback.sh).
#
# Inputs (env / files):
#   HEAD_SHA      newest candidate sha (for cache seeding + messaging)
#   HEAD_CACHED   "true" if simc-cache/simc is a cache hit for HEAD_SHA
#   candidates.txt   newest-first candidate shas, one per line
#   simc_io/gearset_{popular,simcbis}.simc   profiles to validate against
#   simc-cache/simc     cached HEAD binary (if HEAD_CACHED)
#   simc-lastgood/simc  last-known-good binary (fallback, if any)
# Outputs:
#   simc-bin/simc        the chosen, validated binary (uploaded to the matrix)
#   simc-bin/CHOSEN_SHA  the commit it was built from
#   simc-cache/simc      seeded when HEAD passed, so next run hits the fast path
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=backend_scripts/simcWalkback.sh
source "$HERE/simcWalkback.sh"

GEARSETS=(simc_io/gearset_popular.simc simc_io/gearset_simcbis.simc)
mkdir -p simc-bin

compile_sha() {
  # Shallow-fetch just this commit and build the CLI (no GUI). Returns non-zero
  # on any failure so the caller moves to the next candidate.
  local sha="$1" dest="$2"
  rm -f "$dest"
  rm -rf simc-src
  git init -q simc-src \
    && git -C simc-src remote add origin https://github.com/simulationcraft/simc.git \
    && git -C simc-src fetch -q --depth=1 origin "$sha" \
    && git -C simc-src checkout -q FETCH_HEAD \
    && cmake -S simc-src -B simc-src/build -DCMAKE_BUILD_TYPE=Release -DBUILD_GUI=OFF >/dev/null \
    && cmake --build simc-src/build --parallel "$(nproc)" --target simc >/dev/null \
    && cp "$(find simc-src/build -name simc -type f -executable | head -1)" "$dest"
}

# Fast path: HEAD already built and cached. Cached binaries were validated before
# being cached, and game data is baked into the binary, so a cache hit is still good.
if [ "${HEAD_CACHED:-}" = "true" ] && [ -f simc-cache/simc ]; then
  cp simc-cache/simc simc-bin/simc
  printf '%s\n' "$HEAD_SHA" > simc-bin/CHOSEN_SHA
  echo "Using cached HEAD simc $HEAD_SHA"
  exit 0
fi

# mapfile returns 0 even when the substitution produced nothing, so check the count.
mapfile -t CANDIDATES < <(select_candidates candidates.txt)
if [ "${#CANDIDATES[@]}" -eq 0 ]; then
  echo "::error::no simc candidates to try; candidates.txt is empty or unreadable." >&2
  exit 1
fi
echo "Trying ${#CANDIDATES[@]} of $(wc -l < candidates.txt) candidates: ${CANDIDATES[*]}"

for sha in "${CANDIDATES[@]}"; do
  echo "::group::Building & validating simc $sha"
  if compile_sha "$sha" simc-bin/simc; then
    chmod +x simc-bin/simc
    # The gearset profiles are the authoritative gate: they are generated from our
    # own spec list, so a spec we rank that can't sim fails right here. simc's own
    # all-spec CI profile is added for breadth (hero-talent variants our gearsets
    # don't carry) and is free — compile_sha just checked it out.
    PROFILES=("${GEARSETS[@]}")
    if [ -f simc-src/profiles/CI.simc ]; then
      PROFILES+=(simc-src/profiles/CI.simc)
    else
      echo "::error::simc-src/profiles/CI.simc missing at $sha — upstream moved it; update the gate." >&2
      exit 1
    fi
    if bash "$HERE/validateSimc.sh" simc-bin/simc "${PROFILES[@]}"; then
      printf '%s\n' "$sha" > simc-bin/CHOSEN_SHA
      # Seed the HEAD cache only when HEAD itself passed, so the fast path works next run.
      if [ "$sha" = "$HEAD_SHA" ]; then mkdir -p simc-cache && cp simc-bin/simc simc-cache/simc; fi
      echo "::endgroup::"
      [ "$sha" != "$HEAD_SHA" ] && echo "::warning::simc HEAD ($HEAD_SHA) failed validation; walked back to $sha"
      echo "Selected simc commit $sha"
      exit 0
    fi
  else
    echo "compile failed for $sha" >&2
  fi
  echo "::endgroup::"
done

# Nothing we sampled worked — fall back to last-known-good if we have one.
if [ -f simc-lastgood/simc ]; then
  cp simc-lastgood/simc simc-bin/simc
  good=$(cat simc-lastgood/CHOSEN_SHA 2>/dev/null || echo unknown)
  printf '%s\n' "$good" > simc-bin/CHOSEN_SHA
  echo "::warning::No sampled simc commit passed validation; using last-known-good $good"
  exit 0
fi

echo "::error::No simc commit passed validation and no last-known-good binary is cached. Aborting." >&2
exit 1
