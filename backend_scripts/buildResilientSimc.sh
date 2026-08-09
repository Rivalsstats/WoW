#!/usr/bin/env bash
# Build a *validated* simc binary for the tierlist sims (buildPages.yml).
#
# Instead of trusting the absolute tip of simc's actively-developed `midnight`
# branch (which briefly broke Unholy DK's `army_ghoul` and failed run
# 31325084815), walk newest -> oldest over recent commits and pick the first one
# whose build initializes EVERY spec in the real generated gearset profiles. That
# keeps simc as fresh as possible while never shipping a build that can't sim a
# spec we rank — so no spec is dropped from the tierlist.
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

PROFILES=(simc_io/gearset_popular.simc simc_io/gearset_simcbis.simc)
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

while read -r sha; do
  [ -n "$sha" ] || continue
  echo "::group::Building & validating simc $sha"
  if compile_sha "$sha" simc-bin/simc; then
    chmod +x simc-bin/simc
    if bash backend_scripts/validateSimc.sh simc-bin/simc "${PROFILES[@]}"; then
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
done < candidates.txt

# Nothing in the window worked — fall back to last-known-good if we have one.
if [ -f simc-lastgood/simc ]; then
  cp simc-lastgood/simc simc-bin/simc
  good=$(cat simc-lastgood/CHOSEN_SHA 2>/dev/null || echo unknown)
  printf '%s\n' "$good" > simc-bin/CHOSEN_SHA
  echo "::warning::No candidate simc commit in the walk-back window passed validation; using last-known-good $good"
  exit 0
fi

echo "::error::No simc commit passed validation and no last-known-good binary is cached. Aborting." >&2
exit 1
