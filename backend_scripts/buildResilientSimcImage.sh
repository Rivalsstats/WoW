#!/usr/bin/env bash
# Build & publish a *validated* simc image to GHCR (buildSimcImage.yml).
#
# Same walk-back idea as buildResilientSimc.sh, but for the collector's image and
# self-contained (this workflow installs no DB driver): validate each candidate
# with the generated smoke.simc (one default actor per simulated spec). Publish
# the newest commit whose image initializes every spec.
#
# Crucially this bounds staleness by the newest *working* commit, not by the
# weekly cron: a broken tip just publishes yesterday's good commit. If nothing in
# the window passes we DON'T push — the collector keeps pulling the last good
# :latest — and the job fails loudly so a human sees a sustained upstream break.
#
# Inputs (env / files):
#   IMAGE            target repo, default ghcr.io/mythistone/simc
#   candidates.txt   newest-first candidate shas, one per line
#   smoke.simc       all-spec smoke profile (from simcSmoke.py)
set -uo pipefail

IMAGE="${IMAGE:-ghcr.io/mythistone/simc}"
HEAD_SHA="$(head -1 candidates.txt)"

# Static provenance labels (mirror the OCI labels the workflow used to set); the
# revision label is stamped per-candidate below to keep GPL-3 source provenance honest.
LABELS=(
  --label "org.opencontainers.image.title=SimulationCraft (unofficial build)"
  --label "org.opencontainers.image.description=Unofficial SimulationCraft build for the MythiStone collector"
  --label "org.opencontainers.image.source=https://github.com/simulationcraft/simc"
  --label "org.opencontainers.image.licenses=GPL-3.0-or-later"
)

while read -r sha; do
  [ -n "$sha" ] || continue
  echo "::group::Building & validating simc image $sha"

  if ! docker buildx build --load \
        -f Dockerfile.simc --platform linux/amd64 \
        --build-arg "SIMC_REV=$sha" \
        --cache-from type=gha --cache-to type=gha,mode=max \
        "${LABELS[@]}" --label "org.opencontainers.image.revision=$sha" \
        -t "$IMAGE:candidate" . ; then
    echo "::warning::docker build failed for $sha; trying older"
    echo "::endgroup::"
    continue
  fi

  # Entrypoint is /app/simc; mount the smoke profile and let simc init every actor.
  if docker run --rm -v "$PWD:/io" "$IMAGE:candidate" /io/smoke.simc iterations=1 max_time=1; then
    # Re-run with --push (cache hit -> near-instant) to publish a proper registry
    # manifest tagged latest + the exact sha.
    docker buildx build --push \
      -f Dockerfile.simc --platform linux/amd64 \
      --build-arg "SIMC_REV=$sha" \
      --cache-from type=gha \
      "${LABELS[@]}" --label "org.opencontainers.image.revision=$sha" \
      -t "$IMAGE:latest" -t "$IMAGE:$sha" .
    echo "::endgroup::"
    [ "$sha" != "$HEAD_SHA" ] && echo "::warning::simc HEAD ($HEAD_SHA) failed validation; published image built from $sha"
    echo "Published $IMAGE:latest and $IMAGE:$sha"
    exit 0
  fi

  echo "::warning::simc commit $sha failed smoke validation; trying older"
  echo "::endgroup::"
done < candidates.txt

echo "::error::No simc commit in the walk-back window passed validation; kept existing :latest so the collector stays on the last good image." >&2
exit 1
