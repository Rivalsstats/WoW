#!/usr/bin/env bash
# Build & publish a *validated* simc image to GHCR (buildSimcImage.yml).
#
# Same walk-back idea as buildResilientSimc.sh, but for the collector's image and
# self-contained (this workflow installs no DB driver, and has no generated
# gearset profiles). Validation therefore runs simc's own all-spec CI profile,
# fetched at the exact candidate commit. Publish the newest sampled commit whose
# image can actually simulate every spec.
#
# Using upstream's CI.simc rather than a hand-rolled profile is deliberate: its
# actors carry `load_default_gear=1`, so they are properly geared. The previous
# hand-rolled smoke shipped weaponless actors, Death Knights hard-fail init
# without a main hand, and so runs 31335443071 / 31359307342 rejected every
# candidate over a defect in OUR profile and published nothing for days.
#
# Crucially the walk-back bounds staleness by the newest *working* commit, not by
# the cron: a broken tip just publishes an older good commit. If nothing we
# sampled passes we DON'T push — the collector keeps pulling the last good
# :latest — and the job fails loudly so a human sees a sustained upstream break.
#
# Inputs (env / files):
#   IMAGE            target repo, default ghcr.io/mythistone/simc
#   candidates.txt   newest-first candidate shas, one per line
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=backend_scripts/simcWalkback.sh
source "$HERE/simcWalkback.sh"

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

# Let validateSimc.sh drive the containerised binary exactly like a local one, so
# both workflows share one gate and can never drift apart on fight shapes or
# timeouts. `-w /io` makes the container resolve the profile path relative to the
# mounted workdir, which is also where validateSimc.sh checks it exists on the host.
cat > docker-simc.sh <<EOF
#!/usr/bin/env bash
exec docker run --rm -v "\$PWD:/io" -w /io "$IMAGE:candidate" "\$@"
EOF
chmod +x docker-simc.sh

# mapfile returns 0 even when the substitution produced nothing, so check the count.
mapfile -t CANDIDATES < <(select_candidates candidates.txt)
if [ "${#CANDIDATES[@]}" -eq 0 ]; then
  echo "::error::no simc candidates to try; candidates.txt is empty or unreadable." >&2
  exit 1
fi
echo "Trying ${#CANDIDATES[@]} of $(wc -l < candidates.txt) candidates: ${CANDIDATES[*]}"

for sha in "${CANDIDATES[@]}"; do
  echo "::group::Building & validating simc image $sha"

  # Fetch the CI profile AT THIS COMMIT, so the gate always matches the build.
  # CI.simc is self-contained (no includes), so a single file is enough — no need
  # to bloat the runtime image with upstream's profiles/ directory.
  if ! curl -fsSL -o CI.simc \
        "https://raw.githubusercontent.com/simulationcraft/simc/$sha/profiles/CI.simc"; then
    echo "::warning::could not fetch profiles/CI.simc at $sha; trying older"
    echo "::endgroup::"
    continue
  fi

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

  if bash "$HERE/validateSimc.sh" ./docker-simc.sh CI.simc; then
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

  echo "::warning::simc commit $sha failed validation; trying older"
  echo "::endgroup::"
done

echo "::error::No sampled simc commit passed validation; kept existing :latest so the collector stays on the last good image." >&2
exit 1
