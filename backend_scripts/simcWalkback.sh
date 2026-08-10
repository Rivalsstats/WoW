#!/usr/bin/env bash
# Shared candidate selection for the two simc walk-backs (buildResilientSimc.sh
# for the tierlist binary, buildResilientSimcImage.sh for the collector image).
#
# Sourced, not executed. Kept in one place deliberately: if the two walk-backs
# ever searched different windows, the binary and the image could end up on
# different simc commits without anything failing.

# select_candidates <candidates-file>
#
# Prints the shas to actually try, newest first.
#
# candidates.txt is newest-first, but compiling a candidate costs ~5 minutes, so
# a linear scan cannot get far. Run 31335443090 broke on a regression 18 commits
# deep (`654ce1df [Unholy] Some pet AI adjustments`), well past the old caps of 6
# and 8 — every candidate in the window was broken and the run failed. Sample
# geometrically instead: offsets 0,1,2,4,8,16,32 reach ~30 commits back in ~7
# compiles. We give up "strictly newest working commit" for "recent and working",
# which is the right trade when upstream has been broken for a day.
select_candidates() {
  local file="$1"
  local -a all=() clean=() picked=()
  local sha offset n

  mapfile -t all < "$file"
  for sha in "${all[@]}"; do
    [ -n "$sha" ] && clean+=("$sha")
  done

  n=${#clean[@]}
  if [ "$n" -eq 0 ]; then
    echo "select_candidates: '$file' has no candidate shas" >&2
    return 1
  fi

  for offset in 0 1 2 4 8 16 32; do
    [ "$offset" -lt "$n" ] && picked+=("${clean[$offset]}")
  done

  # Always finish on the oldest candidate we were given, so a window that is
  # broken everywhere we sampled still tries its floor before giving up.
  if [ "${picked[-1]}" != "${clean[$((n - 1))]}" ]; then
    picked+=("${clean[$((n - 1))]}")
  fi

  printf '%s\n' "${picked[@]}"
}
