---
name: simc-resilient-build
description: simc builds validate-and-walk-back over commits instead of trusting raw HEAD, and the gate must SIMULATE a real fight (not just init). Use when editing simcWalkback.sh, validateSimc.sh, the build-simc job in buildPages.yml, or buildSimcImage.yml.
---

# Resilient simc Build / Walk-Back

Both simc builds ship a **validated** binary/image, not the raw tip of simc's `midnight` branch (which IS the live-WoW branch, and simc dropped usable release tags back at BfA, so pinning is not an option). When a build causes an individual spec to fail simc exit 50 aborts everything, every matrix leg dies, assemble/deploy skipped.

Two design errors from the first (broken) gate, both worth remembering:

1. **`iterations=1 max_time=1` is not a gate.** It only catches static action creation. Some spells e.g. `army_ghoul` lives on a dynamically-spawned pet whose actions are created at summon time, so a 1-second fight never reaches it. `validateSimc.sh` runs real fights at both production shapes (`desired_targets=1 max_time=180` and `desired_targets=8 max_time=60`) at `iterations=5`.
2. **Hand-rolled smoke profiles need gear.** Bare actors with no gear or weapon hard-fail init. Both gates now use simc's own `profiles/CI.simc` (actors carry `load_default_gear=1`), self-contained and covering all simmed specs.

**A broken spec can DEADLOCK rather than exit 50.** Every simc invocation (sim legs AND validation) is wrapped in `timeout`; exit 124/137 is a failed candidate, not infra error. Sim legs normally take 2-7 min.

**Walk-back is geometric, not linear** (`backend_scripts/simcWalkback.sh`, sourced by both build scripts): offsets `0 1 2 4 8 16 32` plus the oldest candidate. A ~5 min compile means a linear scan cannot reach a break 18 commits deep. `SIMC_WALKBACK_CAP` (40) is only how many commits we ask for.

**Anti-stall:** build-simc keeps a last-known-good binary via actions/cache as a floor; the image build does not push if everything sampled fails and fails loudly. No spec is ever silently dropped.

**Gotcha:** `if ! cmd; then rc=$?` captures 0, not the real code. Use `cmd || rc=$?`.

Related: [[simc-chunked-checkpoint]].
