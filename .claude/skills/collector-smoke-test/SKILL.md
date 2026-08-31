---
name: collector-smoke-test
description: The collector image push is gated on a runtime smoke test that runs the built image against a seeded throwaway DB (with a real spec-62 slice) and must actually collect + run simc. Use when changing the collector (collectLeaderboardData.py, entrypoint.sh, Dockerfile, simcBis.py, databaseConnector collector paths, shipped data/static) or its build, and ALWAYS update the smoke test in lockstep with a collector change.
---

# Collector image smoke-test gate

`buildCollectorImage.yml` builds the collector image but **only pushes `:latest`/`:sha` after a
`smoke-test` job passes** (`build-and-push` has `needs: smoke-test`). Deployment is pull-based
Watchtower on `:latest` (see `docker-compose.yml`), so gating the push IS the deploy gate: a build
that imports cleanly but breaks at runtime never reaches the running collector. `docker build`'s only
check is the static `verifyImageImports.py`; the smoke test catches runtime breakage it cannot.

## Keep the smoke test in sync with the collector

Any change to the collector's runtime contract must be mirrored in the smoke test, or the gate goes
stale and silently stops protecting deploys:

- New required env in `entrypoint.sh` (its `REQUIRED` array or region loop) → add it to the smoke
  env in the workflow's "Run collector smoke test" step and, if it must reach the container, to
  `FORWARD_ENV` in `backend_scripts/localDev/collector_smoke.py`.
- Changed startup flow in `collectLeaderboardData.py` → the smoke test's `STARTUP_MARKER`
  (`Starting data collection for regions:`, emitted via `stats.console_log`) must still print on a
  clean start; update the marker if that line changes.
- New external dependency or data file → also update the `Dockerfile` COPY blocks and this
  workflow's `on.push.paths` (both the collector-source list and the smoke files), the same
  discipline the `verifyImageImports.py` preflight enforces.
- Changed simc profile generation in `simcBis.py` → the real-data simc gate below is what proves it
  still produces a valid profile; keep the spec-62 slice and the success signal working.

## How the smoke test runs

`collector_smoke.py` assumes the test DB is already seeded, then `docker run -d` the built image with
`--network host` (reach the seeded MySQL at `127.0.0.1:3399`), `-v /var/run/docker.sock` and a shared
named volume at `/app/data/simc_io` (so simc sibling containers work as in production), polls for
`--seconds` (default 360), always dumps `docker logs`, and tears the container + any
`mythistone.role=simc-sim` siblings down. It touches the **test DB only**.

## Two decoupled gates, both auto-relaxing

Always required (any build failing these is broken): no crash-exit, no `Traceback` /
`CRITICAL STARTUP ERROR` in logs, and the startup marker appears.

Conditionally required, so off-season / missing-live-DB never false-fails:

- `--require-rows` comes from `seasonHasData.py`'s `has_data` (read-only live-DB check, same as
  buildPages) → the `runs` COUNT must grow (the collector actually collected live leaderboard data).
- `--require-simc` comes from `seed_test_db.py --simc-live-spec`'s `simc_live_seeded` output → a real
  simc chunk must SUCCEED.

## Real spec-62 data is what makes the simc gate meaningful

Synthetic seeded gear/talents make **every** simc chunk fail at init (bad talent hashes, invalid
item/slot combos), which would mask a real profile-generation bug. So the smoke test seeds a real
slice: `seed_test_db.py --simc-live-spec 62` opens a READ-ONLY connection to the live DB
(`LIVE_DATABASE_*`, mapped from the same `DATABASE_*` secrets, never written) via
`simc_live_seed.py`, pulls ~50 recent current-season Arcane Mage (spec 62) runs with their roster
members, equipment, enchantments, sockets, and referenced `talent_sets`/`bonus_sets`, **purges the
synthetic spec-62 rows first** (so its aggregates are pure real, since `simcBis` picks the most
popular item per slot), preserves the live auto-increment ids verbatim (synthetic ids start at 1, so
no collision), then rebuilds aggregates via `sp_run_agg_pipeline`. Only spec 62 is swapped; other
specs stay synthetic and keep failing simc init, which is fine. See
[[dedup-dictionary-hash-contract]] and [[gear-and-talent-data-retention]].

Success is detected clock-skew-immune: `collector_smoke.py` snapshot-diffs the
`(spec_id, season, updated_at)` tuples of `simc_bis_meta` rows with `baseline_dps > 0`, on a fresh
pooled connection each read (the REPEATABLE-READ trap from [[pooled-connection-gotchas]]). `simcBis`
delete+reinserts a spec's meta with a new `updated_at` and a positive `baseline_dps` only on a real
success, so a new tuple means a genuine sim landed; failed/placeholder chunks leave `baseline_dps`
NULL and are ignored.

## Secrets and tuning

Only region-agnostic `BLIZ_CLIENT_ID` / `BLIZ_CLIENT_SECRET` exist as secrets. The collector's
entrypoint wants per-region names, so `collector_smoke.py` derives `BLIZ_CLIENT_ID_<REGION>` from that
single pair for each region in `REGIONS` (an explicitly-set suffixed var wins). CI runs `REGIONS=us`.
`WEBHOOK_URL` is a placeholder (`https://example.invalid/webhook`) so entrypoint's non-empty check
passes without spamming Discord. simc is tuned tiny-but-valid: `SIMC_MAX_COMBINATIONS=3` (exactly 3
combinations, not a full profileset), `SIMC_COMBO_ITERATIONS=200`, high `SIMC_TARGET_ERROR`, so one
spec-62 chunk completes fast within the window.

## Running it

Seed (with the live slice) via [[local-test-render]]'s `seed_test_db.py` plus
`--simc-live-spec 62` and `LIVE_DATABASE_*`, build `mythistone-collector:smoke`, then run
`collector_smoke.py --require-rows true --require-simc true`. `--network host` is limited on Windows
Docker Desktop (the container cannot reach the host MySQL the same way), so the CI Linux run is the
authoritative check. Related simc build knowledge: [[simc-resilient-build]], [[simc-chunked-checkpoint]].
