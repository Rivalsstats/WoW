---
name: runs-per-dungeon-rollup
description: The aggregated_runs_per_dungeon_per_level nightly rollup replaces full-season runs scans, and fetchers fall back to a live scan when it is missing/empty. Use when editing fetch_runs_per_dungeon* / fetch_total_season_runs in databaseConnector.py, the runs rollup proc, or the overview-image query paths.
---

# Runs-per-dungeon rollup

`fetch_runs_per_dungeon`, `fetch_runs_per_dungeon_per_level[_above_level]`, and `fetch_total_season_runs` read the ~240-row nightly rollup `aggregated_runs_per_dungeon_per_level` (built by `sp_agg_runs_per_dungeon_per_level`, registered in `sp_run_agg_pipeline`, current season only, shadow+RENAME like [[aggregation-pipeline]]). This keeps the dashboard/index/dungeon/item steps off the full-season `runs ⋈ dungeon_data` scans (`DUNGEON_UPGRADES[_PER_KEYLEVEL]_SQL`, ~2-3 min each at 10-20M runs), which would otherwise run ~12x per build (worst case `createDungeonOverviewImg` scanning per level once per dungeon).

Why: the DB runs at 100% load and cannot be scaled, so every redundant query removed is a direct win. Query parallelism is deliberately avoided for the same reason (keystone.guru thumbnail HTTP is threaded instead, zero DB load). Thumbnails cannot be cached across builds (they differ every time).

How to apply: each fetcher falls back to the legacy scan when the rollup table is missing OR empty for the requested season, via `_fetch_runs_rollup_with_fallback` in `databaseConnector.py`. So builds work both before and after the initial populate. The overview-image renderers accept pre-fetched data via keyword args (`None` = fetch, preserving standalone social-post callers), and the page generators pass everything through so image steps do zero duplicate queries (verified byte-identical both paths).

The proc and table live in the main schema file `backend_scripts/database.sql` (the `migrations/` dir is empty). Adoption-rate denominators that consume these globals are in [[item-page-aggregate-perf]].
