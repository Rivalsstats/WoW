---
name: season-rollover-wipe
description: How the automated per-season DB wipe works as a CI-intent + collector-pause + MySQL-event handshake keyed off seasonInfo.json. Use when touching seasonRolloverWipe.yml, requestSeasonWipe.py, wipe_control, ev_season_wipe / sp_season_wipe in database.sql, or the collector WriteGate.
---

# Season Rollover Wipe

The per-season blanket DB clear is a three-actor handshake so it never fights the always-on collector or the nightly agg/purge events.

1. **CI intent**: `.github/workflows/seasonRolloverWipe.yml`. Automated triggers (`workflow_run` after "Gather Static WoW data", Wed `schedule`) run only the read-only `detect` + `notify` (Discord) jobs and can NEVER commit. `requestSeasonWipe.py --commit` runs solely from a manual `workflow_dispatch` whose required `confirm` input must equal the detected `current` season (validated by an `authorize` job).

2. **Collector pause**: `collectLeaderboardData.py`: `WriteGate` (`WRITE_GATE`) + `wipe_watch()` coroutine. While `request_season > done_season` it pauses the DB writers and acks `collector_paused=1` once quiesced. Helpers `read_wipe_control` / `set_collector_wipe_state` live in `databaseConnector.py`.

3. **DB executor**: `ev_season_wipe` event in `database.sql` fires only when pending + `collector_paused=1` + it can take `GET_LOCK('agg_pipeline')`, then `CALL sp_season_wipe()` (blanket TRUNCATE with FK checks off, preserves static/reference tables, resets `summary_meta` pointers) and advances `done_season`.

**Semantics:** `request_season = current` = the season rolled INTO (e.g. 18). Requesting 18 correctly clears the old season-17 data and sets `done_season=18`. It is a boundary marker, not "the season to delete".

**Why keyed off seasonInfo.json, not `MAX(runs.season)`:** the collector flips `runs.season` before `getStaticData` flips seasonInfo.json, and buildPages archives under the seasonInfo id. Wiping on live `runs.season` would let a build overwrite the good archive branch with an empty page.

Runs after [[season-snapshot-archive]]. See invariants in [[season-wipe-invariants]]. Verify SQL changes via [[verify-database-sql]].
