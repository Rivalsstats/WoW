---
name: season-wipe-invariants
description: Four hard-won invariants of the season-rollover wipe handshake. Use when editing wipe_watch/WriteGate in collectLeaderboardData.py, sp_season_wipe / sp_truncate_with_retry / ev_season_wipe in database.sql, or the SEASON_FLOOR regional-rollover guard.
---

# Season Wipe Invariants

Four non-obvious invariants of the season-rollover wipe. Break any of these and collection halts silently or the wipe deadlocks with no error. See [[season-rollover-wipe]] for the overall handshake.

1. **`wipe_watch` owns the only path that un-pauses `WRITE_GATE`.** If it dies, collection halts silently forever, because `asyncio.gather(..., return_exceptions=True)` in `main()` swallows the traceback and the failsafe lives in the same loop. Every tick is individually try/except'd and a `finally` resumes the gate on any exit. Never add an un-guarded `await` to that loop (`collectLeaderboardData.py`, `wipe_watch()`).

2. **`TRUNCATE` blocks on the metadata lock, governed by `lock_wait_timeout` (default 1 year), NOT `innodb_lock_wait_timeout`.** Setting only the latter let one idle-in-transaction session hang `ev_season_wipe` forever while holding `GET_LOCK('agg_pipeline')`, stalling the nightly pipeline and member purge with no error raised. `sp_truncate_with_retry` in `database.sql` sets `lock_wait_timeout`, escalates the budget, logs blockers, kills idle holders, then SIGNALs so the event releases the lock and retries.

3. **The collector resolves season and active period exactly once, before `realm_poller`'s loop.** Resuming after a wipe would re-insert old-season rows, and since `done_season == current` blocks any further wipe they would survive the whole season. So `wipe_watch` sets `restart_event` instead of resuming, and `restart: always` brings the process back. This is also the only thing that picks up a new weekly period.

4. **Partial regional rollover:** each region resolves its OWN `get_current_season_id`, so US resets first and triggers the wipe while EU/KR/TW still return the old season. Module global `SEASON_FLOOR` (read once at startup from `_wipe_state` in `collectLeaderboardData.py`) makes `main()` skip creating a `realm_poller` for any region whose `current_season < SEASON_FLOOR`, and `process_batch` skips rows with `int(r["season"]) < SEASON_FLOOR`. `0` = no floor (no-op on old DBs). Lagging regions self-heal on a later restart once they roll over.

Related: [[simc-chunked-checkpoint]].
