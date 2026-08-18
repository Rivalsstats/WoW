---
name: aggregation-pipeline
description: How the nightly aggregation runs and how to add a new aggregate. Use when editing aggregation stored procs (sp_agg_*, sp_run_agg_pipeline, sp_swap_public_table) in backend_scripts/database.sql, adding an aggregate table, or debugging morning lock-ups / RENAME contention.
---

# Aggregation pipeline (shadow swap)

The per-table aggregation events were replaced by stored procedures (`sp_agg_*`) run sequentially by `ev_nightly_agg_pipeline` -> `sp_run_agg_pipeline`. All of this now lives directly in `backend_scripts/database.sql` (verify with `grep sp_run_agg_pipeline backend_scripts/database.sql`). The one-off `backend_scripts/migrations/` files the original notes referenced have been folded into `database.sql` and the migrations dir is now empty. Per-step timing/errors go to `agg_pipeline_log`.

Rebuilds use shadow tables (`<t>_new`) plus atomic `RENAME`, never `TRUNCATE`. Reason: a `TRUNCATE`'s exclusive metadata-lock request behind a long reader wedged the whole server (MDL waits use `lock_wait_timeout`, which defaults to one year), which caused the morning lock-ups. FKs on `aggregated_*` tables were dropped so `CREATE TABLE ... LIKE` shadows work.

How to add a new aggregate: write a new `sp_agg_<step>` proc plus one `CALL sp_run_agg_step('<step>')` line in the pipeline. Never add a new standalone `TRUNCATE` event. Route table swaps through `sp_swap_public_table(p_base)`, which does the `RENAME`+`DROP` with escalating `lock_wait_timeout` (60->300s), logs the blocking session into `agg_lock_diag` via `sp_capture_lock_holders`, and after 3 failed attempts calls `sp_kill_lock_holders`, which KILLs only idle (`PROCESSLIST_COMMAND='Sleep'`) MDL holders so it can never hit the always-active collector. Use a short `lock_wait_timeout` plus retry (`sp_run_agg_step` retries on 1205 with 30s backoff, 5 attempts), NOT a long timeout, since a long timeout parks the exclusive RENAME in the fair MDL queue and stacks readers behind it (the freeze).

Read-only Python scripts must call `databaseConnector.configure_read_session(conn, cursor)` right after opening the connection, see [[pooled-connection-gotchas]]. Gear-joined aggregates must be full rebuilds, see [[gear-data-retention]].

Auditing lesson: the server can hold events/tables not in `database.sql` (a lost heatmap aggregation was found this way). When auditing, compare `information_schema.events` / `SHOW TABLES` against the repo file. Verify proc changes with [[verify-database-sql]].
