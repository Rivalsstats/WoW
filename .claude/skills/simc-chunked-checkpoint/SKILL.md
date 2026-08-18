---
name: simc-chunked-checkpoint
description: simc BiS runs are chunked and checkpointed to simc_bis_progress so heavy specs resume across the ~daily collector restart. Use when editing run_simc_bis / _build_run / prep_snapshot / pick_next_spec in simcBis.py, or the simc_bis_progress[_meta] tables.
---

# simc Chunked Checkpoint / Resume

The collector container restarts ~daily. On a 2-core host simc is pinned to ONE core (docker-compose `SIMC_CPUSET=1`/`THREADS=1`/`CPUS=1`, to protect co-located MySQL), so the heaviest specs (Balance/Guardian Druid) need more than one container lifetime to sim all profilesets.

**Why not just raise the timeout:** a per-spec timeout ABOVE the restart interval is worse than a short one. `pick_next_spec` orders by `updated_at`, only written on completion or a graceful timeout. A run killed mid-flight by the restart never writes it, so that spec is re-picked first on next boot, restarts from zero, and permanently head-of-line-blocks every other spec.

**Design (`backend_scripts/simcBis.py`, `run_simc_bis`):** a spec is simmed in chunks of `SIMC_CHUNK_SIZE` (default 64) run back-to-back until the spec completes (one prep per run), each chunk checkpointed to `simc_bis_progress` (per-profileset means) + `simc_bis_progress_meta` (header). A killed chunk loses only itself. Key pieces:

- `_build_run`: deterministic combos. `tier_slots` MUST be sorted there, because `detect_tier` returns a set and Python's per-process string-hash randomisation would otherwise reorder the profile and change the signature every restart.
- `run_signature` = sha256 of the full .simc text, EXCLUDING the simc build so 6-hourly image pulls do not nuke a run.
- **`prep_snapshot`** (`_load_prep_snapshot`): JSON of header/candidates/baseline/tier/active_slots stored in `progress_meta`. Resume rebuilds the run from the snapshot, NOT from re-prepared data, because candidate bags come from nightly-rebuilt popularity aggregations whose drift would otherwise mismatch the signature and discard all banked chunks. A signature mismatch now only means the profile-gen CODE changed (a legit reset).
- **`failed` flag**: a genuinely failing chunk sets it and the spec queues by `last_attempt_at` (back), while an unfailed in-progress run queues by `started_at` (front, resumed immediately after restart). A SIGTERM-killed chunk checks `cancel_event` and leaves the checkpoint untouched (NOT failed).
- `_finalize_run` is idempotent (all-banked + stored baseline finalizes with zero sims). `SIMC_RUN_TIMEOUT` (default 8h) bounds ONE chunk and must stay below the restart interval.

Only rank-1/best-combo is consumed by any page. Deeper per-slot ranks are stored but dead. See also [[pooled-connection-gotchas]] and [[simc-baseline-vs-page-gear]].
