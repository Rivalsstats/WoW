---
name: add-database-table
description: What else to update when you add a new table to backend_scripts/database.sql. Use whenever the schema grows a base table, or when the local seeder fails with UnknownTableError. Covers registering the table in localDev/table_registry.py and seeding id-bearing columns from the same static lookup the template uses.
---

# Adding a table to database.sql

Adding a base table to `backend_scripts/database.sql` is never a one-file change. The local seeder introspects the live schema and calls `table_registry.classify_all`, which raises `UnknownTableError` on any base table it cannot place. So a new table fails the seed loudly until you register it. This guarantee is deliberate: it never ships pages built on a half-populated schema.

Register the table in `backend_scripts/localDev/table_registry.py` by adding its name to the matching set:
- `REFERENCE_TABLES` - a lookup / FK-target table seeded from `data/static`.
- `RAW_TABLES` - a collector detail table the aggregation procs read.
- `STANDALONE_TABLES` - a read table the pipeline does not build (like `top_player_*`, `simc_bis_*`, `trend_snapshot`).
- `CONTROL_TABLES` / `IGNORE_TABLES` - control/watermark or diagnostics/log tables.
- Anything matching `aggregated_*` / `global_aggregated_*` is auto-classified `PIPELINE` (built by `sp_run_agg_pipeline`) and needs no seeder. Same for `*_new` / `*_old` shadow tables (auto `IGNORE`).

If the table is REFERENCE / RAW / STANDALONE, add rows for it in `backend_scripts/localDev/seeders.py` (follow the existing `seed_*` functions) and call it from `seed_test_db.py`.

Critical id-lookup rule: templates subscript their static lookups without a guard, so a page crashes on any id the lookup does not contain (Jinja `dict object has no element`). If the new column carries ids that a template will render, seed it only from the exact same static-file lookup the template uses: items from `equippable-items.json`, enchants by `enchantments.json` `id`, gems by the `itemId` of its `slot=='socket'` entries, talent nodes / hero trees from the processed `data/static/talents/<specId>.json` `talents` / `subTrees`. `seeders.py` already filters against these, so add new id types the same way.

Then verify with [[local-test-render]]. See [[verify-database-sql]] for syntax-checking the schema change itself.
