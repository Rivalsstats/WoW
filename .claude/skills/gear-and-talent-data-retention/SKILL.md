---
name: gear-and-talent-data-retention
description: Equipped-gear rows and talent information are purged after ~2 weeks, so aggregations must be full rebuild, never watermark-incremental. Use when adding or editing any aggregation that joins through the equipment table (embellishments, missives, enchants, item aggregates) or talent tables (class_talents, spec_talents, hero_talents) in backend_scripts/database.sql.
---

# Gear and talent data 2-week retention

Equipped-gear data (equipment / bonus_ids rows) and talent information older than ~2 weeks is discarded from the MythiStone DB.

Why it matters: watermark-incremental aggregation (the `summary_meta` pattern) accumulates counts forever, so purged runs would stay in the aggregates indefinitely and inflate them. Any aggregation that joins through `equipment` must be a full rebuild (shadow table + RENAME, or 14-day batched rebuild) so purged data drops out.

How to apply: when adding new aggregation over gear data, never propose the watermark pattern. If you find any remaining watermark aggregation over gear talents or other character specific information that is used to display the current state/suggestions instead of data across the season, that is a bug to fix, not a model to copy. Full-rebuild procs run through the shadow-swap pipeline, see [[aggregation-pipeline]].

The aggregation procs also ignore anything older than 14 days on the read side (seeded test runs must be timestamped inside the last 14 days for the same reason, see [[local-test-render]]). This retention is also why gear aggregates cannot be summary_meta-incremental even for performance, see [[item-page-aggregate-perf]].
