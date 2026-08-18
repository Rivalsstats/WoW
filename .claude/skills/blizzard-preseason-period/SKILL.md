---
name: blizzard-preseason-period
description: Blizzard's season API lists a phantom pre-season period ending exactly at season start; it is filtered out to keep week numbering aligned. Use when week numbers look off by one, or when touching fetchSeasonAndPeriodInfo.py, periods.json, or season_periods.
---

# Blizzard Pre-Season Period Quirk

Blizzard's mythic-keystone season details API includes the period that *ends exactly at the season start* (season 17: period 1055 in all regions). It has zero runs, so tables/queries that derive week numbers from `season_periods` counted a phantom "week 1" while ordinal-position-based views (aggregated_key_throughput, the "Key Throughput" chart) did not. The dashboard's "Keys per Week" started at Week 2 versus Week 1 on keys/min.

Since 2026-07-19, `backend_scripts/fetchSeasonAndPeriodInfo.py` skips periods where `end_timestamp <= season_start` (verified around line 182, applied both to `periods.json` and the `season_periods` insert via `databaseConnector.insert_season_periods`). A one-time migration `2026-07-19_remove_preseason_periods.sql` deleted the already-inserted season-17 row.

Patch annotations index into the filtered `periods.json`, so all three week axes agree. If week numbering ever looks off by one again at a new season, check for this quirk first. Note the keys-per-week SQL also silently drops any zero-run week, which would compress its axis.
