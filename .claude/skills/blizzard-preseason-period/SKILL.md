---
name: blizzard-preseason-period
description: Blizzard's season API lists a phantom pre-season period ending exactly at season start; it is filtered out to keep week numbering aligned. Use when week numbers look off by one, or when touching fetchSeasonAndPeriodInfo.py, periods.json, or season_periods.
---

# Blizzard Pre-Season Period Quirk

Blizzard's mythic-keystone season details API includes the period that *ends exactly at the season start* (for example season 17's period 1055 in all regions). It has zero runs. Left in place, tables and queries that derive week numbers from `season_periods` count a phantom "week 1" while ordinal-position-based views (`aggregated_key_throughput`, the "Key Throughput" chart) do not, so the week axes disagree by one (the dashboard's "Keys per Week" would start at Week 2 against Week 1 on keys/min).

`backend_scripts/fetchSeasonAndPeriodInfo.py` skips periods where `end_timestamp <= season_start`, applied both to `periods.json` and to the `season_periods` insert via `databaseConnector.insert_season_periods`. Patch annotations index into the filtered `periods.json`, so all three week axes agree.

If week numbering ever looks off by one at a new season, check for this quirk first. Note the keys-per-week SQL also silently drops any zero-run week, which would compress its axis.
