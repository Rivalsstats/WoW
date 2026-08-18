---
name: enchant-lookup-filter
description: Dropping enchant IDs absent from data/static/enchantments.json is the INTENTIONAL mechanism for hiding old/unusable enchants, not a sign of missing raidbots data. Use when enchant entries look missing on a spec page or when editing fetch_enchant_info in generateSpecPages.py.
---

# Enchant Lookup Filter Is Intentional

Dropping enchant IDs that are not in `data/static/enchantments.json` (spec page build, `fetch_enchant_info` in `backend_scripts/generateSpecPages.py`, ~line 1629) is the *intended* mechanism for hiding old-expansion enchants people still have equipped on normally-unenchantable slots (HANDS/WAIST/WRIST noise). The user rejected building supplement files or wago.tools imports for IDs absent from the raidbots file: "raidbots is not missing any data." A warning log for each skipped ID (`generateSpecPages.py` ~line 1650) is all that is wanted. Do NOT propose auto-generating enchant metadata from other sources.

**Why:** the 1%-popularity threshold was originally added for the same noise-suppression reason. Lookup filter + threshold together keep junk slots hidden.

**How to apply:** If enchant entries seem missing from a spec page, check the popularity thresholds FIRST. They must use the 14-day `fetch_spec_sample_size` denominator (`generateSpecPages.py` ~line 2235), never the season-wide `fetch_runs_per_spec` (~line 2325). See [[gear-data-retention]] for why equipped-gear counts are 14-day only. Only after ruling out the threshold should you suspect the lookup file, and even then absent IDs are usually deliberate noise suppression.
