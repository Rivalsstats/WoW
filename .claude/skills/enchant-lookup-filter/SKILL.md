---
name: enchant-lookup-filter
description: How enchants are filtered for display (catalog membership + current expansion + equipRequirements slot fit) through one shared commonUtils util used by both the spec and item pages. Use when enchant entries look missing on a spec or item page, or when editing enchant filtering in generateSpecPages.py / generateItemPages.py / commonUtils.py.
---

# Enchant Display Filter Is Intentional And Shared

Enchant relevance for display is decided by one shared predicate,
`commonUtils.is_enchant_relevant(record, current_expansion, slot_group)`, called by BOTH the
spec page (`fetch_enchant_info` in `backend_scripts/generateSpecPages.py`) and the item page
(the enchant ingestion loop in `build_payloads` in `backend_scripts/generateItemPages.py`). It
gates on three things, all intentional:

- **Catalog membership.** An id absent from `data/static/enchantments.json` is dropped. This is
  the *intended* mechanism for hiding old-expansion enchants people still have equipped on
  normally-unenchantable slots (HANDS/WAIST/WRIST noise). The user rejected building supplement
  files or wago.tools imports for absent ids: "raidbots is not missing any data." The spec page
  keeps a warning log for each skipped-because-absent id, item-page drops are silent. Do NOT
  propose auto-generating enchant metadata from other sources.
- **Current expansion.** A catalog record whose `expansion` is not the current expansion is
  dropped. Records with no `expansion` key are expansion-agnostic (the DK weapon runes) and are
  always kept. The catalog legitimately holds multiple expansions at once, so membership alone
  does NOT hide last-expansion enchants, the expansion gate does.
- **equipRequirements slot fit.** `commonUtils.enchant_slot_groups(record)` decodes
  `equipRequirements` (`itemClass` weapon/armor/profession-tool, armor `invTypeMask` bit index =
  Blizzard inventoryType) to the set of slot_group tokens the enchant is valid for. An enchant
  is kept under a slot_group only if that group is in the set. Off-hand inventory types collapse
  to `WEAPON`, matching the DB `slot_group` namespace (`SLOT_GROUP_MAP` maps MAIN_HAND/OFF_HAND
  to WEAPON). Profession tools and gems (null `equipRequirements`) yield an empty set and drop
  from gear enchant lists.

`enchant_slot_pos` (enchant display ordering) lives in `commonUtils` too, built on
`enchant_slot_groups` so ordering and filtering share one source and cannot diverge. It is
re-exported from `generateSpecPages.py` (with `INVTYPE_DISPLAY_ORDER` and the `ENCHANT_CLASS_*`
constants) for existing callers. It still raises on an unknown catalog shape, but only ever sees
post-filter records, so a raise means the catalog grew a shape the map does not model.

**Current expansion is read offline** via `commonUtils.current_expansion_id()`, which reads
`expansion_id` from `seasonInfo.json` (parallel to `current_season_id` reading
`blizzard_season_id`). `fetchSeasonAndPeriodInfo.py` writes that field, deriving it once from
`commonUtils.derive_expansion_id()` (a network call). Generators stay offline and never call
`derive_expansion_id` themselves. If a build fails on `expansion_id missing from seasonInfo.json`,
the season fetch has not run since the field was added, re-run it or add the field to the
committed `data/static/seasonInfo.json`.

**Why:** the spec page's 1%-popularity threshold was originally the only noise-suppression, and
it only worked on the spec page. The shared expansion + equipRequirements filter makes the item
page hide the same old/incompatible enchants, and makes the rule explicit rather than a side
effect of popularity.

**How to apply:** If enchant entries seem missing from a spec page, check the popularity
thresholds FIRST. They must use the 14-day `fetch_spec_sample_size` denominator (in
`generateSpecPages.py`), never the season-wide `fetch_runs_per_spec`. See
[[gear-data-retention]] for why equipped-gear counts are 14-day only. If an enchant is missing
from a spec or item page and threshold is not the cause, confirm its catalog record's
`expansion` matches `current_expansion_id()` and that its `equipRequirements` map to the
slot_group it should show under (`enchant_slot_groups`). Absent-from-catalog and wrong-expansion
drops are usually deliberate noise suppression.
