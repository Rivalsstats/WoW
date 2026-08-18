---
name: item-source-filters
description: Covers the items page "Drops from" source filter, its flat source-token spine, and raid metadata discovery. Use when editing generateItemPages.py, pageGeneration.build_source_lookups, assets/js/items.js, or backend_scripts/fetchRaidData.py.
---

# Items Page "Drops from" Source Filter

The items browse-page "Drops from" filter is a bootstrap-select multi-select with OR matching. Dropdown order (`buildSourceOptions` in `assets/js/items.js`): Crafted / Tier Set / PvP, divider, a "Dungeons" optgroup, one optgroup per raid ("All of <raid>" + one per boss), divider, then Other. Only tokens a rendered manifest item actually carries are offered, so empty dungeons/raids never appear.

## Source-token spine
- Each item carries a flat source-token array in `assets/json/items_index.json` `sources`. Tokens: `d:<dungeonKey>`, `r:<raidInstanceId>`, `b:<raidInstanceId>:<encounterId>`, `crafted` (has `profession`), `tier` (synthetic Raidbots instanceId **-87**), `pvp` (synthetic **-85**), `other`. A raid item carries both its `r:` and each `b:` token so whole-raid and per-boss selections match.
- Constants `TIER_SET_INSTANCE_ID = -87`, `PVP_INSTANCE_ID = -85` is defined in `backend_scripts/pageGeneration.py::build_source_lookups` if there is ever changes or new additions change it and this skill.
- Frontend: `assets/js/items.js` (`buildSourceOptions`/`applyFilters`/`parseSourceParam`). URL is `?source=` comma-separated readable slugs (boss = `<raidSlug>--<bossSlug>`; categories literal `crafted`/`tier`/`pvp`/`other`). Legacy single dungeon slug still resolves.

## Raid metadata (do NOT use Raider.IO for raids)
- `backend_scripts/fetchRaidData.py` writes `data/static/raids.json` (shape mirrors dungeons.json, keyed by journal instance id, `bosses` keyed by encounter id). Wired into `getStaticData.yml` right after fetchDungeonData.
- GOTCHA: Raider.IO raiding static-data returns a Raider.IO raid id, NOT the Blizzard journal instance id, so it never joins to the loot. Discover raids straight from Raidbots loot instead: `expansion_id = wowBuild major - 1`; candidate raid instances = positive `sources[].instanceId` from current-expansion items minus dungeon journal ids; confirm/label each via Blizzard `journal-instance/{id}` (keep `category.type == "RAID"`). Both instance and boss ids stored are Blizzard journal ids.
- `raids.json` is optional (absent pre-raid-release): `generateItemPages` degrades to dungeon/crafted/other with a notice, not a crash. Raids are genuinely optional so warn-not-raise is intentional here.

Item→dungeon resolution shares the journal bridge in [[dungeon-best-loot-card]]. See also [[frontend-json-dungeon-keys]].
