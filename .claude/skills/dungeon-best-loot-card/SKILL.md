---
name: dungeon-best-loot-card
description: Covers the dungeon page "Best Loot From This Dungeon" card and item→dungeon source resolution. Use when editing generateDungeonPages.py, the dungeon-best-loot section of templates/dungeon_page.html, or wiring item drops via journal_instance_id.
---

# Dungeon "Best Loot From This Dungeon" Card

The dungeon page right-column card (template id `dungeon-best-loot`, `templates/dungeon_page.html`) lists gear that drops in the dungeon, ranked by how much the current meta equips it.

## How the loot data works (no new fetcher)
- `data/static/equippable-items.json` (pulled from Raidbots in `.github/workflows/getStaticData.yml`) ships a per-item `sources` array `[{instanceId, encounterId}]`. `instanceId` = Blizzard Dungeon Journal instance id (positive = dungeons or raids, negative = world/other).
- Bridge to our dungeons (keyed by `challenge_mode_id`): `fetchDungeonData.py` persists `journal_instance_id` (from `data["dungeon"]["id"]`) into `dungeons.json`. User validated `journal_id` == Raidbots `instanceId`.
- `generateDungeonPages.py` builds a `journal_instance_id -> dungeon` reverse map, groups items by dungeon, and ranks by a global item-usage sweep reusing `fetch_item_spec_usage` (sum across specs = equipping runs, argmax spec = "most used by"). Top 6 go to `top_loot` in the template.

## Gotchas (each bit once, keep)
- TYPE MISMATCH: `global_aggregated_items.item_id` is `varchar(100)`, so usage rows return `item_id` as a STRING while `equippable-items.json` ids are JSON ints. The loot join MUST normalise (`int(row['item_id'])`) or every lookup misses and the card renders "No tracked loot".
- EMPTY-STATE + DataTables: render `<table id=best-loot-table>` only `{% if top_loot %}`, else a plain `<p>`. A `<td colspan>` placeholder row throws DataTables "Requested unknown parameter".
- `fetch_dungeon_totals` is NOT orphaned: `createDungeonOverviewImg(dungeon_totals=local_total_res)` needs its rows. Removing `local_total_res = fetch_dungeon_totals(...)` breaks social image generation with `NameError`.

Item source tokens for the items page reuse the same journal bridge, see [[item-source-filters]].
