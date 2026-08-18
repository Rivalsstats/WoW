---
name: keystone-guru-mapping-data
description: What route_data.mapping_version means and how the "Most Skipped NPCs" card is computed today. Use when touching mapping_version handling, aggregated_npc_skip_rates, or the Most Skipped card in generateDungeonPages.py / dungeon_page.html.
---

# keystone.guru Mapping Data / Most Skipped

**How Most Skipped works:** `aggregated_npc_skip_rates` (table + nightly rebuild in `backend_scripts/database.sql`,) counts, per `dungeon_id`+`npc_id`, `total_encounters` (routes that pulled that npc type) vs `total_routes`. `fetch_dungeon_skip_rates` (`databaseConnector.py`, `FETCH_DUNGEON_SKIP_RATES_SQL`) derives inclusion percentage plus `max_key_played` / `max_key_skipped` by presence/absence of the npc in `pull_enemies` across `route_data`. `generateDungeonPages.py` shows the lowest-inclusion npcs. This is entirely run-derived, consistent with the API being type-level only (see [[keystone-route-api-limits]]).

**`route_data.mapping_version` is stored and per-dungeon.** It comes from the keystone route API `mappingVersion` field (`fetchRouteData.py`, `collectLeaderboardData.py`) and is the per-dungeon version number (5, 6, 7...), NOT the global `mapping_version_id` (600-800 range). If you ever re-introduce seeder matching, match against the seeder `version` field, not the folder number. Today mapping_version is only carried through top-routes queries.

**`data/boss_npcs.json`** is used to validate the Bloodlust timeline (`generateDungeonPages.py`), not to exclude bosses from a skip roster.
