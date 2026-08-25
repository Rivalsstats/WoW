---
name: keystone-guru-mapping-data
description: What route_data.mapping_version means and how the "Most Skipped NPCs" card is computed today. Use when touching mapping_version handling, aggregated_npc_skip_rates, or the Most Skipped card in generateDungeonPages.py / dungeon_page.html.
---

# keystone.guru Mapping Data / Most Skipped

**How Most Skipped works:** `aggregated_npc_skip_rates` (table + nightly rebuild in `backend_scripts/database.sql`,) counts, per `dungeon_id`+`npc_id`, `total_encounters` (routes that pulled that npc type) vs `total_routes`. `fetch_dungeon_skip_rates` (`databaseConnector.py`, `FETCH_DUNGEON_SKIP_RATES_SQL`) derives inclusion percentage plus `max_key_played` / `max_key_skipped` by presence/absence of the npc in `pull_enemies` across `route_data`. `generateDungeonPages.py` shows the lowest-inclusion npcs. This is entirely run-derived, consistent with the API being type-level only (see [[keystone-route-api-limits]]).

**`route_data.mapping_version` is stored and per-dungeon.** It comes from the keystone route API `mappingVersion` field (`fetchRouteData.py`, `collectLeaderboardData.py`) and is the per-dungeon version number (5, 6, 7...), NOT the global `mapping_version_id` (600-800 range). If you ever re-introduce seeder matching, match against the seeder `version` field, not the folder number. Today mapping_version is only carried through top-routes queries.

**`data/boss_npcs.json`** is used to validate the Bloodlust timeline (`generateDungeonPages.py`), not to exclude bosses from a skip roster.

**The Most Lusted Pulls signature encodes per-NPC counts.** A pull's identity in `FETCH_DUNGEON_LUST_TIMELINE_SQL` is `GROUP_CONCAT(CONCAT(npc_id, ':', count) ORDER BY npc_id ASC SEPARATOR ',')` (`npc_id:count` tokens), so pulls with the same enemy types but different counts are distinct rows. That signature is a byte-for-byte join key: the identical expression must appear in the `HAVING` clauses of `FETCH_EXAMPLE_LUST_ROUTE_SQL` and `FETCH_EXAMPLE_LUST_ROUTE_ARM_SQL`, or the example-route links stop resolving. `generateDungeonPages.py` splits the `top_npcs` string on `,` then each token on `:` to recover the npc_id for boss validation and name/icon self-heal, and `dungeon_page.html` renders the count as an `Nx` prefix when it exceeds 1.

**Per-NPC portrait icons** on these cards are sourced via MDT displayId → Wowhead zamimg webthumb; see [[dungeon-npc-portrait-icons]] before changing the creature-image source.
