---
name: keystone-route-api-limits
description: The keystone.guru route API gives only npc_id (creature type) + enemyForces per pull, so per-run pack/position/route-consensus analytics are permanently impossible. Use before proposing pack-popularity, route-consensus, or spatial dungeon-page features.
---

# keystone.guru Route API Is Type-Level Only

The keystone.guru public route API (`/route/{key}`, consumed by `backend_scripts/fetchRouteData.py` and inline in `collectLeaderboardData.py`) exposes, per pull enemy, ONLY `npcId` + the pull's `enemyForces`. The per-instance id (`mdtIndex`/`mdt_id`) is deliberately commented out in their `KillZoneEnemyResource.php`, and there are no coordinates. `pull_spells` is FRIENDLY/player casts (notable markers like Bloodlust/Shroud), not enemy abilities.

Consequence: our stored `pull_enemies` is `npc_id` (creature *type*, which recurs across many physical packs) + count, with no way to tie a run to a specific enemy instance or pack. So anything needing "how often does the community pull THIS pack / this location" (route-consensus maps, pack popularity, spatial death/pull correlation) is **permanently impossible from run data**, regardless of ingesting keystone.guru's static seeders. Their `enemies.json` has `enemy_pack_id`/`lat`/`lng`/`kill_priority`, but the run side has nothing to join on.

Static enemy geometry IS in their open-source seeders (`database/seeders/dungeondata/<exp>/<dungeon>/<floor>/enemies.json`, `enemy_packs.json`), but rebuilding a spatial map duplicates keystone.guru's core product (TOS gray) and still cannot connect to run popularity.

Feasible dungeon-page content is therefore limited to type-level/aggregate: skip rates (via `aggregated_npc_skip_rates`), timing distributions, enemy-forces totals, comp/gear meta, and loot. Do not re-pitch pack/route-consensus ideas.

Related: [[keystone-guru-mapping-data]], [[dungeon-best-loot-card]].
