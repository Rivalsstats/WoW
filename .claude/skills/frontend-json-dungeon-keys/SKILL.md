---
name: frontend-json-dungeon-keys
description: The in-memory frontend_json keys each comp's dungeons map by INT, but on-disk comps_index.json keys them by STR, so per-dungeon lookups must handle both. Use when reading comp['dungeons'] in compArchetypes.py or generateCompPage.py, or when per-dungeon archetype lists come back empty.
---

# frontend_json Dungeon Keys Are Int (On-Disk They Are Str)

`generateCompPage.calculate_comp_stats` builds each comp's `dungeons` map with INTEGER keys (`dungeon_id = int(row[0])`). When serialized to `assets/json/comps_index.json` those keys become STRINGS (a JSON rule: all object keys are strings).

So any code that consumes the IN-MEMORY `frontend_json` (e.g. `compArchetypes.build_dungeon_archetypes`, called on the in-memory list) must look up `comp['dungeons'][int(id)]`, while code reading the ON-DISK JSON uses string keys. `compArchetypes._load_records` handles both by trying `ctx`, `str(ctx)`, and `int(ctx)`.

WHY it matters: a str-only lookup passed every disk-based test but returned empty for every dungeon in the real build (per-dungeon archetype lists all empty, "Nothing to show").

HOW to apply: when unit-testing anything that reads `frontend_json['dungeons']`, simulate the real generator by converting the loaded JSON's dungeon keys back to int, or the bug stays hidden.

Related archetype ranking lives in [[spec-team-comps]] and feeds [[top-trends-bar]].
