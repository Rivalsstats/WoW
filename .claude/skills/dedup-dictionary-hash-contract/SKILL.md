---
name: dedup-dictionary-hash-contract
description: Talents and bonus ids are stored deduplicated in content-hash dictionaries (talent_sets / bonus_sets) whose set_id must be produced identically by the Python collector and by any SQL that writes them, plus the purge-NULL parity rule that keeps talent aggregations correct. Use when touching commonUtils.talent_set_hash / bonus_set_hash, the members.talent_set_id / equipment.bonus_set_id write path, the talent/bonus aggregation readers or their orphan sweeps, the purge proc, or any one-time SQL that backfills the dictionaries.
---

# Dedup dictionary content-hash contract

Talent selections and equipped-item bonus-id combos are stored **once** in content-hash
dictionaries, not repeated per member / per item:

- `talent_sets (set_id BINARY(16), tree, talent_id, rank)` — one `set_id` covers a member's
  full class+spec+hero node set (tree 0=class, 1=spec, 2=hero). `members.talent_set_id`
  references it.
- `bonus_sets (set_id BINARY(16), bonus_id)` — one `set_id` covers an equipped item's whole
  bonus-id set. `equipment.bonus_set_id` references it.

## The set_id is a shared canonical hash — collector and SQL must agree

`set_id = UNHEX(MD5(canonical_string))`. The Python collector helpers
`commonUtils.talent_set_hash` / `bonus_set_hash` and **any** SQL that writes a dictionary (e.g. a
one-time backfill migration) MUST produce byte-identical `set_id`s, otherwise a collector insert
after a backfill lands on a different id than the backfill wrote and the same content splits into
two dictionary entries. The canonical strings are pinned in the two helper docstrings:

- **talents**: each row rendered `tree:talent_id:rank`, sorted by `(tree, talent_id)` ascending,
  joined by `,`. SQL equivalent:
  `MD5(GROUP_CONCAT(CONCAT_WS(':', tree, talent_id, rank) ORDER BY tree, talent_id SEPARATOR ','))`.
- **bonus**: distinct `bonus_id`s ascending, joined by `,`. SQL equivalent:
  `MD5(GROUP_CONCAT(bonus_id ORDER BY bonus_id SEPARATOR ','))`.

Always `SET SESSION group_concat_max_len = 1000000` before any grouping — a full talent set
exceeds the 1 KB default and would silently truncate into wrong hashes. An empty set hashes to
`NULL`: no dictionary row, and the reference column stays `NULL`.

## No FK; orphans swept in the aggregation cycle

Deliberately **no** FK from `members.talent_set_id` / `equipment.bonus_set_id` to the
dictionaries — an FK would block the season wipe's `TRUNCATE`. Correctness is kept by collector
plus an orphan sweep run each aggregation cycle: `sp_agg_talent_sets_gc` /
`sp_agg_bonus_sets_gc` anti-join the dictionary against its referencing column and delete
unreferenced sets. Both are wired into `sp_run_agg_pipeline` via `sp_run_agg_step`. Season wipe
truncates `talent_sets` / `bonus_sets` in `sp_season_wipe`.

## Purge-NULL parity: members are kept and NULLed, equipment is deleted

The daily purge proc keeps a purged member's row (run_members / comps still reference it) but
DELETEs its `equipment` rows. So to drop a purged member out of the talent aggregations, the purge
sets `members.talent_set_id = NULL` — it does **not** delete the member. That NULL reproduces
exactly what deleting the old per-member talent rows used to do: the member falls out of the
talent aggregations because they **INNER JOIN** `talent_sets ON set_id = members.talent_set_id`.
Any new talent aggregation must INNER JOIN the dictionary for this to hold — a LEFT JOIN would
wrongly keep purged members. Bonus needs no NULL step: `equipment` is deleted outright, so its
orphaned `bonus_sets` rows are reclaimed by `sp_agg_bonus_sets_gc`. The orphaned `talent_sets`
rows a NULLed pointer leaves behind are likewise reclaimed by `sp_agg_talent_sets_gc`.

Related: [[gear-and-talent-data-retention]] (why these aggregations must be full-rebuild),
[[aggregation-pipeline]] (how the sweep steps run), [[season-wipe-invariants]] (why no FK).
