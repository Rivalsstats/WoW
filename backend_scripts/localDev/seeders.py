"""Seed randomized-but-plausible data into a fresh Mythistone schema for local renders.

Design notes that make the seeded pages look real rather than uniform noise:

* **Bounded universes.** If every equipment row picked from all ~110k equippable items,
  each item would end up with a run_count of ~1 and every "popularity %" on the page would
  be a meaningless flat distribution. So we build a *small* candidate pool per slot (and a
  handful of talent build variants per spec) up front and draw from them with a Zipf-ish
  weighting, so a few picks dominate exactly like live data.

* **14-day window.** The aggregation procs only count runs whose ``timestamp`` is within the
  last 14 days, so every seeded run is timestamped inside that window or the aggregates come
  out empty.

* **Explicit primary keys.** members / runs / equipment / route pulls are AUTO_INCREMENT,
  but we assign the ids ourselves so children (run_members, equipment, talents, sockets,
  pulls) can be batch-inserted with known parent ids instead of a per-row lastrowid.

Everything is sampled from committed data/static files, so ids (specs, dungeons, items,
talent nodes, enchants, npcs, spells) are real and resolve to real names/icons at render.
The ``loadout`` talent strings are synthetic placeholders: the page generators never decode
them server-side (only the client analyzer does), and the spec-page talent trees are driven
by the seeded class/spec/hero_talents aggregates, not by that string.
"""

import os
import random

import databaseConnector as db
from commonUtils import bonus_set_hash, load_json, talent_set_hash

from loadout_codec import encode_loadout

from table_registry import (
    EQUIPMENT_SLOTS,
    SLOT_GROUP_MAP,
    SLOT_INVENTORY_TYPES,
    ENCHANTABLE_SLOTS,
    SOCKETED_SLOTS,
)

# Blizzard inventoryType -> slot_group, for resolving which slot_group an enchant belongs to.
_INVTYPE_TO_GROUP = {}
for _slot, _itypes in SLOT_INVENTORY_TYPES.items():
    for _it in _itypes:
        _INVTYPE_TO_GROUP[_it] = SLOT_GROUP_MAP[_slot]

REGIONS = ["us", "eu", "kr", "tw"]
FACTIONS = ["Alliance", "Horde"]
SECONDARY_STATS = ["haste", "versatility", "mastery", "crit"]
TERTIARY_STATS = ["avoidance", "lifesteal", "speed"]  # matches commonUtils.TERTIARY_STATS
_MS = 1000
_DAY_MS = 86400 * _MS


def _now_ms():
    import time
    return int(time.time() * _MS)


def _insert_many(conn, cursor, sql, rows, chunk=5000):
    """Batched executemany with the connector's lock-wait retry."""
    for i in range(0, len(rows), chunk):
        db.executemany_with_retry(conn, cursor, sql, rows[i:i + chunk])
    db.commit_with_retry(conn)


def _zipf_pick(rng, pool):
    """Pick from pool with a decaying weight so early entries dominate."""
    weights = [1.0 / (i + 1) for i in range(len(pool))]
    return rng.choices(pool, weights=weights, k=1)[0]


# --------------------------------------------------------------------------------------
# Static data + derived pools
# --------------------------------------------------------------------------------------

class StaticData:
    def __init__(self, lookup_dir):
        self.dir = lookup_dir
        self.specs = load_json(os.path.join(lookup_dir, "specs.json"))
        self.season_info = load_json(os.path.join(lookup_dir, "seasonInfo.json"))
        self.periods = load_json(os.path.join(lookup_dir, "periods.json"))
        self.items = load_json(os.path.join(lookup_dir, "equippable-items.json"))
        self.enchants = load_json(os.path.join(lookup_dir, "enchantments.json"))
        self.bonuses = load_json(os.path.join(lookup_dir, "bonuses.json"))
        self.embellishments = load_json(os.path.join(lookup_dir, "embellishments.json"))
        self.missives = load_json(os.path.join(lookup_dir, "missives.json"))
        # Tier-set membership: the Raidbots item-sets catalog (source of truth, see
        # processBonusIds / commonUtils.load_tier_sets). Tolerate absence early.
        try:
            self.item_sets = load_json(os.path.join(lookup_dir, "item-sets.json"))
        except (OSError, ValueError):
            self.item_sets = []
        self.npcs = load_json(os.path.join(lookup_dir, "npcs.json")).get("en_US", {})
        self.spells = load_json(os.path.join(lookup_dir, "spells.json"))
        # boss_npcs.json lives in data/ (parent of data/static), keyed by challenge_mode_id.
        self.boss_npcs = load_json(os.path.join(os.path.dirname(lookup_dir), "boss_npcs.json"))

        self.season = int(self.season_info["blizzard_season_id"])
        self.dungeons = self.season_info["dungeons"]  # each: challenge_mode_id, keystone_timer_seconds, ...
        # Current expansion = the highest `expansion` present in the item catalog (Midnight = 11
        # here). We seed gear/gems/enchants from this expansion only, so pages show current
        # content instead of random relics ("Worn Shortsword") pulled from all of WoW history.
        self.expansion = max(
            (i["expansion"] for i in self.items if isinstance(i.get("expansion"), int)),
            default=0,
        )

    def talents_for(self, spec_id):
        """Raw talent node partition (classNodes/specNodes/heroNodes/subTreeNodes)."""
        path = os.path.join(self.dir, "talents.json")
        if not hasattr(self, "_talents_raw"):
            self._talents_raw = {str(s["specId"]): s for s in load_json(path)}
        return self._talents_raw.get(str(spec_id))

    def _processed_talents(self, spec_id):
        """The processed data/static/talents/<specId>.json doc, cached, or None."""
        if not hasattr(self, "_processed_cache"):
            self._processed_cache = {}
        if spec_id not in self._processed_cache:
            try:
                self._processed_cache[spec_id] = load_json(
                    os.path.join(self.dir, "talents", f"{spec_id}.json")
                )
            except FileNotFoundError:
                self._processed_cache[spec_id] = None
        return self._processed_cache[spec_id]

    def processed_talents_for(self, spec_id):
        """The render lookup for a spec: (valid talent node ids, valid hero subtree ids).

        The spec page keys its talent display map on data/static/talents/<specId>.json's
        `talents` dict and its hero trees on that file's `subTrees`, and subscripts both
        with no guard -- so we may only seed talent_ids / hero_talent_ids that appear here,
        or the template raises UndefinedError. processTalents drops some raw nodes, so the
        raw talents.json partition is a superset we must intersect against this.
        """
        proc = self._processed_talents(spec_id)
        if not proc:
            return set(), set()
        valid = {int(k) for k in proc.get("talents", {})}
        subtrees = {int(k) for k in proc.get("subTrees", {})}
        return valid, subtrees

    def tree_geometry_for(self, spec_id):
        """(fullNodeOrder, nodes) from the processed talent file -- the decode
        order + node metadata (entries/free) the loadout encoder needs to emit a
        real Blizzard v2 string. Returns ([], {}) when the spec has no tree fields.
        """
        proc = self._processed_talents(spec_id)
        if not proc:
            return [], {}
        return proc.get("fullNodeOrder", []), proc.get("nodes", {})


def _enchant_pools_by_group(static):
    """enchant slot_group -> list of current-expansion enchant ids that resolve to that group."""
    pools = {}
    for e in static.enchants:
        if e.get("expansion") != static.expansion:
            continue
        req = e.get("equipRequirements") or {}
        item_class = req.get("itemClass")
        groups = set()
        if item_class == 2:  # weapon enchant / DK rune
            groups.add("WEAPON")
        elif item_class == 4:  # armor enchant, invTypeMask bit index = inventoryType
            mask = int(req.get("invTypeMask") or 0)
            for bit in range(mask.bit_length()):
                if mask >> bit & 1 and bit in _INVTYPE_TO_GROUP:
                    groups.add(_INVTYPE_TO_GROUP[bit])
        for g in groups:
            pools.setdefault(g, []).append(int(e["id"]))
    return pools


def _item_pools_by_slot(static, rng, per_slot=8):
    """slot -> small list of current-expansion item ids matching the slot's inventoryType."""
    pools = {}
    for slot, itypes in SLOT_INVENTORY_TYPES.items():
        cands = [
            it for it in static.items
            if it.get("inventoryType") in itypes and it.get("expansion") == static.expansion
        ]
        cands.sort(key=lambda it: it.get("itemLevel", 0), reverse=True)
        top = cands[: per_slot * 4] or cands
        chosen = rng.sample(top, min(per_slot, len(top))) if top else []
        pools[slot] = [int(it["id"]) for it in chosen]
    return pools


def _skewed_tree_assignment(trees, n, rng):
    """Assign n slots across `trees` with a deliberate skew, never an even split.

    ~70% of slots go to a dominant tree; the rest spread over the others. When that still
    lands on an even split (e.g. 2 trees, n even), one slot is shifted so a 50/50 is
    impossible. Used for both the raw member build variants and the top-50 loadouts.
    """
    trees = list(trees) or [0]
    k = len(trees)
    if k == 1:
        return [trees[0]] * n
    n_dom = max(1, round(n * 0.7))
    assign = [trees[0]] * min(n_dom, n)
    others = trees[1:]
    for i in range(n - len(assign)):
        assign.append(others[i % len(others)])
    # Force non-even: if every tree got the same count, move one slot to the dominant tree.
    from collections import Counter
    counts = Counter(assign)
    if len(set(counts.values())) == 1 and k > 1:
        assign[assign.index(trees[-1])] = trees[0]
    rng.shuffle(assign)
    return assign


def _synthetic_loadout(spec_id, variant_idx, hero_tree, selected):
    """A deterministic, clearly-synthetic loadout string for seeds whose committed
    talent files carry no tree geometry. Not a decodable Blizzard build -- just a stable,
    unique-per-variant token so the loadout aggregates populate."""
    node_sig = "".join(f"{nid:x}" for nid in sorted(selected))
    return f"SEEDBUILD-{spec_id}-{int(hero_tree)}-{variant_idx}-{node_sig}"[:255]


def _build_variants(static, spec_id, rng, count=4):
    """A few fixed talent builds per spec so talent aggregates concentrate.

    Every node id is filtered through the spec's processed talent map (the render lookup)
    so the template can always resolve it; hero trees come from that map's subTrees.
    """
    t = static.talents_for(spec_id)
    if not t:
        return []
    valid, sub_ids = static.processed_talents_for(spec_id)
    class_ids = [int(n["id"]) for n in (t.get("classNodes") or []) if int(n["id"]) in valid]
    spec_ids = [int(n["id"]) for n in (t.get("specNodes") or []) if int(n["id"]) in valid]
    # hero nodes grouped by subtree, keeping only ids the render lookup knows
    subtrees = {}
    for n in (t.get("heroNodes") or []):
        if int(n["id"]) in valid and n.get("subTreeId") in sub_ids:
            subtrees.setdefault(n["subTreeId"], []).append(int(n["id"]))
    sub_list = [s for s in sub_ids if s in subtrees] or list(sub_ids) or [0]

    # Skew the hero-tree assignment across variants so members never come out an even
    # 50/50 across trees: ~70% of variants take the dominant tree, the rest split the others.
    tree_assign = _skewed_tree_assignment(sub_list, count, rng)

    # Tree geometry for the real Blizzard v2 loadout string. Entries let us pick a
    # valid choice index per selected choice node; free nodes the encoder forces in.
    full_node_order, node_meta = static.tree_geometry_for(spec_id)

    def _entry_index(nid):
        entries = (node_meta.get(str(nid)) or {}).get("entries") or []
        return rng.randrange(len(entries)) if len(entries) > 1 else 0

    variants = []
    for i in range(count):
        hero_tree = tree_assign[i]
        hero_nodes = subtrees.get(hero_tree, [])
        class_sample = rng.sample(class_ids, max(1, int(len(class_ids) * 0.7))) if class_ids else []
        spec_sample = rng.sample(spec_ids, max(1, int(len(spec_ids) * 0.6))) if spec_ids else []
        # Real v2 loadout string over the sampled build: {node_id: entry_index}
        # for every purchased node (free nodes the encoder adds itself). Decodes
        # through analyzer.js so members.loadout is the meta build the analyzer
        # compares a pasted export against.
        selected = {int(nid): _entry_index(nid)
                    for nid in (class_sample + spec_sample + hero_nodes)}
        if full_node_order and node_meta:
            loadout = encode_loadout(spec_id, selected, full_node_order, node_meta)
        else:
            # No processed tree geometry (fullNodeOrder/nodes) in the committed
            # data/static/talents/<spec>.json, so we can't emit a real Blizzard v2
            # string. Fall back to a deterministic synthetic placeholder so the loadout
            # aggregates still populate -- the bot's /spec talents fetch_top_loadout path
            # and the spec page's meta-by-hero read them. Same non-decodable synthetic
            # string the README already flags for the analyzer meta build.
            loadout = _synthetic_loadout(spec_id, i, hero_tree, selected)
        variants.append({
            "class": class_sample,
            "spec": spec_sample,
            "hero_tree": int(hero_tree),
            "hero": hero_nodes,
            "loadout": loadout,
        })
    return variants


# --------------------------------------------------------------------------------------
# Reference tables
# --------------------------------------------------------------------------------------

def seed_reference(conn, cursor, static, rng):
    print("  seeding reference tables...")

    # dungeon_data (FK target of runs). challenge_mode_id is the dungeon_id (string).
    dd_rows = []
    for d in static.dungeons:
        cmid = str(d["challenge_mode_id"])
        timer_ms = int(d["keystone_timer_seconds"]) * _MS
        dd_rows.append((
            cmid, d.get("slug", f"dungeon-{cmid}"), d.get("name", f"Dungeon {cmid}"),
            timer_ms,               # upgrade_1 (+1: within time)
            int(timer_ms * 0.8),    # upgrade_2
            int(timer_ms * 0.6),    # upgrade_3
        ))
    _insert_many(conn, cursor,
        "INSERT INTO dungeon_data (dungeon_id, slug, name_en_us, upgrade_1_duration, "
        "upgrade_2_duration, upgrade_3_duration) VALUES (%s,%s,%s,%s,%s,%s)", dd_rows)

    # slot_group_map
    _insert_many(conn, cursor,
        "INSERT INTO slot_group_map (slot, slot_group) VALUES (%s,%s)",
        list(SLOT_GROUP_MAP.items()))

    # season_periods (from periods.json per region)
    sp_rows = []
    for region, info in static.periods.items():
        for p in info.get("periods", []):
            sp_rows.append((region, int(p["id"]), int(p["start_timestamp"]),
                            int(p["end_timestamp"]), int(info.get("season_id", static.season))))
    if sp_rows:
        _insert_many(conn, cursor,
            "INSERT INTO season_periods (region, period_id, start_timestamp, end_timestamp, "
            "season) VALUES (%s,%s,%s,%s,%s)", sp_rows)

    # bloodlust_spells (the lust spells live in spells.json)
    lust_ids = [int(sid) for sid in static.spells.keys()]
    if lust_ids:
        _insert_many(conn, cursor,
            "INSERT IGNORE INTO bloodlust_spells (spell_id) VALUES (%s)",
            [(sid,) for sid in lust_ids])

    # embellishments / missives are {bonus_id: item_id} maps
    _insert_many(conn, cursor,
        "INSERT IGNORE INTO embellishments (bonus_id, item_id) VALUES (%s,%s)",
        [(int(b), int(i)) for b, i in static.embellishments.items()])
    _insert_many(conn, cursor,
        "INSERT INTO missives (bonus_id, item_id) VALUES (%s,%s)",
        [(int(b), int(i)) for b, i in static.missives.items()])

    # crafted_item_ids derives from equippable-items.json; tier_set_items derives from
    # the item-sets catalog (source of truth for tier-set membership, see processBonusIds).
    crafted = [(int(it["id"]),) for it in static.items if "profession" in it]
    _insert_many(conn, cursor,
        "INSERT IGNORE INTO crafted_item_ids (item_id) VALUES (%s)", crafted)
    tier = [
        (int(iid), int(s["id"]))
        for s in static.item_sets if s.get("id") is not None
        for iid in (s.get("items") or [])
    ]
    _insert_many(conn, cursor,
        "INSERT IGNORE INTO tier_set_items (item_id, item_set_id) VALUES (%s,%s)", tier)

    return {"lust_ids": lust_ids}


# --------------------------------------------------------------------------------------
# Raw event tables (runs -> members -> gear/talents/stats + routes)
# --------------------------------------------------------------------------------------

def seed_runs(conn, cursor, static, rng, cfg, pools):
    print("  seeding runs / members / equipment / talents / stats...")
    now = _now_ms()

    # roles: specs.json role '0' tank, '1' heal, '2' dps
    by_role = {"0": [], "1": [], "2": []}
    for sid, meta in static.specs.items():
        by_role.get(meta.get("role"), by_role["2"]).append(int(sid))

    item_pools = pools["items"]
    enchant_pools = pools["enchants"]
    gem_pool = pools["gems"]
    bonus_pool = pools["bonus"]
    emb_bonus = pools["embellishment_bonus"]
    mis_bonus = pools["missive_bonus"]
    variants = pools["variants"]  # spec_id -> [variant,...]

    # Tier-set coverage: real players wear their class tier set. Now that tier_set_items
    # is scoped to the 42 curated current sets (item-sets.json, see the tier_set_items
    # seeding above), random gear almost never lands 2+ pieces of one set, so without this
    # aggregated_tier_set_comps / the spec-page tier-set card would stay empty locally.
    # Map each spec-specific tier set to its (slot, item_id) armour pieces and equip a
    # coherent subset for some members below.
    _INVTYPE_TO_SLOT = {1: "HEAD", 3: "SHOULDER", 5: "CHEST", 20: "CHEST", 7: "LEGS", 10: "HANDS"}
    _item_invtype = {int(it["id"]): it.get("inventoryType") for it in static.items}
    tier_by_spec = {}
    for s in static.item_sets:
        specids = {sp.get("specId") for sp in (s.get("spells") or []) if sp.get("specId")}
        if not specids:
            continue  # crafted / pvp sets (specId 0) are not class tier sets
        pieces = [
            (_INVTYPE_TO_SLOT[_item_invtype[int(iid)]], int(iid))
            for iid in (s.get("items") or [])
            if _item_invtype.get(int(iid)) in _INVTYPE_TO_SLOT
        ]
        if len(pieces) >= 2:
            for spec in specids:
                tier_by_spec.setdefault(int(spec), pieces)

    runs, run_members, members = [], [], []
    equipment, sockets, enchantments = [], [], []
    char_stats = []
    # (member, region, ts) captured per run so member_character can store the run's
    # region + timestamp. Dungeon-loop members are the "advanced" slice (name / realm
    # / M+ score populated); the comp-distribution members are the "simple" slice
    # (those detail columns NULL). Built after members are assigned their ids below.
    adv_member_meta = []
    simple_member_meta = []
    # Talent dictionary: set_id -> list of (set_id, tree, talent_id, rank) rows.
    # One set_id covers all three trees for a member; members reference it via
    # members.talent_set_id. Mirrors the collector's talent_sets dedup.
    talent_set_rows = {}
    # Bonus dictionary: set_id -> list of (set_id, bonus_id) rows. One set_id
    # covers an equipped item's whole bonus-id set; equipment references it via
    # equipment.bonus_set_id. Mirrors the collector's bonus_sets dedup.
    bonus_set_rows = {}

    run_id = member_id = equip_id = 0

    def add_member(spec_id):
        nonlocal member_id, equip_id
        member_id += 1
        m = member_id
        variant = rng.choice(variants.get(spec_id) or [None])
        hero_tree = variant["hero_tree"] if variant else 0
        loadout = variant["loadout"] if variant else None

        # Build this member's talent rows (with per-member random ranks, so the
        # aggregate AVG(rank) still varies), hash them into a talent_sets set_id,
        # and reference it from members.talent_set_id. Members that share the same
        # nodes AND ranks collapse to one dictionary set, exactly like live data.
        if variant:
            class_rows = [(nid, rng.randint(1, 2)) for nid in variant["class"]]
            spec_rows = [(nid, rng.randint(1, 2)) for nid in variant["spec"]]
            hero_rows = [(nid, 1) for nid in variant["hero"]]
            tsid = talent_set_hash(class_rows, spec_rows, hero_rows)
        else:
            tsid = None
        members.append((m, spec_id, loadout, hero_tree or None, tsid))
        if tsid is not None and tsid not in talent_set_rows:
            rows = ([(tsid, 0, nid, rk) for nid, rk in class_rows]
                    + [(tsid, 1, nid, rk) for nid, rk in spec_rows]
                    + [(tsid, 2, nid, rk) for nid, rk in hero_rows])
            talent_set_rows[tsid] = rows

        # equipment across all 16 slots
        member_eids = []
        # Per-item bonus-id sets, resolved into a bonus_sets dictionary id once all
        # bonuses (slot rolls + embellishment/missive extras below) are decided.
        eid_meta = {}       # eid -> (slot, item_id, ilvl)
        eid_bonuses = {}    # eid -> set(bonus_id)
        # ~half of players wear 2+ pieces of their class tier set (forces some tier-set
        # comps into aggregated_tier_set_comps so the tier-set card has data locally).
        tier_pieces = tier_by_spec.get(spec_id)
        forced_tier = {}
        if tier_pieces and rng.random() < 0.5:
            forced_tier = dict(rng.sample(tier_pieces, rng.randint(2, len(tier_pieces))))
        for slot in EQUIPMENT_SLOTS:
            pool = item_pools.get(slot) or []
            if not pool:
                continue
            equip_id += 1
            eid = equip_id
            member_eids.append(eid)
            item_id = forced_tier.get(slot) or _zipf_pick(rng, pool)
            ilvl = rng.randint(620, 662)
            eid_meta[eid] = (slot, item_id, ilvl)
            eid_bonuses[eid] = set()
            # bonus ids
            for b in rng.sample(bonus_pool, rng.randint(1, 2)):
                eid_bonuses[eid].add(int(b))
            # enchant on enchantable slots
            if slot in ENCHANTABLE_SLOTS:
                grp = SLOT_GROUP_MAP[slot]
                epool = enchant_pools.get(grp)
                if epool:
                    enchantments.append((str(_zipf_pick(rng, epool)), eid))
            # socket + gem on a few slots
            if slot in SOCKETED_SLOTS and gem_pool and rng.random() < 0.6:
                sockets.append(("PRISMATIC", str(_zipf_pick(rng, gem_pool)), eid))

        # An embellishment (most builds run 1-2) and sometimes a missive, added as extra
        # bonus ids on a random equipped piece so sp_agg_embellishments / sp_agg_missives fire.
        if member_eids and emb_bonus and rng.random() < 0.7:
            for eid in rng.sample(member_eids, min(rng.randint(1, 2), len(member_eids))):
                eid_bonuses[eid].add(int(_zipf_pick(rng, emb_bonus)))
        if member_eids and mis_bonus and rng.random() < 0.5:
            eid_bonuses[rng.choice(member_eids)].add(int(rng.choice(mis_bonus)))

        # Now that every item's full bonus-id set is known, hash it into a
        # bonus_sets dictionary id and reference it from equipment.bonus_set_id.
        # Items that share the same combo collapse to one dictionary set, exactly
        # like live data.
        for eid in member_eids:
            slot, item_id, ilvl = eid_meta[eid]
            ids = sorted(eid_bonuses[eid])
            bsid = bonus_set_hash(ids)
            equipment.append((slot, str(item_id), str(ilvl), m, eid, bsid))
            if bsid is not None and bsid not in bonus_set_rows:
                bonus_set_rows[bsid] = [(bsid, b) for b in ids]

        # character stats: primary + stamina + secondaries always; tertiaries on most chars
        # (real players carry 1-2), so the stat card's tertiary row has data.
        char_stats.append((m, "mainstat", None, rng.randint(30000, 46000)))
        char_stats.append((m, "stamina", None, rng.randint(200000, 320000)))
        for s in SECONDARY_STATS:
            char_stats.append((m, s, round(rng.uniform(5, 38), 2), rng.randint(2000, 12000)))
        for s in TERTIARY_STATS:
            if rng.random() < 0.6:
                char_stats.append((m, s, round(rng.uniform(0.3, 3.0), 2), rng.randint(100, 1600)))
        return m

    for d in static.dungeons:
        cmid = str(d["challenge_mode_id"])
        timer_ms = int(d["keystone_timer_seconds"]) * _MS
        for _ in range(cfg["runs_per_dungeon"]):
            run_id += 1
            rid = run_id
            duration = int(timer_ms * rng.uniform(0.45, 1.25))
            ts = now - rng.randint(0, 13 * _DAY_MS)
            region = rng.choice(REGIONS)
            runs.append((cmid, rng.randint(2, 20), duration, ts,
                         rng.choice(FACTIONS), rid, region, static.season))
            # 1 tank, 1 heal, 3 dps
            comp = ([rng.choice(by_role["0"] or by_role["2"])]
                    + [rng.choice(by_role["1"] or by_role["2"])]
                    + [rng.choice(by_role["2"]) for _ in range(3)])
            for spec_id in comp:
                m = add_member(spec_id)
                run_members.append((m, rid))
                adv_member_meta.append((m, region, ts))

    # --- Comp distribution layer -------------------------------------------------------
    # The comps page needs a CONCENTRATED comp distribution that random 5-spec draws can't
    # give: hidden gems only surface when the #1 comp has >1000 runs (window is
    # 20 < runs < max*0.02), and glue specs need comps with timed>5 & avg_key>12. So we seed
    # a *designed* set of comps here, backed by lightweight runs -- comps aggregate from
    # run_members->members.spec_id, so these members carry only a spec_id (no gear/talents),
    # keeping the extra volume cheap.
    tanks = by_role["0"] or by_role["2"]
    heals = by_role["1"] or by_role["2"]
    dps = by_role["2"]
    timer_by_cmid = {str(d["challenge_mode_id"]): int(d["keystone_timer_seconds"]) * _MS
                     for d in static.dungeons}
    comp_cmids = list(timer_by_cmid)

    def _rand_comp():
        return tuple(sorted([rng.choice(tanks), rng.choice(heals)]
                            + [rng.choice(dps) for _ in range(3)]))

    # (target_runs, key_lo, key_hi, timed_ratio). The dominant comp fixes max_comp_runs so the
    # hidden-gem window (20, max*0.02) opens; gems sit inside it, all timed; filler/glue sit
    # above 5 timed with avg key > 12. Fixed volume so gems appear regardless of --runs-per-dungeon.
    comp_plan = [(1600, 14, 20, 0.9)]                                       # dominant meta
    comp_plan += [(rng.randint(250, 500), 13, 19, 0.85) for _ in range(4)]  # secondary meta
    comp_plan += [(rng.randint(23, 30), 12, 16, 1.0) for _ in range(10)]    # hidden gems
    comp_plan += [(rng.randint(8, 40), 13, 17, 0.8) for _ in range(35)]     # glue / filler
    seen_comps = set()
    for target, klo, khi, timed_ratio in comp_plan:
        for _ in range(50):
            specs = _rand_comp()
            if specs not in seen_comps:
                seen_comps.add(specs)
                break
        else:
            continue
        n_timed = round(target * timed_ratio)
        for i in range(target):
            run_id += 1
            rid = run_id
            cmid = rng.choice(comp_cmids)
            timer_ms = timer_by_cmid[cmid]
            timed = i < n_timed
            duration = int(timer_ms * (rng.uniform(0.55, 0.95) if timed else rng.uniform(1.05, 1.3)))
            ts = now - rng.randint(0, 13 * _DAY_MS)
            region = rng.choice(REGIONS)
            runs.append((cmid, rng.randint(klo, khi), duration, ts,
                         rng.choice(FACTIONS), rid, region, static.season))
            for spec_id in specs:
                member_id += 1
                members.append((member_id, spec_id, None, None, None))  # lightweight: spec_id only
                run_members.append((member_id, rid))
                simple_member_meta.append((member_id, region, ts))

    print(f"    {len(runs)} runs, {len(members)} members, {len(equipment)} equipment rows")
    _insert_many(conn, cursor,
        "INSERT INTO runs (dungeon_id, keystone_level, duration, timestamp, faction, run_id, "
        "region, season) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", runs)
    _insert_many(conn, cursor,
        "INSERT INTO members (member, spec_id, loadout, hero_talent_id, talent_set_id) VALUES (%s,%s,%s,%s,%s)", members)

    # member_character: raw Blizzard identity + M+ score, mirroring the collector.
    # blizzard_character_id is drawn from a SMALL per-region pool assigned in run
    # order, so the same (region, blizzard_character_id) recurs across different
    # runs -- the cross-run linkage the feature is built for. Each dungeon run
    # supplies exactly 5 members, so with a pool larger than 5 the recurrence
    # always spans two different runs. Dungeon-loop members are the advanced slice
    # (name / realm / score populated); comp-distribution members are the simple
    # slice (those columns NULL, so the NULL-score-on-simple path is exercised).
    _CHAR_POOL = 8
    region_seq = {}

    def _next_bcid(region):
        seq = region_seq.get(region, 0)
        region_seq[region] = seq + 1
        base = 1_000_000_000 + REGIONS.index(region) * 1_000_000
        return base + (seq % _CHAR_POOL)

    member_character = []
    for m, region, ts in adv_member_meta:
        bcid = _next_bcid(region)
        member_character.append((m, region, bcid, f"char{bcid}",
                                 f"realm-{bcid % 500}", rng.randint(1500, 3600), ts))
    for m, region, ts in simple_member_meta:
        bcid = _next_bcid(region)
        member_character.append((m, region, bcid, None, None, None, ts))
    _insert_many(conn, cursor,
        "INSERT INTO member_character (member, region, blizzard_character_id, character_name, "
        "realm_slug, mplus_score, collected_ts) VALUES (%s,%s,%s,%s,%s,%s,%s)", member_character)

    # member_dungeon_score: per-member per-dungeon rating snapshot, mirroring the
    # mythic-keystone-profile best_runs map_rating. Advanced slice only (the simple
    # slice writes none, matching the collector). dungeon_id is the dungeon's
    # challenge_mode_id as a string, same value stored in runs.dungeon_id, so a
    # per-dungeon score joins straight to runs. collected_ts = the run timestamp so
    # the 14-day purge lines up with member_character.
    dungeon_cmids = [str(d["challenge_mode_id"]) for d in static.dungeons]
    member_dungeon_score = []
    for m, region, ts in adv_member_meta:
        for cmid in dungeon_cmids:
            member_dungeon_score.append((m, cmid, rng.randint(150, 480), ts))
    _insert_many(conn, cursor,
        "INSERT INTO member_dungeon_score (member, dungeon_id, rating, collected_ts) "
        "VALUES (%s,%s,%s,%s)", member_dungeon_score)

    _insert_many(conn, cursor,
        "INSERT INTO run_members (member, run_id) VALUES (%s,%s)", run_members)
    _insert_many(conn, cursor,
        "INSERT INTO equipment (slot, item_id, item_level, member, equipment_id, bonus_set_id) "
        "VALUES (%s,%s,%s,%s,%s,%s)", equipment)
    # Distinct bonus-id combos keyed by content hash; equipment.bonus_set_id points
    # at these. Mirrors the collector's bonus_sets dedup.
    bs_rows = [row for rows in bonus_set_rows.values() for row in rows]
    _insert_many(conn, cursor,
        "INSERT IGNORE INTO bonus_sets (set_id, bonus_id) VALUES (%s,%s)", bs_rows)
    _insert_many(conn, cursor,
        "INSERT INTO sockets (socket_type, socket_item_id, equipment_id) VALUES (%s,%s,%s)", sockets)
    _insert_many(conn, cursor,
        "INSERT INTO enchantments (enchantment_id, equipment_id) VALUES (%s,%s)", enchantments)
    ts_rows = [row for rows in talent_set_rows.values() for row in rows]
    _insert_many(conn, cursor,
        "INSERT IGNORE INTO talent_sets (set_id, tree, talent_id, `rank`) VALUES (%s,%s,%s,%s)", ts_rows)
    _insert_many(conn, cursor,
        "INSERT INTO character_stats (member, stat, percent, raw) VALUES (%s,%s,%s,%s)", char_stats)


def seed_routes(conn, cursor, static, rng, cfg, ref):
    print("  seeding routes...")
    # route_data.timestamp is stored in SECONDS (raider.io epoch), unlike runs.timestamp (ms).
    now_s = _now_ms() // _MS
    npc_ids = [int(n) for n in static.npcs.keys()] or [100000]
    lust_ids = ref["lust_ids"] or [32182]
    dps_specs = [int(s) for s, m in static.specs.items() if m.get("role") == "2"] or [62]
    all_specs = [int(s) for s in static.specs.keys()]

    route_data, route_pulls, route_specs = [], [], []
    pull_enemies, pull_spells = [], []
    rio = 1

    for d in static.dungeons:
        cmid = str(d["challenge_mode_id"])
        timer_ms = int(d["keystone_timer_seconds"]) * _MS
        for _ in range(cfg["routes_per_dungeon"]):
            rio += 1
            route_key = f"seedroute-{cmid}-{rio}"
            route_data.append((rio, 1, rng.randint(90, 105),
                               now_s - rng.randint(0, 13 * 86400), rng.randint(8, 20),
                               int(timer_ms * rng.uniform(0.5, 1.1)), cmid, route_key))
            # comp = 5 specs
            comp = [rng.choice(all_specs) for _ in range(5)]
            for sid in comp:
                route_specs.append((sid, route_key))
            # pulls. The last pull is a "boss pull": it carries a dungeon boss npc AND a
            # bloodlust spell, which the dungeon page requires (it validates that the lust
            # timeline contains at least one boss pull, and fails loudly otherwise).
            bosses = [int(b) for b in static.boss_npcs.get(cmid, [])] or npc_ids
            n_pulls = rng.randint(6, 10)
            for pull_id in range(1, n_pulls + 1):
                route_pulls.append((route_key, pull_id))
                is_boss_pull = pull_id == n_pulls
                enemies = rng.sample(npc_ids, min(len(npc_ids), rng.randint(1, 4)))
                if is_boss_pull:
                    enemies.append(rng.choice(bosses))
                for npc in set(enemies):
                    pull_enemies.append((route_key, npc, pull_id, rng.randint(1, 6)))
                # lust on the boss pull always, plus occasional trash-pull lust
                if is_boss_pull or rng.random() < 0.2:
                    pull_spells.append((route_key, rng.choice(lust_ids), pull_id))

    _insert_many(conn, cursor,
        "INSERT INTO route_data (rio_run_id, mapping_version, enemy_forces, timestamp, "
        "keystone_level, duration, dungeon_id, route_key) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", route_data)
    _insert_many(conn, cursor,
        "INSERT INTO route_pulls (route_key, pull_id) VALUES (%s,%s)", route_pulls)
    _insert_many(conn, cursor,
        "INSERT INTO route_specs (spec_id, route_key) VALUES (%s,%s)", route_specs)
    _insert_many(conn, cursor,
        "INSERT INTO pull_enemies (route_key, npc_id, pull_id, `count`) VALUES (%s,%s,%s,%s)", pull_enemies)
    _insert_many(conn, cursor,
        "INSERT IGNORE INTO pull_spells (route_key, spell_id, pull_id) VALUES (%s,%s,%s)", pull_spells)


# --------------------------------------------------------------------------------------
# Standalone tables the aggregation pipeline does NOT build
# --------------------------------------------------------------------------------------

def _top50_plan(static, spec_id, rng, cmids):
    """Per-spec structure for realistic top-50 loadouts.

    The spec page's Talent Differences modal buckets loadouts by the hero tree their nodes
    belong to, then flags talents whose adoption in one dungeon deviates from that tree's
    baseline -- so each loadout must carry a FULL build (class + spec + HERO nodes, or the
    hero tree can't be inferred and the whole per-dungeon stat is dropped), and picks must
    vary by dungeon. We fix a core build plus per-dungeon "flex" nodes so a clear per-dungeon
    signal emerges, and only ever use ids the render lookup knows (processed talent map).
    """
    valid, subtrees = static.processed_talents_for(spec_id)
    t = static.talents_for(spec_id) or {}
    class_ids = [int(n["id"]) for n in t.get("classNodes", []) if int(n["id"]) in valid]
    spec_ids = [int(n["id"]) for n in t.get("specNodes", []) if int(n["id"]) in valid]
    hero_by_tree = {}
    for n in t.get("heroNodes", []):
        if int(n["id"]) in valid and n.get("subTreeId") in subtrees:
            hero_by_tree.setdefault(int(n["subTreeId"]), []).append(int(n["id"]))
    trees = [s for s in subtrees if s in hero_by_tree] or list(hero_by_tree) or [0]
    class_core = rng.sample(class_ids, max(1, int(len(class_ids) * 0.6))) if class_ids else []
    spec_core = rng.sample(spec_ids, max(1, int(len(spec_ids) * 0.5))) if spec_ids else []
    core = set(class_core) | set(spec_core)
    flex = [x for x in (class_ids + spec_ids) if x not in core]
    dungeon_flex = {c: (rng.sample(flex, min(3, len(flex))) if flex else []) for c in cmids}
    return {
        "core": sorted(core),
        "hero_by_tree": hero_by_tree,
        "trees": trees,
        "dungeon_flex": dungeon_flex,
    }


def _top50_rank_trees(trees, n, rng):
    """Assign each of the n ranked players a hero tree: a ~60/40 skew for the common
    two-tree case (never 50/50), while keeping BOTH trees above
    TALENT_DIFF_MIN_DUNGEON_LOADOUTS so each tree still gets a per-dungeon talent diff."""
    trees = list(trees) or [0]
    if len(trees) == 2:
        major = round(n * 0.6)
        minor = n - major
        if major == minor:  # even split -> break it so 50/50 is impossible
            major, minor = major + 1, minor - 1
        assign = [trees[0]] * major + [trees[1]] * minor
        rng.shuffle(assign)
        return assign
    return _skewed_tree_assignment(trees, n, rng)


def seed_standalone(conn, cursor, static, rng, cfg, pools):
    print("  seeding top_player_* and simc_bis_* tables...")
    season = static.season
    cmids = [int(d["challenge_mode_id"]) for d in static.dungeons]
    item_pools = pools["items"]
    enchant_pools = pools["enchants"]
    gem_pool = pools["gems"]
    bonus_pool = pools["bonus"]
    variants = pools["variants"]
    import datetime
    now_dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def bonus_csv():
        return ",".join(str(b) for b in rng.sample(bonus_pool, min(2, len(bonus_pool))))

    ench_groups = {SLOT_GROUP_MAP[s] for s in ENCHANTABLE_SLOTS}
    tpl, tpl_items, tpl_ench, tpl_gems, tpl_tal = [], [], [], [], []
    for sid_str in static.specs:
        sid = int(sid_str)
        plan = _top50_plan(static, sid, rng, cmids)
        # Each ranked player runs one hero tree, skewed (never 50/50) but keeping both trees
        # above the per-dungeon diff threshold. The collector stores one loadout per dungeon
        # per player, so we emit a row for every (rank, dungeon).
        rank_trees = _top50_rank_trees(plan["trees"], cfg["top_player_ranks"], rng)
        # Tree geometry (entries/type/maxRanks) so the per-node talent rows below
        # carry a real chosen-entry spellId (the spec page's top-50 node-usage
        # stats read those) and so the per-loadout loadout_text below is a valid
        # Blizzard v2 string encoded over the selected build. The loadout_key column
        # is a synthetic production-style token (see below), NOT a talent code;
        # loadout_text is the real export string generateSimcProfiles feeds simc.
        full_node_order, node_meta = static.tree_geometry_for(sid)

        def _entry_idx(nid):
            entries = (node_meta.get(str(nid)) or {}).get("entries") or []
            return rng.randrange(len(entries)) if len(entries) > 1 else 0

        for rank in range(1, cfg["top_player_ranks"] + 1):
            tree = rank_trees[rank - 1]
            hero_nodes = plan["hero_by_tree"].get(tree, [])
            for cmid in cmids:
                # Full loadout: core (stable across dungeons) + hero nodes (so the tree is
                # inferrable) + this dungeon's flex picks (the per-dungeon difference signal).
                nodes = set(plan["core"]) | set(hero_nodes) | set(plan["dungeon_flex"][cmid])
                selected = {int(nid): _entry_idx(nid) for nid in nodes}
                # loadout_key mirrors PRODUCTION: the collector stores a synthetic
                # option token here (chosen.optionKey / id, e.g. logged-mplus__<id>),
                # NOT a Blizzard talent code. loadout_text mirrors the OTHER
                # production column: the real Blizzard v2 export string the player
                # used in game, which is what generateSimcProfiles._top50_talents now
                # feeds simc verbatim. We synthesize a valid v2 string over this
                # loadout's selected build via encode_loadout (the seeder-test-only
                # encoder) so the local top50 modal decodes it and the local simc
                # validation exercises the real "use the stored string" path. Missing
                # geometry leaves the column NULL, exactly as production does when
                # raider.io exposes no string.
                loadout_key = f"logged-mplus__{rng.randint(10**8, 10**9)}"
                if full_node_order and node_meta:
                    loadout_text = encode_loadout(sid, selected, full_node_order, node_meta)
                else:
                    loadout_text = None
                tpl.append((sid, season, rank, cmid, rng.choice(REGIONS),
                            rng.randint(10**6, 10**9), f"Player{sid}r{rank}", "TestRealm",
                            loadout_key, now_dt, rng.randint(12, 22), loadout_text))
                for slot in EQUIPMENT_SLOTS:
                    pool = item_pools.get(slot) or []
                    if not pool:
                        continue
                    tpl_items.append((sid, season, rank, cmid, slot, _zipf_pick(rng, pool),
                                      rng.randint(620, 662), bonus_csv()))
                for grp in ench_groups:  # PK keys on slot_group; FINGER_1/2 collapse to FINGER
                    epool = enchant_pools.get(grp)
                    if epool:
                        tpl_ench.append((sid, season, rank, cmid, grp, _zipf_pick(rng, epool)))
                for g in (rng.sample(gem_pool, min(3, len(gem_pool))) if gem_pool else []):
                    tpl_gems.append((sid, season, rank, cmid, int(g), rng.randint(1, 30)))
                # Per-node rows carry entry_id/spell_id like production so the spec
                # page's top-50 node-usage stats have real data to aggregate (the
                # tierlist top50 talents now come from loadout_text above, not these
                # rows): spell_id is the chosen entry's real spellId; entry_id is a
                # stable synthetic id so the column is populated as it is live.
                for nid in nodes:
                    meta = node_meta.get(str(nid)) or {}
                    entries = meta.get("entries") or []
                    idx = selected.get(int(nid), 0)
                    entry = entries[idx] if idx < len(entries) else (entries[0] if entries else {})
                    spell_id = entry.get("spellId")
                    max_ranks = int(meta.get("maxRanks") or 1)
                    node_rank = max_ranks if max_ranks > 1 else 1
                    entry_id = (int(nid) * 100 + idx) if entries else None
                    tpl_tal.append((sid, season, rank, cmid, nid, node_rank, entry_id, spell_id))

    _insert_many(conn, cursor,
        "INSERT INTO top_player_loadouts (spec_id, season, `rank`, map_challenge_mode_id, region, "
        "character_id, character_name, realm, loadout_key, loadout_updated_at, keystone_level, loadout_text) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", tpl)
    _insert_many(conn, cursor,
        "INSERT INTO top_player_loadout_items (spec_id, season, `rank`, map_challenge_mode_id, slot, "
        "item_id, item_level, bonus_ids) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", tpl_items)
    _insert_many(conn, cursor,
        "INSERT INTO top_player_loadout_enchants (spec_id, season, `rank`, map_challenge_mode_id, "
        "slot_group, enchantment_id) VALUES (%s,%s,%s,%s,%s,%s)", tpl_ench)
    _insert_many(conn, cursor,
        "INSERT INTO top_player_loadout_gems (spec_id, season, `rank`, map_challenge_mode_id, "
        "gem_item_id, usage_count) VALUES (%s,%s,%s,%s,%s,%s)", tpl_gems)
    _insert_many(conn, cursor,
        "INSERT INTO top_player_loadout_talents (spec_id, season, `rank`, map_challenge_mode_id, "
        "node_id, node_rank, entry_id, spell_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", tpl_tal)

    # simc_bis_meta + items
    meta_rows, item_rows = [], []
    prog_meta, prog_rows = [], []
    for sid_str in static.specs:
        sid = int(sid_str)
        base_dps = rng.uniform(1.8e6, 3.2e6)
        meta_rows.append((sid, season, "simc-seed", base_dps, 10000, 0.1, "tww3", now_dt))
        for slot in EQUIPMENT_SLOTS:
            pool = item_pools.get(slot) or []
            if not pool:
                continue
            grp = SLOT_GROUP_MAP[slot]
            ench = (enchant_pools.get(grp) or [None])
            for rank in range(1, cfg["simc_bis_ranks"] + 1):
                gain = 0.0 if rank == 1 else round(rng.uniform(0.2, 3.5), 2)
                enchant_id = rng.choice(ench) if slot in ENCHANTABLE_SLOTS and ench[0] else None
                gem_ids = str(_zipf_pick(rng, gem_pool)) if slot in SOCKETED_SLOTS and gem_pool else None
                item_rows.append((sid, season, slot, rank, _zipf_pick(rng, pool),
                                  bonus_csv(), rng.randint(639, 665), base_dps * (1 - gain / 100),
                                  gain, 0, None, enchant_id, gem_ids))
        prog_meta.append((sid, season, f"{sid:064x}", 120, base_dps, "simc-seed",
                          now_dt, now_dt, 0, None))
        for i in range(rng.randint(3, 8)):
            prog_rows.append((sid, season, f"pset_{i}", base_dps * rng.uniform(0.98, 1.03), now_dt))

    _insert_many(conn, cursor,
        "INSERT INTO simc_bis_meta (spec_id, season, simc_version, baseline_dps, iterations, "
        "target_error, tier_config, updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", meta_rows)
    _insert_many(conn, cursor,
        "INSERT INTO simc_bis_items (spec_id, season, slot, `rank`, item_id, bonus_list, ilevel, dps, "
        "dps_pct_gain, is_set_piece, item_set_id, enchant_id, gem_ids) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", item_rows)
    _insert_many(conn, cursor,
        "INSERT INTO simc_bis_progress_meta (spec_id, season, run_signature, total_profilesets, "
        "baseline_dps, simc_version, started_at, last_attempt_at, failed, prep_snapshot) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", prog_meta)
    _insert_many(conn, cursor,
        "INSERT INTO simc_bis_progress (spec_id, season, profileset_name, mean_dps, updated_at) "
        "VALUES (%s,%s,%s,%s,%s)", prog_rows)


def seed_control(conn, cursor, static):
    """Minimal rows for the control/watermark tables (kept out of the season wipe path)."""
    watermarks = [("purge_member_pointer",), ("purge_routes_pointer",)]
    _insert_many(conn, cursor,
        "INSERT IGNORE INTO summary_meta (name, last_run_id) VALUES (%s, 0)", watermarks)
    db.execute_with_retry(conn, cursor,
        "INSERT IGNORE INTO wipe_control (id, request_season, done_season, collector_paused, "
        "collector_beat, requested_at) VALUES (1, 0, %s, 0, 0, 0)", (static.season,))
    db.commit_with_retry(conn)


# --------------------------------------------------------------------------------------
# Trends (post-pipeline): duplicate the latest snapshot week to a previous week so the
# Top-Trends bar has two weeks to diff. snapshotTrends.py writes the current week.
# --------------------------------------------------------------------------------------

def duplicate_latest_trend_week(conn, cursor):
    """Copy the newest trend_snapshot week to week-1 with light jitter, if one exists."""
    rows = db.fetch_with_retry(conn, cursor, "SELECT MAX(week_id) FROM trend_snapshot")
    latest = rows[0][0] if rows and rows[0] else None
    if latest is None:
        print("  no trend_snapshot rows yet; Top-Trends bar will stay hidden")
        return
    db.execute_with_retry(conn, cursor,
        "INSERT INTO trend_snapshot (week_id, feed, group_key, entity_key, label, tier, rank_pos, "
        "score, popularity, run_count) "
        "SELECT %s, feed, group_key, entity_key, label, tier, rank_pos, "
        "score*0.95, popularity*0.95, FLOOR(run_count*0.9) "
        "FROM trend_snapshot WHERE week_id = %s "
        "ON DUPLICATE KEY UPDATE popularity = VALUES(popularity)", (latest - 1, latest))
    db.commit_with_retry(conn)
    print(f"  duplicated trend week {latest} -> {latest - 1} for the Top-Trends bar")


# --------------------------------------------------------------------------------------
# Orchestration helper: build the shared bounded pools once
# --------------------------------------------------------------------------------------

def _gem_pool(static, rng):
    """Valid gem item ids = the itemId of enchantments.json entries with slot 'socket'.

    The spec page builds socket_lookup as {e['itemId']: e for slot=='socket'} and the
    template subscripts it, so a gem id that isn't one of these raises UndefinedError.
    """
    gem_ids = [int(e["itemId"]) for e in static.enchants
               if e.get("slot") == "socket" and e.get("itemId")
               and e.get("expansion") == static.expansion]
    if not gem_ids:
        raise RuntimeError(
            f"no socket-slot gems for expansion {static.expansion} in enchantments.json")
    return rng.sample(gem_ids, min(10, len(gem_ids)))


def build_pools(static, rng):
    variants = {int(s): _build_variants(static, int(s), rng) for s in static.specs}
    item_pools = _item_pools_by_slot(static, rng)
    bonus_pool = rng.sample([int(k) for k in static.bonuses.keys()],
                            k=min(12, len(static.bonuses)))
    # Real embellishment / missive bonus ids (keys of the {bonus_id: item_id} maps). Seeding
    # a few of these onto gear is the only way sp_agg_embellishments / sp_agg_missives produce
    # rows (they join equipment -> bonus_sets -> the embellishments / missives tables on bonus_id).
    emb_bonus = [int(b) for b in static.embellishments.keys()]
    mis_bonus = [int(b) for b in static.missives.keys()]
    return {
        "variants": variants,
        "items": item_pools,
        "enchants": _enchant_pools_by_group(static),
        "gems": _gem_pool(static, rng),
        "bonus": bonus_pool,
        "embellishment_bonus": rng.sample(emb_bonus, min(8, len(emb_bonus))),
        "missive_bonus": mis_bonus,
    }
