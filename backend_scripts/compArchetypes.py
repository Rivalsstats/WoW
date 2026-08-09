"""Group "close" team comps into archetypes for the comps page.

A team comp family = a popular meta comp plus every comp that is one spec-swap away from
it (leader / greedy min-distance clustering, radius 1). Because a new family seed can only
be an unassigned comp, no two families are ever within one swap of each other, so the same
meta region is never described twice. The family's displayed *core* is the member comp that
reached the highest key done among comps with enough runs to be trusted (_core_rank), not the
most popular one. Clustering seeds families with that same key, so each family's core equals
its seed and no two cores are ever within one swap (otherwise near-identical cores could
surface as separate families). Every member is <=1 swap from the seed, which lets us break
that core comp into 5 slots and, per slot, list the alternate specs that swap in, ranked by
the highest key they reached (then by how often they are played).

The page needs the grouping per dungeon (the dungeon dropdown re-ranks everything) in three
flavours:
  * popular  - ranked by total runs (all key levels)
  * highkey  - ranked by play at the context's highest 2 key levels
  * gems     - niche but strong at the context's highest 5 key levels

"Highest N key levels" is computed per context (per dungeon, and globally for 'all'): the N
highest levels present, requiring MIN_HIGH_RUNS runs to count, except the single highest
level always counts. So a dungeon pushed to +30 defines its own high-key band, and the
global band is the globally-highest levels (which may only exist in one dungeon).

Runs on the already-collapsed comps, so it is a few-thousand-row, sub-second pass with no
extra DB work. build_dungeon_archetypes() is the page entry point.
"""

from collections import Counter, defaultdict
from itertools import combinations

MIN_RANK_RUNS = 20         # a family needs this many runs to rank in popular
MIN_HIGH_RUNS = 5          # a key level needs this many runs to raise the high-key bar
MIN_CORE_RUNS = MIN_HIGH_RUNS  # a comp needs this many runs to define a family's core
HIGHKEY_LEVELS = 2         # "Best for High Keys" looks at the highest 2 key levels
GEM_LEVELS = 5             # "Hidden Gems" looks at the highest 5 key levels
HIGHKEY_EXP = 3            # exponent emphasising higher keys in the high-key score
GEM_MIN_RUNS = 5           # ignore near-empty families as gems (high keys are sparse)
GEM_MAX_SHARE = 0.10       # a gem plays < 10% of the busiest family's high-key runs
GEM_MIN_SUCCESS = 75       # but still times most of its high-key runs (percent)


def collapse_comps(rows, spec_lookup):
    """Collapse raw aggregated_dungeon_comps rows into the per-comp clustering input
    (same shape as generateCompPage's archetype_input) so spec / dungeon pages can reuse
    the same team-comp grouping. Each row is (dungeon_id, keystone_level, comp_csv,
    timed_runs, depleted_runs), tuple- or dict-shaped. Weight mirrors the comps page:
    timed * (level-9)^2, depleted at 10%."""
    def field(row, idx, key):
        return row[key] if isinstance(row, dict) else row[idx]

    compiled = {}
    for row in rows:
        try:
            did = int(field(row, 0, "dungeon_id"))
            lvl = int(field(row, 1, "keystone_level"))
            comp_str = field(row, 2, "comp")
            timed = int(field(row, 3, "timed_runs"))
            depleted = int(field(row, 4, "depleted_runs"))
        except (KeyError, IndexError, TypeError, ValueError):
            continue
        specs = [int(s) for s in str(comp_str).split(",") if s.strip()]
        if len(specs) != 5:
            continue
        specs.sort(key=lambda s: (int(spec_lookup.get(str(s), {}).get("role", 2)), s))
        h = ",".join(map(str, specs))
        runs = timed + depleted
        kf = max(1, lvl - 9)
        w = timed * kf * kf + depleted * 0.1 * kf * kf

        e = compiled.get(h)
        if e is None:
            e = {"c": specs, "w": 0.0, "t": 0, "d": 0, "runs": 0, "mk": 0,
                 "_ka": 0.0, "kl": {}, "dungeons": {}}
            compiled[h] = e
        e["w"] += w
        e["t"] += timed
        e["d"] += depleted
        e["runs"] += runs
        e["mk"] = max(e["mk"], lvl)
        e["_ka"] += lvl * runs
        kl = e["kl"].setdefault(lvl, {"r": 0, "t": 0})
        kl["r"] += runs
        kl["t"] += timed

        dj = e["dungeons"].setdefault(did, {"t": 0, "d": 0, "runs": 0, "mk": 0,
                                            "w": 0.0, "_ka": 0.0, "kl": {}})
        dj["t"] += timed
        dj["d"] += depleted
        dj["runs"] += runs
        dj["w"] += w
        dj["mk"] = max(dj["mk"], lvl)
        dj["_ka"] += lvl * runs
        dkl = dj["kl"].setdefault(lvl, {"r": 0, "t": 0})
        dkl["r"] += runs
        dkl["t"] += timed

    out = []
    for e in compiled.values():
        e["avg_key"] = round(e.pop("_ka") / e["runs"], 2) if e["runs"] else 0
        for dj in e["dungeons"].values():
            dj["avg_key"] = round(dj.pop("_ka") / dj["runs"], 2) if dj["runs"] else 0
        out.append(e)
    return out


def _spec_play_in_family(fam, spec_id):
    """Best occurrence of spec_id in fam as (is_main, max_key, play_runs, slot_index), or None
    if the spec is absent from the displayed slots. A main-slot occurrence (spec already in the
    core comp) is preferred over a flex alternate; otherwise the occurrence that reached the
    highest key done wins, then the higher-played one."""
    best = None
    for p, slot in enumerate(fam.get("slots", [])):
        if slot["spec"] == spec_id:
            cand = (True, slot.get("max_key", 0), slot.get("primary_runs", 0), p)
        else:
            alt = next((a for a in slot.get("alts", []) if a["spec"] == spec_id), None)
            if alt is None:
                continue
            cand = (False, alt.get("max_key", 0), alt.get("runs", 0), p)
        if best is None or cand > best:
            best = cand
    return best


def _swap_view(fam, p, spec_id, spec_lookup, class_lookup):
    """A shallow copy of fam whose slot p shows spec_id as the main (the displaced meta spec
    becomes an alternate), so the rendered 5-spec comp always contains spec_id. Never mutates
    fam — one family is surfaced under many specs."""
    orig = fam["slots"][p]
    tgt = next((a for a in orig.get("alts", []) if a["spec"] == spec_id), None)

    new_main = _spec_meta(spec_lookup, class_lookup, spec_id)
    new_main["primary_pct"] = tgt["pct"] if tgt else orig.get("primary_pct", 0)
    new_main["primary_runs"] = tgt["runs"] if tgt else orig.get("primary_runs", 0)
    new_main["max_key"] = tgt["max_key"] if tgt else orig.get("max_key", 0)

    displaced = _spec_meta(spec_lookup, class_lookup, orig["spec"])
    displaced["pct"] = orig.get("primary_pct", 0)
    displaced["runs"] = orig.get("primary_runs", 0)
    displaced["max_key"] = orig.get("max_key", 0)

    alts = [displaced] + [a for a in orig.get("alts", []) if a["spec"] != spec_id]
    alts.sort(key=lambda a: (a.get("max_key", 0), a.get("runs", 0)), reverse=True)
    new_main["alts"] = alts
    new_main["top_alt"] = alts[0] if alts else None
    new_main["more"] = max(0, len(alts) - 1)
    new_main["hidden"] = orig.get("hidden", 0)

    new_slots = list(fam["slots"])
    new_slots[p] = new_main
    new_c = list(fam["c"])
    new_c[p] = spec_id

    view = dict(fam)
    view["slots"] = new_slots
    view["c"] = new_c
    return view


def spec_team_comps(families, spec_lookup, class_lookup, limit=2):
    """Per-spec team comps for the spec pages: {spec_id: [view, ...]}. Every view's rendered
    comp is guaranteed to contain the spec — swapped into its slot when the spec is only a
    flex alternate (see _swap_view). Families are ranked by the highest key the spec reached
    in them (ties broken by how much the spec is actually played), not by the family's overall
    popularity."""
    by_spec = defaultdict(list)
    for f in families:
        specs = set()
        for slot in f.get("slots", []):
            specs.add(slot["spec"])
            specs.update(a["spec"] for a in slot.get("alts", []))
        for sid in specs:
            best = _spec_play_in_family(f, sid)
            if best is None:
                continue
            is_main, key, play, slot_idx = best
            by_spec[sid].append((key, play, is_main, f, slot_idx))

    out = {}
    for sid, cands in by_spec.items():
        cands.sort(key=lambda c: (c[0], c[1]), reverse=True)
        out[sid] = [f if is_main else _swap_view(f, slot_idx, sid, spec_lookup, class_lookup)
                    for key, play, is_main, f, slot_idx in cands[:limit]]
    return out


def top_comps_with_spec(collapsed_comps, spec_id, spec_lookup, class_lookup, limit=2):
    """Fallback for the spec pages: the highest-key raw comps (no archetype clustering) that
    contain spec_id, shaped for the comp_slots macro. Ranked by highest key done (ties broken
    by popularity). Used only when spec_team_comps has no family for the spec, so the panel is
    never empty."""
    matches = [e for e in collapsed_comps if spec_id in e.get("c", [])]
    matches.sort(key=lambda e: (e.get("mk", 0), e.get("runs", 0)), reverse=True)
    views = []
    for e in matches[:limit]:
        runs = int(e.get("runs", 0))
        timed = int(e.get("t", 0))
        slots = []
        for sid in e["c"]:
            m = _spec_meta(spec_lookup, class_lookup, sid)
            m["primary_pct"] = 100
            m["primary_runs"] = runs
            m["max_key"] = int(e.get("mk", 0))
            m["top_alt"] = None
            m["more"] = 0
            m["alts"] = []
            m["hidden"] = 0
            slots.append(m)
        views.append({
            "c": list(e["c"]),
            "slots": slots,
            "runs": runs,
            "t": timed,
            "d": int(e.get("d", 0)),
            "mk": int(e.get("mk", 0)),
            "avg_key": e.get("avg_key", 0),
            "success": round(timed / runs * 100) if runs else 0,
            "members": 1,
        })
    return views


class _Comp:
    __slots__ = ("specs", "weight", "runs", "timed", "depleted", "max_key", "avg_key", "kl",
                 "dungeons")

    def __init__(self, specs, weight, runs, timed, depleted, max_key, avg_key, kl, dungeons):
        self.specs = specs
        self.weight = weight
        self.runs = runs
        self.timed = timed
        self.depleted = depleted
        self.max_key = max_key
        self.avg_key = avg_key
        self.kl = kl            # {key_level(int): {'r': runs, 't': timed}}
        self.dungeons = dungeons  # {dungeon_id: {'t','d','runs','mk'}} — only for the 'all' ctx


def _load_records(comps_json, ctx=None):
    """Lightweight comp records for a context. ctx=None uses each comp's global stats;
    ctx=<dungeon id> uses that comp's slice from its `dungeons` map. Handles int- or
    str-keyed dungeon maps (in-memory frontend uses int, on-disk JSON uses str)."""
    recs = []
    for e in comps_json:
        specs = tuple(int(s) for s in e.get("c", []))
        if len(specs) != 5:
            continue
        if ctx is None:
            src = e
        else:
            dungeons = e.get("dungeons") or {}
            src = dungeons.get(ctx)
            if src is None:
                src = dungeons.get(str(ctx))
            if src is None:
                try:
                    src = dungeons.get(int(ctx))
                except (TypeError, ValueError):
                    src = None
            if not src:
                continue
        timed = int(src.get("t", 0))
        depleted = int(src.get("d", 0))
        runs = int(src.get("runs", timed + depleted))
        if runs <= 0:
            continue
        kl = {int(lvl): {"r": int(v.get("r", 0)), "t": int(v.get("t", 0))}
              for lvl, v in (src.get("kl") or {}).items()}
        # Per-dungeon split, kept only for the 'all' context (src == the comp's global row,
        # which carries its `dungeons` map). Lets a family expose a merged per-dungeon
        # breakdown in the details view; empty for single-dungeon contexts.
        dungeons = {int(did): {"t": int(v.get("t", 0)), "d": int(v.get("d", 0)),
                               "runs": int(v.get("runs", v.get("t", 0) + v.get("d", 0))),
                               "mk": int(v.get("mk", 0))}
                    for did, v in (src.get("dungeons") or {}).items()}
        recs.append(_Comp(
            specs=specs,
            weight=float(src.get("w", 0.0)),
            runs=runs,
            timed=timed,
            depleted=depleted,
            max_key=int(src.get("mk", 0)),
            avg_key=float(src.get("avg_key", 0) or 0),
            kl=kl,
            dungeons=dungeons,
        ))
    return recs


def top_keylevels(recs, k, min_runs=MIN_HIGH_RUNS):
    """The k highest key levels across `recs`: the highest level always counts, then the
    next-highest levels that clear `min_runs` runs, up to k total (highest first)."""
    agg = defaultdict(int)
    for r in recs:
        for lvl, v in r.kl.items():
            agg[lvl] += v["r"]
    if not agg:
        return set()
    present = sorted(agg, reverse=True)     # highest first
    ordered = [present[0]] + [lvl for lvl in present if agg[lvl] >= min_runs]
    sel = []
    for lvl in ordered:
        if lvl not in sel:
            sel.append(lvl)
        if len(sel) >= k:
            break
    return set(sel[:k])


def _core_rank(c):
    """Ranking key for a family's core / clustering seed: a comp with enough runs to be
    trusted outranks any rarer comp, then highest key done, then popularity. group_leader()
    and _slot_alternates() use the SAME key, so every family's core equals its seed and no
    two families ever display cores within one spec-swap of each other."""
    return (c.runs >= MIN_CORE_RUNS, c.max_key, c.weight)


def group_leader(recs, radius=1):
    """Greedy leader clustering. Seed a family from the highest-key well-played unassigned comp
    (see _core_rank), absorb every comp within `radius` spec-swaps of it (share >= 5 - radius
    specs), repeat. Seeding by the same key used to pick the displayed core keeps every family's
    core equal to its seed, so no two families are ever within one swap of each other.
    Returns {seed_index: [member_index, ...]} with the seed first."""
    n = len(recs)
    share_needed = 5 - radius

    index = defaultdict(list)
    subs_of = []
    for i, c in enumerate(recs):
        subs = {tuple(sorted(x)) for x in combinations(c.specs, share_needed)}
        subs_of.append(subs)
        for s in subs:
            index[s].append(i)

    order = sorted(range(n), key=lambda i: _core_rank(recs[i]), reverse=True)
    assigned = [False] * n
    groups = {}
    for seed in order:
        if assigned[seed]:
            continue
        assigned[seed] = True
        members = [seed]
        neighbours = set()
        for s in subs_of[seed]:
            neighbours.update(index[s])
        for j in neighbours:
            if not assigned[j]:
                assigned[j] = True
                members.append(j)
        groups[seed] = members
    return groups


def _slot_alternates(members, recs):
    """Split a radius-1 family's core comp into 5 slots and collect, per slot, the alternate
    specs that swapped into it. The core is the member comp with the best _core_rank — the
    highest key done among comps with enough runs to be trusted (ties broken by popularity), so
    the archetype is defined by its best key result rather than by raw play, without letting a
    fluke one-off key define it. Because group_leader() seeds families with the same key, this
    core equals the family's seed, guaranteeing distinct cores across families. Each alternate
    carries the highest key its swap reached, and the slots list alternates ordered by that key
    (then by how often the swap is played)."""
    rep_idx = max(members, key=lambda i: _core_rank(recs[i]))
    rep = list(recs[rep_idx].specs)
    rep_mk = recs[rep_idx].max_key
    rep_cnt = Counter(rep)
    family_runs = sum(recs[i].runs for i in members)

    slots = [{"spec": rep[p], "alts": []} for p in range(len(rep))]
    for i in members:
        if i == rep_idx:
            continue
        m = Counter(recs[i].specs)
        removed = list((rep_cnt - m).elements())
        added = list((m - rep_cnt).elements())
        if len(removed) != 1 or len(added) != 1:
            continue
        y, x = removed[0], added[0]
        for p in range(len(rep)):
            if slots[p]["spec"] == y:
                slots[p]["alts"].append((x, recs[i].runs, recs[i].max_key))
                break

    for p in range(len(rep)):
        alt_runs = sum(r for _, r, _ in slots[p]["alts"])
        slots[p]["primary_runs"] = family_runs - alt_runs
        slots[p]["total"] = family_runs
        slots[p]["max_key"] = rep_mk
        slots[p]["alts"].sort(key=lambda t: (t[2], t[1]), reverse=True)
    return rep, slots


def _class_color(class_lookup, class_id):
    col = (class_lookup.get(str(class_id), {}) or {}).get("color", {})
    try:
        return "#%02x%02x%02x" % (int(col["r"]), int(col["g"]), int(col["b"]))
    except (KeyError, ValueError, TypeError):
        return "#c8c8c8"


def _spec_meta(spec_lookup, class_lookup, sid):
    m = spec_lookup.get(str(sid), {}) or {}
    cid = m.get("classID")
    return {
        "spec": sid,
        "icon": m.get("SpellIconFileId", ""),
        "name": m.get("name", str(sid)),
        "class": (class_lookup.get(str(cid), {}) or {}).get("name", ""),
        "color": _class_color(class_lookup, cid),
        "role": int(m.get("role", 2)),
    }


def _level_label(levels):
    """Human range for a set of key levels: 'key levels 23-25', 'key level 25', or a
    fallback when the context has no key-level data."""
    lv = sorted(levels)
    if not lv:
        return "the top key levels"
    if lv[0] == lv[-1]:
        return f"key level {lv[-1]}"
    return f"key levels {lv[0]}-{lv[-1]}"


def _level_stats(members, recs, levels):
    """Runs / timed / highest-level-reached / exponential score for a family, restricted
    to `levels` (a set of key levels)."""
    runs = timed = mx = 0
    score = 0.0
    for i in members:
        for lvl in levels:
            v = recs[i].kl.get(lvl)
            if not v:
                continue
            runs += v["r"]
            timed += v["t"]
            if v["r"] > 0 and lvl > mx:
                mx = lvl
            kf = max(1, lvl - 9)
            score += v["t"] * (kf ** HIGHKEY_EXP) + (v["r"] - v["t"]) * 0.1 * (kf ** HIGHKEY_EXP)
    return {"runs": runs, "timed": timed, "max": mx, "score": round(score, 2),
            "success": round(timed / runs * 100) if runs else 0}


def _family(members, recs, spec_lookup, class_lookup, min_alt_frac, hk_levels, gem_levels):
    """Build one team-comp family dict: meta comp + per-slot alternates + stats (overall,
    high-key restricted, and gem restricted)."""
    rep, slots = _slot_alternates(members, recs)
    arch_slots = []
    for slot in slots:
        total = slot["total"] or 1
        alts = []
        for x, r, mk in slot["alts"]:
            pct = r / total * 100
            if pct >= min_alt_frac * 100:
                meta = _spec_meta(spec_lookup, class_lookup, x)
                meta["pct"] = round(pct)
                meta["runs"] = r
                meta["max_key"] = mk
                alts.append(meta)
        hidden = len(slot["alts"]) - len(alts)
        main = _spec_meta(spec_lookup, class_lookup, slot["spec"])
        main["primary_pct"] = round(slot["primary_runs"] / total * 100)
        main["primary_runs"] = slot["primary_runs"]
        main["max_key"] = slot["max_key"]
        # alts arrive sorted by highest key done, so top_alt is the highest-key swap
        main["top_alt"] = alts[0] if alts else None
        main["more"] = max(0, len(alts) - 1)
        main["alts"] = alts
        main["hidden"] = hidden
        arch_slots.append(main)

    runs = sum(recs[i].runs for i in members)
    timed = sum(recs[i].timed for i in members)
    key_runs = sum(recs[i].avg_key * recs[i].runs for i in members)
    hk = _level_stats(members, recs, hk_levels)
    gem = _level_stats(members, recs, gem_levels)

    # Merged per-dungeon breakdown across every family member, so the details view can show
    # the whole archetype's numbers (not just the core comp's). Only populated for the 'all'
    # context, where members carry their per-dungeon split; empty for single-dungeon contexts.
    fam_dungeons = {}
    for i in members:
        for did, ds in recs[i].dungeons.items():
            agg = fam_dungeons.setdefault(did, {"t": 0, "d": 0, "runs": 0, "mk": 0})
            agg["t"] += ds["t"]
            agg["d"] += ds["d"]
            agg["runs"] += ds["runs"]
            agg["mk"] = max(agg["mk"], ds["mk"])
    best_did, best_runs = None, 0
    for did, ds in fam_dungeons.items():
        if ds["runs"] > best_runs:
            best_did, best_runs = did, ds["runs"]

    return {
        "c": list(rep),
        "runs": runs,
        "t": timed,
        "d": sum(recs[i].depleted for i in members),
        "mk": max((recs[i].max_key for i in members), default=0),
        "avg_key": round(key_runs / runs, 2) if runs else 0,
        "success": round(timed / runs * 100) if runs else 0,
        "members": len(members),
        "slots": arch_slots,
        "weight": sum(recs[i].weight for i in members),
        # merged per-dungeon breakdown + busiest dungeon (for the details view)
        "dungeons": fam_dungeons,
        "bd": best_did,
        "bdr": best_runs,
        # high-key band (highest 2 levels)
        "hk_runs": hk["runs"], "hk_timed": hk["timed"], "hk_max": hk["max"],
        "hk_score": hk["score"], "hk_success": hk["success"],
        # gem band (highest 5 levels)
        "gem_runs": gem["runs"], "gem_timed": gem["timed"], "gem_max": gem["max"],
        "gem_success": gem["success"],
    }


def _rank_gems(families, top_n):
    """Niche families that punch above their weight in the high-key band: rare (< share of
    the busiest family's high-key runs) but with high success at those levels."""
    playable = [f for f in families if f["gem_runs"] >= GEM_MIN_RUNS]
    if not playable:
        return []
    busiest = max(f["gem_runs"] for f in playable) or 1
    gems = [
        f for f in playable
        if f["gem_runs"] < GEM_MAX_SHARE * busiest
        and f["gem_success"] >= GEM_MIN_SUCCESS
    ]
    gems.sort(key=lambda f: (f["gem_max"], f["gem_success"], f["gem_runs"]), reverse=True)
    return gems[:top_n]


def build_archetypes(comps_json, spec_lookup, class_lookup,
                     top_n=8, min_alt_frac=0.01, radius=1):
    """Global (all-dungeon) families ranked by weight — for tools/callers that want a
    single list. The page uses build_dungeon_archetypes()."""
    recs = _load_records(comps_json)
    if not recs:
        return []
    hk = top_keylevels(recs, HIGHKEY_LEVELS)
    gem = top_keylevels(recs, GEM_LEVELS)
    groups = group_leader(recs, radius=radius)
    fams = [_family(m, recs, spec_lookup, class_lookup, min_alt_frac, hk, gem)
            for m in groups.values()]
    fams.sort(key=lambda f: f["weight"], reverse=True)
    return fams[:top_n] if top_n else fams


def build_dungeon_archetypes(comps_json, spec_lookup, class_lookup, dungeon_ids,
                             top_n=6, min_alt_frac=0.01, radius=1):
    """Per-context families for the comps page. Returns
        { 'all' | '<dungeon id>' : { 'popular': [...], 'highkey': [...], 'gems': [...] } }
    Each list holds up to top_n family dicts (see _family) ranked for that card, with the
    high-key / gem bands taken from that context's highest key levels."""
    out = {}
    for ctx in [None] + list(dungeon_ids):
        key = "all" if ctx is None else str(ctx)
        recs = _load_records(comps_json, ctx)
        if not recs:
            out[key] = {"popular": [], "highkey": [], "gems": []}
            continue
        hk_levels = top_keylevels(recs, HIGHKEY_LEVELS)
        gem_levels = top_keylevels(recs, GEM_LEVELS)
        groups = group_leader(recs, radius=radius)
        families = [_family(m, recs, spec_lookup, class_lookup, min_alt_frac,
                            hk_levels, gem_levels)
                    for m in groups.values()]

        popular = sorted((f for f in families if f["runs"] >= MIN_RANK_RUNS),
                         key=lambda f: f["runs"], reverse=True)[:top_n]
        highkey = sorted((f for f in families if f["hk_runs"] > 0),
                         key=lambda f: (f["hk_score"], f["hk_runs"]), reverse=True)[:top_n]
        out[key] = {
            "popular": popular,
            "highkey": highkey,
            "gems": _rank_gems(families, top_n),
            # key-level bands for the card descriptions
            "hk_levels": sorted(hk_levels),
            "gem_levels": sorted(gem_levels),
            "hk_label": _level_label(hk_levels),
            "gem_label": _level_label(gem_levels),
        }
    return out
