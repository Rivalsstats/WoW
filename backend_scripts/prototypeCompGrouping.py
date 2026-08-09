"""Phase 1 prototype: group "close" team comps into archetypes and compare methods.

This is a throwaway comparison harness (it ships nothing to the page). It runs two
grouping strategies over the season's distinct comps and prints a side-by-side report so
we can pick the one that produces the best archetypes.

Two interchangeable input sources so it runs with or without a DB:

  * --from-json PATH   (default: assets/json/comps_index.json)
        Reads the already-collapsed top comps the page ships. Each entry has the comp
        (`c`), a precomputed weight (`w`), timed (`t`), depleted (`d`) and `runs`.

  * --season ID
        Reads Mythistone.aggregated_dungeon_comps via databaseConnector.fetch_all_comps
        and reproduces generateCompPage.calculate_comp_stats' collapse + weighting.

Methods compared:

  Method 1 - Core + flex (frequent sub-multiset)
      Find shared "cores" (sub-multisets of 3 or 4 specs) that span >=2 comps and carry
      enough weight; assign each comp to its single strongest containing core (partition).
      A core = the locked part of the comp, the rest is flex.

  Method 2 - Swap-distance (connected components)
      Link comps that differ by one spec (share >=4 of 5) and take connected components.
      Reported in two variants to expose the single-linkage chaining failure mode:
        - raw        : any one-spec swap links comps
        - dps-flex   : only a DPS swap links comps (tank+healer stay fixed) -> guard

Run examples:
    python backend_scripts/prototypeCompGrouping.py
    python backend_scripts/prototypeCompGrouping.py --from-json assets/json/comps_index.json
    python backend_scripts/prototypeCompGrouping.py --season 15
"""

import argparse
import json
import math
import os
import sys
import time
from collections import defaultdict
from itertools import combinations

# --- paths -------------------------------------------------------------------
HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
SPECS_PATH = os.path.join(REPO, "data", "static", "specs.json")
CLASSES_PATH = os.path.join(REPO, "data", "static", "classes.json")
DEFAULT_JSON = os.path.join(REPO, "assets", "json", "comps_index.json")


# --- data model --------------------------------------------------------------
class Comp:
    __slots__ = ("specs", "weight", "runs", "timed", "depleted", "max_key")

    def __init__(self, specs, weight, runs, timed, depleted, max_key=0):
        # specs: tuple of ints, canonically ordered (role, spec_id)
        self.specs = specs
        self.weight = weight
        self.runs = runs
        self.timed = timed
        self.depleted = depleted
        self.max_key = max_key


def load_classes():
    with open(CLASSES_PATH, "r", encoding="utf-8") as fh:
        raw = json.load(fh)
    out = {}
    for cid, meta in raw.items():
        col = meta.get("color", {})
        out[int(cid)] = {
            "name": meta.get("name", str(cid)),
            "color": "#%02x%02x%02x" % (
                int(col.get("r", 200)), int(col.get("g", 200)), int(col.get("b", 200))
            ),
        }
    return out


def load_specs():
    with open(SPECS_PATH, "r", encoding="utf-8") as fh:
        raw = json.load(fh)
    classes = load_classes()
    # spec_id (int) -> {"name", "role" (int), "class", "color"}
    out = {}
    for sid, meta in raw.items():
        cid = int(meta.get("classID", 0))
        cls = classes.get(cid, {})
        out[int(sid)] = {
            "name": meta.get("name", str(sid)),
            "role": int(meta.get("role", 2)),
            "class": cls.get("name", ""),
            "color": cls.get("color", "#c8c8c8"),
            "icon_file": meta.get("SpellIconFileId", ""),
        }
    return out


def canonical_specs(specs, spec_lookup):
    """Sort by (role, spec_id) - matches generateCompPage.calculate_comp_stats."""
    return tuple(
        sorted(specs, key=lambda s: (spec_lookup.get(s, {}).get("role", 2), s))
    )


def spec_name(sid, spec_lookup):
    meta = spec_lookup.get(sid, {})
    name = meta.get("name", str(sid))
    cls = meta.get("class", "")
    return f"{name} {cls}".strip() if cls else name


def label_specs(specs, spec_lookup):
    return " + ".join(spec_name(s, spec_lookup) for s in specs)


# --- input sources -----------------------------------------------------------
def load_from_json(path, spec_lookup):
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    comps = []
    for entry in data:
        specs = [int(s) for s in entry["c"]]
        if len(specs) != 5:
            continue
        specs = canonical_specs(specs, spec_lookup)
        timed = int(entry.get("t", 0))
        depleted = int(entry.get("d", 0))
        comps.append(
            Comp(
                specs=specs,
                weight=float(entry.get("w", 0.0)),
                runs=int(entry.get("runs", timed + depleted)),
                timed=timed,
                depleted=depleted,
                max_key=int(entry.get("mk", 0)),
            )
        )
    return comps


def load_from_db(season, spec_lookup):
    """Reproduce calculate_comp_stats' collapse from aggregated_dungeon_comps."""
    import databaseConnector  # noqa: local, only needed for DB path

    connection = databaseConnector.get_connection()
    cursor = connection.cursor()
    raw = databaseConnector.fetch_all_comps(connection, cursor, season)

    compiled = {}  # comp_hash -> aggregate dict
    for row in raw:
        dungeon_id, keystone_level, comp_str, timed, depleted = (
            row[0], int(row[1]), row[2], int(row[3]), int(row[4])
        )
        specs = [int(s) for s in comp_str.split(",") if s.strip()]
        if len(specs) != 5:
            continue
        specs = canonical_specs(specs, spec_lookup)
        # exponential weight, mirrors generateCompPage.py:61-65
        key_factor = max(1, keystone_level - 9)
        w_timed = math.pow(key_factor, 2)
        w_dep = w_timed * 0.1
        row_weight = (timed * w_timed) + (depleted * w_dep)
        agg = compiled.get(specs)
        if agg is None:
            agg = {"weight": 0.0, "timed": 0, "depleted": 0, "max_key": 0}
            compiled[specs] = agg
        agg["weight"] += row_weight
        agg["timed"] += timed
        agg["depleted"] += depleted
        if keystone_level > agg["max_key"]:
            agg["max_key"] = keystone_level

    try:
        cursor.close()
        connection.close()
    except Exception:
        pass

    comps = []
    for specs, agg in compiled.items():
        comps.append(
            Comp(
                specs=specs,
                weight=agg["weight"],
                runs=agg["timed"] + agg["depleted"],
                timed=agg["timed"],
                depleted=agg["depleted"],
                max_key=agg["max_key"],
            )
        )
    return comps


# --- Method 1: core + flex ---------------------------------------------------
def group_core_flex(comps, core_sizes=(4, 3), min_weight_frac=0.001, min_comps=2):
    """Assign each comp to its single strongest containing core (partition).

    core_sizes: which sub-multiset sizes may act as a core, in preference order.
        (4,)   -> 4 locked specs + 1 flex slot   (tightest archetypes)
        (3,)   -> 3 locked specs + 2 flex slots  (broader families)
        (4, 3) -> prefer a 4-core, fall back to a 3-core

    A candidate core spans >= min_comps comps and carries >= min_weight_frac of total
    weight. Each comp picks the best core it contains, preferring a larger size, then
    heavier weight. Comps with no qualifying core become singleton archetypes.
    """
    total_weight = sum(c.weight for c in comps) or 1.0
    min_weight = min_weight_frac * total_weight

    # tally weight + distinct-comp count per candidate sub-multiset
    core_weight = defaultdict(float)
    core_ncomps = defaultdict(int)
    comp_subsets = []  # parallel to comps: set of candidate core tuples
    for c in comps:
        subs = set()
        for size in core_sizes:
            for combo in combinations(c.specs, size):
                subs.add(tuple(sorted(combo)))
        comp_subsets.append(subs)
        for core in subs:
            core_weight[core] += c.weight
            core_ncomps[core] += 1

    def qualifies(core):
        return core_ncomps[core] >= min_comps and core_weight[core] >= min_weight

    # assign each comp to the best qualifying core it contains
    groups = defaultdict(list)  # core_tuple | ("SINGLETON", i) -> [comp idx]
    for i, c in enumerate(comps):
        best = None  # (size, weight, core)
        for core in comp_subsets[i]:
            if not qualifies(core):
                continue
            key = (len(core), core_weight[core], core)
            if best is None or key > best:
                best = key
        if best is None:
            groups[("SINGLETON", i)].append(i)
        else:
            groups[best[2]].append(i)
    return groups


# --- Method 3: greedy leader clustering (built-in minimum distance) ----------
def group_leader(comps, radius=1):
    """Greedy "leader" clustering with a guaranteed minimum distance between families.

    Repeatedly take the heaviest still-unassigned comp as a family seed (its 5 specs =
    the representative meta comp) and absorb every unassigned comp within `radius` spec
    swaps of it (share >= 5 - radius specs). Because a new seed can only be an
    unassigned comp, no two seeds are ever within `radius` of each other -> two families
    can never describe the same region, which is exactly the redundancy we want gone.

    radius=1 -> meta comp + its one-swap variants (share >=4)
    radius=2 -> broader families                (share >=3)

    Returns {seed_idx: [member idx, ...]} with the seed first in each list.
    """
    n = len(comps)
    share_needed = 5 - radius

    # inverted index: sub-multiset of size share_needed -> comps containing it.
    # Two comps share >= share_needed specs iff they share such a sub-multiset.
    index = defaultdict(list)
    subs_of = []
    for i, c in enumerate(comps):
        subs = {tuple(sorted(x)) for x in combinations(c.specs, share_needed)}
        subs_of.append(subs)
        for s in subs:
            index[s].append(i)

    order = sorted(range(n), key=lambda i: comps[i].weight, reverse=True)
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


# --- Method 2: swap-distance connected components ----------------------------
class UnionFind:
    def __init__(self, n):
        self.parent = list(range(n))

    def find(self, x):
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[x] != root:
            self.parent[x], x = root, self.parent[x]
        return root

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[ra] = rb


def group_swap_distance(comps, spec_lookup, dps_only=False):
    """Connected components where an edge = one-spec swap (share >=4 of 5).

    Two 5-multisets share >=4 iff they share a common 4-sub-multiset, so we bucket by
    4-subset and union everything in a bucket - near linear, no O(n^2) pair scan.

    dps_only=True keeps tank+healer fixed by only forming an edge through a 4-subset
    whose removed spec is a DPS (role 2). This is the chaining guard.
    """
    uf = UnionFind(len(comps))
    buckets = defaultdict(list)  # 4-subset tuple -> [comp idx]
    for i, c in enumerate(comps):
        for j in range(5):
            removed = c.specs[j]
            if dps_only and spec_lookup.get(removed, {}).get("role", 2) != 2:
                continue
            key = c.specs[:j] + c.specs[j + 1:]  # the retained 4 (already sorted)
            buckets[key].append(i)
    for members in buckets.values():
        first = members[0]
        for other in members[1:]:
            uf.union(first, other)

    groups = defaultdict(list)
    for i in range(len(comps)):
        groups[uf.find(i)].append(i)
    return groups


# --- reporting ---------------------------------------------------------------
def shared_core(members, comps):
    """Specs present in every member comp (multiset intersection)."""
    from collections import Counter
    inter = None
    for idx in members:
        cnt = Counter(comps[idx].specs)
        inter = cnt if inter is None else (inter & cnt)
    out = []
    for sid, n in (inter or {}).items():
        out.extend([sid] * n)
    return tuple(sorted(out))


def flex_distribution(members, core, comps, spec_lookup):
    """Weight-share of the specs that vary outside the shared core, top few."""
    from collections import Counter
    core_cnt = Counter(core)
    flex_weight = defaultdict(float)
    for idx in members:
        c = comps[idx]
        remaining = Counter(c.specs) - core_cnt
        for sid in remaining.elements():
            flex_weight[sid] += c.weight
    total = sum(flex_weight.values()) or 1.0
    ranked = sorted(flex_weight.items(), key=lambda kv: kv[1], reverse=True)
    return [(sid, w / total) for sid, w in ranked]


def family_base(members, comps, header_mode):
    """Locked-spec base for a family: the representative (heaviest) comp's 5 specs for
    'rep' families (leader clustering), or the shared-core intersection for 'core'."""
    if header_mode == "rep":
        rep = max(members, key=lambda i: comps[i].weight)
        return comps[rep].specs
    return shared_core(members, comps)


def report(method_name, groups, comps, spec_lookup, top_n=10, header_mode="core"):
    total_weight = sum(c.weight for c in comps) or 1.0
    n_comps = len(comps)

    group_weights = []
    for key, members in groups.items():
        gw = sum(comps[i].weight for i in members)
        group_weights.append((gw, key, members))
    group_weights.sort(reverse=True, key=lambda x: x[0])

    n_groups = len(group_weights)
    multi = [g for g in group_weights if len(g[2]) > 1]
    singletons = n_groups - len(multi)
    multi_weight = sum(g[0] for g in multi)
    largest_share = (group_weights[0][0] / total_weight) if group_weights else 0.0

    print("\n" + "=" * 78)
    print(f"METHOD: {method_name}")
    print("-" * 78)
    print(f"  input comps            : {n_comps}")
    print(f"  archetypes (groups)    : {n_groups}")
    print(f"  multi-comp archetypes  : {len(multi)}")
    print(f"  singleton archetypes   : {singletons}  "
          f"({singletons / n_comps * 100:.1f}% of comps)")
    print(f"  weighted coverage      : {multi_weight / total_weight * 100:.1f}%  "
          f"(weight inside multi-comp archetypes)")
    print(f"  largest archetype share: {largest_share * 100:.1f}%  "
          f"(>~30% suggests chaining/blob)")
    label = "meta comp" if header_mode == "rep" else "core"
    print(f"\n  Top {top_n} archetypes by weight:")
    for rank, (gw, key, members) in enumerate(group_weights[:top_n], 1):
        base = family_base(members, comps, header_mode)
        runs = sum(comps[i].runs for i in members)
        share = gw / total_weight * 100
        if len(members) == 1:
            print(f"   {rank:2}. [single] {label_specs(comps[members[0]].specs, spec_lookup)}"
                  f"  | runs={runs:,} share={share:.1f}%")
            continue
        flex = flex_distribution(members, base, comps, spec_lookup)
        flex_str = ", ".join(
            f"{spec_name(sid, spec_lookup)} {frac * 100:.0f}%" for sid, frac in flex[:4]
        )
        print(f"   {rank:2}. {label}: {label_specs(base, spec_lookup)}")
        print(f"        members={len(members)} runs={runs:,} share={share:.1f}% "
              f"| flex: {flex_str}")


# --- HTML report (exact member comps for evaluation) -------------------------
def _esc(s):
    return (str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


def _spec_chip(sid, spec_lookup):
    meta = spec_lookup.get(sid, {})
    return (f'<span class="chip" style="border-color:{meta.get("color", "#888")}">'
            f'{_esc(meta.get("name", sid))}'
            f'<em>{_esc(meta.get("class", ""))}</em></span>')


def _comp_row(comp, spec_lookup, arch_weight, core):
    """Render one member comp: locked (core) specs first, then flex specs.
    Both groups keep (role, spec_id) ordering; a divider marks the boundary.
    """
    from collections import Counter
    role_key = lambda s: (spec_lookup.get(s, {}).get("role", 2), s)
    core_cnt = Counter(core or ())
    locked, flex = [], []
    for s in comp.specs:
        if core_cnt.get(s, 0) > 0:
            core_cnt[s] -= 1
            locked.append(s)
        else:
            flex.append(s)
    locked.sort(key=role_key)
    flex.sort(key=role_key)
    chips = "".join(_spec_chip(s, spec_lookup) for s in locked)
    if flex:
        chips += '<span class="divider"></span>'
        chips += "".join(_spec_chip(s, spec_lookup) for s in flex)
    share = comp.weight / arch_weight * 100 if arch_weight else 0
    return (f'<div class="comp"><div class="chips">{chips}</div>'
            f'<div class="cstats">{comp.runs:,} runs &middot; mk {comp.max_key} '
            f'&middot; {share:.0f}% of family</div></div>')


def build_html(methods, comps, spec_lookup, out_path, source_desc,
               min_member_frac=0.01):
    total_weight = sum(c.weight for c in comps) or 1.0
    parts = [f"""<!doctype html><html><head><meta charset="utf-8">
<title>Comp archetype comparison</title><style>
:root{{color-scheme:dark light}}
body{{font:14px/1.5 system-ui,sans-serif;margin:0;padding:24px;
 background:#14161a;color:#e7e9ee}}
h1{{font-size:20px;margin:0 0 4px}} h2{{font-size:17px;margin:28px 0 6px}}
.src{{color:#8a90a0;font-size:12px;margin-bottom:16px}}
.summary{{background:#1d2027;border:1px solid #2b2f39;border-radius:8px;
 padding:10px 14px;margin:8px 0 14px;font-size:13px}}
.summary b{{color:#fff}}
details{{background:#1a1d23;border:1px solid #2b2f39;border-radius:8px;
 margin:6px 0;padding:6px 12px}}
details[open]{{background:#191c22}}
summary{{cursor:pointer;list-style:none;display:flex;gap:10px;align-items:baseline;
 flex-wrap:wrap}}
summary::-webkit-details-marker{{display:none}}
.rank{{color:#6b7180;font-variant-numeric:tabular-nums;min-width:26px}}
.core{{font-weight:600}}
.badge{{color:#8a90a0;font-size:12px}}
.flex{{color:#c7b06b;font-size:12px;margin:2px 0 8px 36px}}
.members{{margin:4px 0 8px 36px;display:grid;gap:4px}}
.comp{{display:flex;justify-content:space-between;gap:12px;align-items:center;
 padding:3px 8px;background:#20242c;border-radius:6px}}
.chips{{display:flex;gap:4px;flex-wrap:wrap}}
.chip{{border:1px solid #888;border-left-width:4px;border-radius:4px;
 padding:1px 6px;font-size:12px;white-space:nowrap}}
.chip em{{color:#8a90a0;font-style:normal;margin-left:4px;font-size:10px}}
.divider{{width:2px;align-self:stretch;background:#3a4050;border-radius:2px;margin:0 4px}}
.cstats{{color:#8a90a0;font-size:11px;white-space:nowrap;font-variant-numeric:tabular-nums}}
.hidden-note{{color:#6b7180;font-size:11px;font-style:italic;padding:2px 8px}}
.two{{display:grid;grid-template-columns:1fr 1fr;gap:24px;align-items:start}}
@media(max-width:900px){{.two{{grid-template-columns:1fr}}}}
</style></head><body>
<h1>Team-comp archetype comparison</h1>
<div class="src">source: {_esc(source_desc)} &middot; {len(comps):,} distinct comps</div>
<div class="two">"""]

    for method in methods:
        title, subtitle, groups = method[0], method[1], method[2]
        header_mode = method[3] if len(method) > 3 else "core"
        gw = []
        for key, members in groups.items():
            w = sum(comps[i].weight for i in members)
            gw.append((w, key, members))
        gw.sort(reverse=True, key=lambda x: x[0])
        multi = [g for g in gw if len(g[2]) > 1]
        singles = len(gw) - len(multi)
        largest = gw[0][0] / total_weight * 100 if gw else 0

        parts.append(f'<div><h2>{_esc(title)}</h2>')
        parts.append(f'<div class="summary">{_esc(subtitle)}<br>'
                     f'<b>{len(gw)}</b> archetypes &middot; '
                     f'<b>{len(multi)}</b> multi-comp &middot; '
                     f'<b>{singles}</b> singletons &middot; '
                     f'largest family <b>{largest:.1f}%</b> of weight</div>')

        for rank, (w, key, members) in enumerate(gw, 1):
            share = w / total_weight * 100
            members_sorted = sorted(members, key=lambda i: comps[i].runs, reverse=True)
            if len(members) == 1:
                core = comps[members[0]].specs
                header_core = label_specs(core, spec_lookup) + " (singleton)"
            else:
                core = family_base(members, comps, header_mode)
                header_core = label_specs(core, spec_lookup)
            runs = sum(comps[i].runs for i in members)
            parts.append("<details>")
            parts.append(
                f'<summary><span class="rank">{rank}.</span>'
                f'<span class="core">{_esc(header_core)}</span>'
                f'<span class="badge">{len(members)} comps &middot; {runs:,} runs '
                f'&middot; {share:.1f}%</span></summary>')
            if len(members) > 1:
                flex = flex_distribution(members, core, comps, spec_lookup)
                flex_str = ", ".join(
                    f"{spec_name(s, spec_lookup)} {f*100:.0f}%" for s, f in flex[:6])
                parts.append(f'<div class="flex">flex: {_esc(flex_str)}</div>')
            shown = [i for i in members_sorted
                     if len(members) == 1 or (comps[i].weight / w if w else 0) >= min_member_frac]
            hidden = len(members_sorted) - len(shown)
            row_core = None if len(members) == 1 else core
            parts.append('<div class="members">')
            for i in shown:
                parts.append(_comp_row(comps[i], spec_lookup, w, row_core))
            if hidden:
                parts.append(f'<div class="hidden-note">+ {hidden} more comps '
                             f'below {min_member_frac*100:.0f}% of family (hidden)</div>')
            parts.append('</div></details>')
        parts.append('</div>')

    parts.append("</div></body></html>")
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write("".join(parts))
    return out_path


# --- per-slot alternates + card view (leader radius=1 only) ------------------
import base64

_ICON_CACHE = {}


def icon_data_uri(sid, spec_lookup):
    if sid in _ICON_CACHE:
        return _ICON_CACHE[sid]
    fid = spec_lookup.get(sid, {}).get("icon_file", "")
    uri = ""
    if fid:
        path = os.path.join(REPO, "data", "icons", f"{fid}.jpg")
        try:
            with open(path, "rb") as fh:
                uri = "data:image/jpeg;base64," + base64.b64encode(fh.read()).decode()
        except OSError:
            uri = ""
    _ICON_CACHE[sid] = uri
    return uri


def slot_alternates(members, comps):
    """For a leader radius=1 family, break the representative (meta) comp into 5 slots
    and collect, per slot, the alternate specs that swapped into it.

    Every member is <=1 swap from the representative, so each non-seed member removes
    exactly one rep spec (the slot) and adds one (the alternate). Returns:
        rep_specs, slots, family_runs
    where slots[p] = {spec, primary_runs, total, alts:[(alt_spec, runs, weight), ...]}.
    """
    from collections import Counter
    rep_idx = max(members, key=lambda i: comps[i].weight)
    rep = list(comps[rep_idx].specs)  # canonical (role, spec) order
    rep_cnt = Counter(rep)
    family_runs = sum(comps[i].runs for i in members)

    slots = [{"spec": rep[p], "alts": []} for p in range(len(rep))]
    for i in members:
        if i == rep_idx:
            continue
        m = Counter(comps[i].specs)
        removed = list((rep_cnt - m).elements())
        added = list((m - rep_cnt).elements())
        if len(removed) != 1 or len(added) != 1:
            continue  # not a clean one-swap (shouldn't happen at radius=1)
        y, x = removed[0], added[0]
        for p in range(len(rep)):
            if slots[p]["spec"] == y:
                slots[p]["alts"].append((x, comps[i].runs, comps[i].weight))
                break

    for p in range(len(rep)):
        alt_runs = sum(r for _, r, _ in slots[p]["alts"])
        slots[p]["primary_runs"] = family_runs - alt_runs
        slots[p]["total"] = family_runs
        slots[p]["alts"].sort(key=lambda t: t[1], reverse=True)
    return rep, slots, family_runs


def _icon_img(sid, spec_lookup, size, ring=False, title=True):
    meta = spec_lookup.get(sid, {})
    uri = icon_data_uri(sid, spec_lookup)
    ring_style = f"box-shadow:0 0 0 2px {meta.get('color', '#888')}" if ring else ""
    label = f"{meta.get('name', sid)} {meta.get('class', '')}".strip()
    title_attr = f'title="{_esc(label)}" ' if title else ""
    return (f'<img src="{uri}" width="{size}" height="{size}" {title_attr}'
            f'alt="{_esc(label)}" style="border-radius:6px;{ring_style}">')


def build_cards_html(groups, comps, spec_lookup, out_path, source_desc,
                     top_families=30, min_alt_frac=0.01):
    total_weight = sum(c.weight for c in comps) or 1.0
    gw = []
    for key, members in groups.items():
        w = sum(comps[i].weight for i in members)
        gw.append((w, members))
    gw.sort(reverse=True, key=lambda x: x[0])

    parts = [f"""<!doctype html><html><head><meta charset="utf-8">
<title>Comp archetype cards</title><style>
:root{{color-scheme:dark light}}
body{{font:14px/1.5 system-ui,sans-serif;margin:0;padding:24px;
 background:#14161a;color:#e7e9ee}}
h1{{font-size:20px;margin:0 0 4px}}
.src{{color:#8a90a0;font-size:12px;margin-bottom:18px}}
.card{{background:#1a1d23;border:1px solid #2b2f39;border-radius:12px;
 padding:14px 16px;margin:0 0 16px}}
.card-head{{display:flex;gap:10px;align-items:baseline;flex-wrap:wrap;margin-bottom:12px}}
.rank{{color:#6b7180;font-weight:700;font-variant-numeric:tabular-nums}}
.title{{font-weight:600;font-size:15px}}
.head-stats{{color:#8a90a0;font-size:12px;margin-left:auto}}
.slots{{display:grid;grid-template-columns:repeat(5,1fr);gap:10px}}
@media(max-width:760px){{.slots{{grid-template-columns:repeat(2,1fr)}}}}
.slot{{background:#20242c;border:1px solid #2b2f39;border-radius:10px;padding:10px 8px}}
.primary{{display:flex;flex-direction:column;align-items:center;gap:4px;
 padding-bottom:8px;border-bottom:1px dashed #333944}}
.primary .nm{{font-weight:600;font-size:12px;text-align:center}}
.primary .pct{{color:#7fd18b;font-size:11px;font-variant-numeric:tabular-nums}}
.role{{color:#6b7180;font-size:10px;text-transform:uppercase;letter-spacing:.04em;
 text-align:center;margin-bottom:4px}}
.orlbl{{color:#6b7180;font-size:10px;text-align:center;margin:6px 0 4px}}
.alt{{display:flex;align-items:center;gap:6px;padding:2px 2px;border-radius:5px}}
.alt .nm{{font-size:11px;flex:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
.alt .pct{{color:#8a90a0;font-size:10px;font-variant-numeric:tabular-nums}}
.noalt{{color:#6b7180;font-size:11px;text-align:center;font-style:italic;padding:6px 0}}
.more{{color:#6b7180;font-size:10px;font-style:italic;text-align:center;margin-top:4px}}
</style></head><body>
<h1>Team-comp archetype cards &mdash; leader radius=1</h1>
<div class="src">source: {_esc(source_desc)} &middot; top {min(top_families, len(gw))} of
 {len(gw)} families &middot; each slot shows the meta pick then popularity-ranked
 alternates</div>"""]

    ROLE = {0: "Tank", 1: "Healer", 2: "DPS"}
    for rank, (w, members) in enumerate(gw[:top_families], 1):
        rep, slots, family_runs = slot_alternates(members, comps)
        share = w / total_weight * 100
        title = " + ".join(spec_name(s, spec_lookup) for s in rep)
        parts.append('<div class="card"><div class="card-head">'
                     f'<span class="rank">{rank}.</span>'
                     f'<span class="title">{_esc(title)}</span>'
                     f'<span class="head-stats">{len(members)} comps &middot; '
                     f'{family_runs:,} runs &middot; {share:.1f}% of weight</span></div>'
                     '<div class="slots">')
        for slot in slots:
            sid = slot["spec"]
            meta = spec_lookup.get(sid, {})
            ppct = slot["primary_runs"] / slot["total"] * 100 if slot["total"] else 0
            parts.append('<div class="slot">')
            parts.append(f'<div class="role">{ROLE.get(meta.get("role", 2), "DPS")}</div>')
            parts.append('<div class="primary">'
                         f'{_icon_img(sid, spec_lookup, 46, ring=True)}'
                         f'<div class="nm">{_esc(meta.get("name", sid))}</div>'
                         f'<div class="pct">{ppct:.0f}% meta</div></div>')
            shown = [(x, r, wt) for x, r, wt in slot["alts"]
                     if r / slot["total"] >= min_alt_frac] if slot["total"] else []
            hidden = len(slot["alts"]) - len(shown)
            if shown:
                parts.append('<div class="orlbl">or</div>')
                for x, r, wt in shown:
                    xm = spec_lookup.get(x, {})
                    apct = r / slot["total"] * 100 if slot["total"] else 0
                    parts.append(
                        f'<div class="alt">{_icon_img(x, spec_lookup, 22)}'
                        f'<span class="nm" style="color:{xm.get("color", "#ccc")}">'
                        f'{_esc(xm.get("name", x))}</span>'
                        f'<span class="pct">{apct:.0f}%</span></div>')
                if hidden:
                    parts.append(f'<div class="more">+{hidden} more</div>')
            elif not slot["alts"]:
                parts.append('<div class="noalt">locked</div>')
            else:
                parts.append(f'<div class="more">+{hidden} rare alts</div>')
            parts.append('</div>')
        parts.append('</div></div>')

    parts.append("</body></html>")
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write("".join(parts))
    return out_path


def build_card_options_html(groups, comps, spec_lookup, out_path, source_desc,
                            n_families=8, min_alt_frac=0.01):
    """Render the top families two ways at Most-Popular-row scale: B (polished, always-
    visible ranked alternates) as the main proposal, and A (hover popover) as a fallback.
    Percentages = share of this comp's own plays (never labelled 'meta')."""
    total_weight = sum(c.weight for c in comps) or 1.0
    gw = sorted(
        ((sum(comps[i].weight for i in m), m) for m in groups.values()),
        reverse=True, key=lambda x: x[0])[:n_families]
    fam = [slot_alternates(m, comps) + (w,) for w, m in gw]  # (rep, slots, runs, weight)

    def alts_shown(slot):
        shown = [(x, r) for x, r, _ in slot["alts"]
                 if slot["total"] and r / slot["total"] >= min_alt_frac]
        return shown, len(slot["alts"]) - len(shown)

    ROLE = {0: "Tank", 1: "Healer", 2: "DPS"}

    def stats_html(runs, share, members):
        return (f'<div class="stats"><b>{runs:,}</b> runs'
                f'<span>{members} comps &middot; {share:.1f}%</span></div>')

    def alt_icon(x, width_px):
        xm = spec_lookup.get(x, {})
        return (f'<img src="{icon_data_uri(x, spec_lookup)}" width="{width_px}" '
                f'height="{width_px}" title="{_esc(xm.get("name", x))} '
                f'{_esc(xm.get("class",""))}" '
                f'style="border-radius:4px;border-bottom:2px solid {xm.get("color","#888")}">')

    # ---- The card: top alternate always shown, full list on hover ------------
    b = ['<h2>Archetype card &mdash; top alternate shown, rest on hover</h2>',
         '<p class="hint">Each slot shows its main spec, and directly beneath it the single '
         'most-played alternate. A <b>+N</b> means more alternates exist &mdash; hover the '
         'slot for the full ranked list with usage %.</p>']
    for rank, (rep, slots, runs, w) in enumerate(fam, 1):
        share = w / total_weight * 100
        b.append(f'<div class="arow"><span class="rk">{rank}</span><div class="icons">')
        for slot in slots:
            shown, hidden = alts_shown(slot)
            b.append('<div class="slot">')
            b.append(_icon_img(slot["spec"], spec_lookup, 40, ring=True, title=False))
            b.append('<div class="undr">')
            if shown:
                top_x, top_r = shown[0]
                b.append(alt_icon(top_x, 22))
                more = len(shown) - 1 + hidden
                if more:
                    b.append(f'<span class="plusN">+{more}</span>')
            elif slot["alts"]:  # only sub-threshold alts exist
                b.append(f'<span class="plusN">+{hidden}</span>')
            else:
                b.append('<span class="dash" title="no alternate used">&mdash;</span>')
            b.append('</div>')
            # hover popover: the full ranked list (incl. sub-threshold as +N)
            if slot["alts"]:
                b.append('<div class="pop"><div class="pop-h">alternatives</div>')
                for x, r in shown:
                    xm = spec_lookup.get(x, {})
                    b.append(f'<div class="pr">{_icon_img(x, spec_lookup, 20)}'
                             f'<span style="color:{xm.get("color","#ccc")}">'
                             f'{_esc(xm.get("name", x))}</span>'
                             f'<b>{r/slot["total"]*100:.0f}%</b></div>')
                if hidden:
                    b.append(f'<div class="pr more">+{hidden} rarer (&lt;1%)</div>')
                b.append('</div>')
            b.append('</div>')
        b.append('</div>' + stats_html(runs, share, _fam_members(gw, rank)) + '</div>')
    b2 = []
    a = []

    html = f"""<!doctype html><html><head><meta charset="utf-8">
<title>Archetype card &mdash; visual options</title><style>
:root{{color-scheme:dark light}}
body{{font:14px/1.5 system-ui,sans-serif;margin:0;padding:24px;background:#14161a;color:#e7e9ee}}
h1{{font-size:19px;margin:0 0 2px}} h2{{font-size:15px;margin:28px 0 2px}}
.src{{color:#8a90a0;font-size:12px}} .hint{{color:#8a90a0;font-size:12px;margin:2px 0 10px;max-width:760px}}
.note{{background:#1d2027;border:1px solid #2b2f39;border-radius:8px;padding:8px 12px;
 font-size:12px;color:#c7cbd4;margin:8px 0 4px;max-width:900px}}
.rk{{color:#6b7180;font-weight:700;min-width:16px;text-align:right;padding-top:2px}}
.stats{{margin-left:auto;text-align:right;font-size:12px;white-space:nowrap;align-self:center}}
.stats b{{font-size:14px}} .stats span{{color:#8a90a0;display:block;font-size:11px}}
img{{display:block}}
.arow{{display:flex;align-items:center;gap:14px;background:#1a1d23;border:1px solid #2b2f39;
 border-radius:10px;padding:8px 14px;margin:6px 0}}
.icons{{display:flex;gap:14px;align-items:flex-start}}
.slot{{position:relative;display:flex;flex-direction:column;align-items:center;gap:5px}}
.undr{{display:flex;align-items:center;gap:3px;min-height:22px}}
.plusN{{color:#8a90a0;font-size:10px;font-weight:600}}
.dash{{color:#3f4550;font-size:14px;line-height:22px}}
.pop{{display:none;position:absolute;top:calc(100% + 4px);left:50%;transform:translateX(-50%);
 z-index:20;background:#22262e;border:1px solid #38404c;border-radius:8px;padding:6px;
 min-width:170px;box-shadow:0 6px 20px rgba(0,0,0,.5)}}
.slot:hover .pop{{display:block}}
.pop-h{{color:#8a90a0;font-size:10px;text-transform:uppercase;letter-spacing:.05em;margin:0 4px 4px}}
.pr{{display:flex;align-items:center;gap:6px;font-size:12px;padding:2px 4px;white-space:nowrap}}
.pr b{{margin-left:auto;color:#8a90a0;font-weight:600}}
.pr.more{{color:#6b7180;font-style:italic;font-size:11px}}
</style></head><body>
<h1>Archetype card &mdash; compact visual options</h1>
<div class="src">source: {_esc(source_desc)} &middot; leader radius=1 &middot; top
 {n_families} families</div>
<div class="note">Percentages are the <b>share of this comp's own plays</b> &mdash; e.g. an
 alternate at 14% means 14% of this comp's runs used that spec in that slot. It is
 <b>not</b> a claim about the global meta.</div>
{''.join(b)}{''.join(b2)}{''.join(a)}
</body></html>"""
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write(html)
    return out_path


def _fam_members(gw, rank):
    return len(gw[rank - 1][1])


# --- main --------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    src = ap.add_mutually_exclusive_group()
    src.add_argument("--from-json", metavar="PATH", nargs="?", const=DEFAULT_JSON,
                     help=f"read comps from a comps_index.json (default: {DEFAULT_JSON})")
    src.add_argument("--season", type=int, help="read comps from the DB for this season")
    ap.add_argument("--mode", choices=("leader", "core"), default="leader",
                    help="leader = greedy min-distance clustering (default); "
                         "core = fixed 3-core vs 4-core mining")
    ap.add_argument("--core-min-frac", type=float, default=0.003,
                    help="core mode: min weight fraction for a core (default 0.003)")
    ap.add_argument("--top", type=int, default=10, help="archetypes to print per method")
    ap.add_argument("--html", metavar="OUT", nargs="?",
                    const=os.path.join(REPO, "comp_archetypes_report.html"),
                    help="write a browsable HTML report of 3-core vs 4-core with exact "
                         "member comps (default: comp_archetypes_report.html)")
    ap.add_argument("--hide-below", type=float, default=0.01,
                    help="HTML: hide member comps below this fraction of family weight "
                         "(default 0.01 = 1%%)")
    ap.add_argument("--cards", metavar="OUT", nargs="?",
                    const=os.path.join(REPO, "comp_archetype_cards.html"),
                    help="write the card view (leader radius=1: meta comp + per-slot "
                         "alternates with spec icons)")
    ap.add_argument("--top-families", type=int, default=30,
                    help="cards: how many top families to render (default 30)")
    ap.add_argument("--card-options", metavar="OUT", nargs="?",
                    const=os.path.join(REPO, "comp_card_options.html"),
                    help="write the compact visual-options comparison (A/B/C) for the "
                         "archetype card")
    args = ap.parse_args()

    spec_lookup = load_specs()

    if args.season is not None:
        print(f"Loading comps from DB for season {args.season} ...")
        comps = load_from_db(args.season, spec_lookup)
        source = f"DB season {args.season}"
    else:
        path = args.from_json or DEFAULT_JSON
        if not os.path.exists(path):
            sys.exit(f"input JSON not found: {path}")
        print(f"Loading comps from {path} ...")
        comps = load_from_json(path, spec_lookup)
        source = os.path.relpath(path, REPO)

    print(f"Loaded {len(comps)} distinct comps from {source}.")
    frac = args.core_min_frac

    if args.mode == "core":
        t0 = time.perf_counter()
        g4 = group_core_flex(comps, core_sizes=(4,), min_weight_frac=frac)
        t4 = time.perf_counter() - t0
        t0 = time.perf_counter()
        g3 = group_core_flex(comps, core_sizes=(3,), min_weight_frac=frac)
        t3 = time.perf_counter() - t0
        report(f"4-core / 1-flex  (min_weight_frac={frac})", g4, comps, spec_lookup,
               args.top)
        print(f"\n  [timing] 4-core: {t4 * 1000:.1f} ms")
        report(f"3-core / 2-flex  (min_weight_frac={frac})", g3, comps, spec_lookup,
               args.top)
        print(f"\n  [timing] 3-core: {t3 * 1000:.1f} ms")
        methods = [
            ("4-core / 1-flex", f"4 locked specs + 1 flex slot  (min weight {frac})", g4),
            ("3-core / 2-flex", f"3 locked specs + 2 flex slots (min weight {frac})", g3),
        ]
        tip = ("Compare: 4-core = tighter/more archetypes, 3-core = broader families.")
    else:
        # leader clustering: built-in minimum distance between families
        t0 = time.perf_counter()
        g1 = group_leader(comps, radius=1)
        t1 = time.perf_counter() - t0
        t0 = time.perf_counter()
        g2 = group_leader(comps, radius=2)
        t2 = time.perf_counter() - t0
        report("leader radius=1 (meta comp + 1-swap variants)", g1, comps, spec_lookup,
               args.top, header_mode="rep")
        print(f"\n  [timing] leader r=1: {t1 * 1000:.1f} ms")
        report("leader radius=2 (broader families, 2-swap)", g2, comps, spec_lookup,
               args.top, header_mode="rep")
        print(f"\n  [timing] leader r=2: {t2 * 1000:.1f} ms")
        methods = [
            ("leader radius=1", "meta comp + all comps 1 spec-swap away "
             "(min distance 2 between families)", g1, "rep"),
            ("leader radius=2", "meta comp + all comps up to 2 spec-swaps away "
             "(min distance 3 between families)", g2, "rep"),
        ]
        tip = ("Leader clustering guarantees families are >radius apart, so no two "
               "families describe the same meta region.")

    if args.html:
        out = build_html(methods, comps, spec_lookup, args.html, source,
                         min_member_frac=args.hide_below)
        print(f"\nHTML report written: {out}")

    if args.cards:
        if args.mode != "leader":
            print("note: --cards uses leader radius=1; computing it now.")
        g_cards = g1 if args.mode == "leader" else group_leader(comps, radius=1)
        out = build_cards_html(g_cards, comps, spec_lookup, args.cards, source,
                               top_families=args.top_families,
                               min_alt_frac=args.hide_below)
        print(f"Card view written: {out}")

    if args.card_options:
        g_co = g1 if args.mode == "leader" else group_leader(comps, radius=1)
        out = build_card_options_html(g_co, comps, spec_lookup, args.card_options,
                                      source, min_alt_frac=args.hide_below)
        print(f"Card options written: {out}")

    print("\n" + "=" * 78)
    print(tip + " Open the --html report to inspect exact member comps per family.")


if __name__ == "__main__":
    main()
