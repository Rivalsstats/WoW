import json
import os
import re

import databaseConnector

ROLE_FOLDERS = {
    "0": "Tank",
    "1": "Healer",
    "2": "Dps",
}

# S..F letters, index-aligned with tierMath / snapshotTrends (0 = S = best).
TIER_LETTERS = ["S", "A", "B", "C", "D", "F"]

# Feeds whose entities carry an S..F tier (movement = tier change); everything
# else is ranked (movement = rank-position change). Mirrors snapshotTrends.py.
TIERED_FEEDS = {"spec", "dungeon", "buff", "sim"}

# Of the tiered feeds, these render in "tier" display mode: current tier badge
# only, plus a prev->now change badge when the tier moved. No "% behind #1"
# number, no green/red delta. The sim feed is tiered but stays in "pct" mode
# (it keeps "% behind #1" + the tier-change badge, an explicit earlier ask).
TIER_MODE_FEEDS = {"spec", "dungeon", "buff"}

# How many top movers the bar shows. Kept wide enough that the marquee never
# clones the single .trends-seq within one viewport (which would repeat an item).
TREND_LIMIT = 12
# Ignore sub-threshold popularity wobble so the bar shows real movement.
TREND_MIN_DELTA = 0.05
# Max npc portraits shown for a dungeon "lusted pull" cluster before a "+N" chip.
NPC_CLUSTER_CAP = 4

# Item-subpage per-item ranked feeds. Their long tail includes ~0%-usage entries
# that are noise on the bar, so they need a popularity floor (unless they actually
# moved this week).
ITEM_SUB_FEEDS = {"item_spec", "item_gem", "item_embellishment", "item_missive", "item_variant"}
ITEM_SUB_MIN_POP = 0.5


def slugify(text):
    """Turn an item name into a URL slug (lowercase, hyphen-separated).

    Mirrors the dungeon slug style: apostrophes are dropped (so "Flarendo's"
    -> "flarendos"), every other run of non-alphanumeric characters collapses
    to a single hyphen, and leading/trailing hyphens are trimmed.
    """
    text = (text or "").lower().replace("'", "").replace("’", "")
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")


def build_item_slug_map(item_lookup):
    """Map every item id to a URL slug derived from its name.

    Item names are not guaranteed unique. When a slug is shared by more than one
    item, *all* of the colliding items get ``<slug>-<id>`` so the result is
    unambiguous and independent of iteration order (a stable map for a given set
    of items). Built purely from item names, so any generator that loads the same
    equippable-items lookup produces an identical map regardless of run order.
    """
    base = {}
    counts = {}
    for iid, item in item_lookup.items():
        slug = slugify(item.get("name", "")) or str(iid)
        base[iid] = slug
        counts[slug] = counts.get(slug, 0) + 1
    return {
        iid: (f"{slug}-{iid}" if counts[slug] > 1 else slug)
        for iid, slug in base.items()
    }


# Synthetic Raidbots instance ids shared by every item of their kind rather than
# pointing at a real journal instance.
TIER_SET_INSTANCE_ID = -87  # tier set pieces
PVP_INSTANCE_ID = -85       # PvP / gladiator gear


def build_source_lookups(item_lookup, dungeon_lookup, raids_json):
    """Classify every item's Raidbots ``sources`` into dungeon keys / raid ids
    plus a flat token set, shared by the item pages and the spec pages so the two
    never drift.

    Returns ``(dungeons_map, raids_map, source_dungeons_by_item,
    source_raids_by_item, source_tokens_by_item)``:

    - ``dungeons_map`` / ``raids_map`` flatten the locale ``name`` dicts to
      ``en_US`` and expose ``slug`` (+ dungeon ``short``, raid ``bosses``) for
      resolving display objects.
    - ``source_dungeons_by_item`` maps ``int item_id -> [dungeon_key, ...]``.
    - ``source_raids_by_item`` maps ``int item_id -> {raid_id: [encounter_id_str, ...]}``.
    - ``source_tokens_by_item`` maps ``int item_id -> [token, ...]`` for the
      browse-page filter (``d:<key>`` / ``r:<rid>`` / ``b:<rid>:<enc>`` /
      ``crafted`` / ``tier`` / ``pvp`` / ``other``).

    ``raids_json`` may be an empty dict (raids.json absent early season); raid
    sources simply won't appear.
    """
    dungeons_map = {}
    for did, d in dungeon_lookup.items():
        name = d.get("name")
        if isinstance(name, dict):
            name = name.get("en_US") or next(iter(name.values()), did)
        dungeons_map[str(did)] = {
            "name": name,
            "slug": d.get("slug"),
            # dungeons.json carries the abbreviation under raiderio_short_name.
            "short": d.get("raiderio_short_name") or d.get("short_name") or name,
            "icon": d.get("icon"),
        }

    raids_map = {}
    for rid, r in (raids_json or {}).items():
        name = r.get("name")
        if isinstance(name, dict):
            name = name.get("en_US") or next(iter(name.values()), rid)
        bosses = {}
        for enc_id, b in (r.get("bosses") or {}).items():
            bname = b.get("name")
            if isinstance(bname, dict):
                bname = bname.get("en_US") or next(iter(bname.values()), enc_id)
            # Boss portrait icon filename (data/icons/boss_<enc>.png), written by
            # fetchRaidData; absent for a boss whose creature display id is unknown.
            bosses[str(enc_id)] = {"name": bname, "slug": b.get("slug"), "icon": b.get("icon")}
        raids_map[str(rid)] = {
            "name": name,
            "slug": r.get("slug"),
            "icon": r.get("icon"),
            "order": r.get("order", 0),
            "bosses": bosses,
        }

    # Each Raidbots "sources" entry carries an instanceId (dungeon or raid journal
    # instance) and an encounterId (the boss). Resolve those to our dungeon keys /
    # raid ids and emit a flat set of source tokens per item, which the browse-page
    # filter matches by set intersection. Keyed by int item id to match slug_map.
    instance_to_dungeon = {}
    for did, d in dungeon_lookup.items():
        jii = d.get("journal_instance_id")
        if jii is not None:
            instance_to_dungeon[int(jii)] = str(did)

    source_dungeons_by_item = {}
    source_raids_by_item = {}
    source_tokens_by_item = {}
    for iid, itm in item_lookup.items():
        dids = []
        raids_for_item = {}   # raid id -> set(encounter id)
        tokens = []
        for src in itm.get("sources", []) or []:
            inst = src.get("instanceId")
            did = instance_to_dungeon.get(inst)
            if did:
                if did not in dids:
                    dids.append(did)
                tok = f"d:{did}"
                if tok not in tokens:
                    tokens.append(tok)
                continue
            rid = str(inst) if inst is not None and str(inst) in raids_map else None
            if rid:
                rtok = f"r:{rid}"
                if rtok not in tokens:
                    tokens.append(rtok)
                bucket = raids_for_item.setdefault(rid, set())
                enc = src.get("encounterId")
                if enc is not None and str(enc) in raids_map[rid]["bosses"]:
                    bucket.add(str(enc))
                    btok = f"b:{rid}:{enc}"
                    if btok not in tokens:
                        tokens.append(btok)

        crafted = "profession" in itm
        if crafted:
            tokens.append("crafted")
        is_tier = any(src.get("instanceId") == TIER_SET_INSTANCE_ID
                      for src in (itm.get("sources", []) or []))
        if is_tier:
            tokens.append("tier")
        is_pvp = any(src.get("instanceId") == PVP_INSTANCE_ID
                     for src in (itm.get("sources", []) or []))
        if is_pvp:
            tokens.append("pvp")
        if not dids and not raids_for_item and not crafted and not is_tier and not is_pvp:
            tokens.append("other")

        if dids:
            source_dungeons_by_item[iid] = dids
        if raids_for_item:
            source_raids_by_item[iid] = {r: sorted(encs) for r, encs in raids_for_item.items()}
        source_tokens_by_item[iid] = tokens

    return (dungeons_map, raids_map, source_dungeons_by_item,
            source_raids_by_item, source_tokens_by_item)


def resolve_item_sources(iid, item_lookup, dungeons_map, raids_map,
                         source_dungeons_by_item, source_raids_by_item):
    """Resolve one item's classified sources into the display objects the
    templates consume: ``{source_dungeons, source_raids, crafted}``. ``iid`` must
    be the int item id used by ``build_source_lookups``."""
    source_dungeons = [
        {"id": did, "name": dungeons_map[did]["name"],
         "slug": dungeons_map[did]["slug"], "short": dungeons_map[did]["short"]}
        for did in source_dungeons_by_item.get(iid, [])
        if did in dungeons_map
    ]
    source_raids = []
    for rid, enc_ids in source_raids_by_item.get(iid, {}).items():
        if rid not in raids_map:
            continue
        rinfo = raids_map[rid]
        source_raids.append({
            "id": rid,
            "name": rinfo["name"],
            "slug": rinfo["slug"],
            "bosses": [
                {"id": e, "name": rinfo["bosses"][e]["name"], "slug": rinfo["bosses"][e]["slug"]}
                for e in enc_ids if e in rinfo["bosses"]
            ],
        })
    crafted = "profession" in item_lookup.get(iid, {})
    return {"source_dungeons": source_dungeons, "source_raids": source_raids, "crafted": crafted}


def build_item_source_map(item_lookup, dungeon_lookup, raids_json):
    """Spec-page convenience: ``int item_id -> {source_dungeons, source_raids,
    crafted}`` for every item that has a displayable source (a dungeon, a raid, or
    a profession). Items with no loot source are omitted so the map stays small
    and ``item_sources.get(id)`` is falsy for them."""
    (dungeons_map, raids_map, source_dungeons_by_item,
     source_raids_by_item, _tokens) = build_source_lookups(
        item_lookup, dungeon_lookup, raids_json)
    out = {}
    for iid, itm in item_lookup.items():
        if (iid in source_dungeons_by_item or iid in source_raids_by_item
                or "profession" in itm):
            out[iid] = resolve_item_sources(
                iid, item_lookup, dungeons_map, raids_map,
                source_dungeons_by_item, source_raids_by_item)
    return out


def generateDungeonNav(dungeons):
    dungeon_nav = []
    for d_id, d_data in dungeons.items():
        dungeon_nav.append({
            "name": d_data["name"]["en_US"],
            "url": f"/dungeons/{d_data['slug']}",
            "icon": d_data.get("icon", None),
        })
    dungeon_nav.sort(key=lambda x: x["name"])
    return dungeon_nav

# ---------------------------------------------------------------------------
# "Top Trends" bar
#
# build_trends() reads the latest two weekly snapshots (databaseConnector /
# trend_snapshot) for the feeds a page cares about, diffs them, and returns the
# biggest movers as render-ready dicts. Presentation (icon/href) is resolved
# here from the same lookups the pages already load, so templates/trends_bar.html
# stays dumb. When fewer than two weeks exist (e.g. season start) it returns [],
# and the partial renders nothing.
# ---------------------------------------------------------------------------

_COMBO_LABELS = {
    "set_combo": "Tier-set combo",
    "embellishment_combo": "Embellishment combo",
    "crafted_combo": "Crafted combo",
    "gem_combo": "Gem combo",
}


def trend_feeds_for_index():
    """Global spec + dungeon + group-buff movement (the index bar)."""
    return [("spec", ""), ("dungeon", ""), ("buff", "")]


def trend_feeds_for_spec(spec_id):
    """A single spec's own talents / gear / combos (the spec-page bar)."""
    gk = str(spec_id)
    return [
        (feed, gk)
        for feed in (
            "talent", "item", "embellishment", "gem", "crafted", "missive",
            "set_combo", "embellishment_combo", "crafted_combo", "gem_combo",
        )
    ]


def trend_feeds_for_comps():
    """The comps-page bar: the "best for high keys" archetype families (same data
    as the page's "Best for Highest Keys" card) shown as rank movement, plus the
    Glue Specs / Flexibility Index feed. Two feeds so the bar fills 12 without the
    duplication the small high-key family set alone would produce. The rank-1
    high-key family is "meta" purely by being #1 (no separate flag). Separate from
    the dungeon page's popular ``archetype`` feed so the two can diverge."""
    return [("archetype_hk", "all"), ("flex", "all")]


def trend_feeds_for_dungeon(dungeon_id):
    """A dungeon's popular team-comp archetype, most-lusted pull and best-loot
    movement (the dungeon-page bar)."""
    gk = str(dungeon_id)
    return [("archetype", gk), ("pull", gk), ("loot", gk)]


def trend_feeds_for_items():
    """Global per-slot item share movement (the items list-page bar)."""
    return [("item", "")]


def trend_feeds_for_item(item_id):
    """A single item's used-by-specs / gems / embellishments / missives / ilvl
    variant movement (the item subpage bar). Feeds with no data for the item
    resolve to nothing and self-hide."""
    gk = str(item_id)
    return [
        ("item_spec", gk), ("item_gem", gk), ("item_embellishment", gk),
        ("item_missive", gk), ("item_variant", gk),
    ]


def _maybe_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return value


def _icon_src(icon_file, ext="jpg"):
    """Mirror the templates' /data/icons/<file>.<ext> convention. Spec/buff icons
    are .jpg; item/talent/gem/embellishment icons are .png; dungeon icons already
    carry their extension. Passes through absolute paths / URLs and names that
    already include an extension."""
    if not icon_file:
        return None
    s = str(icon_file)
    if s.startswith(("http://", "https://", "/")):
        return s
    if s.endswith((".png", ".jpg")):
        return "/data/icons/" + s
    return f"/data/icons/{s}.{ext}"


def _name_str(name, fallback):
    if isinstance(name, dict):
        return name.get("en_US") or fallback
    return name or fallback


def _resolve_icon_cluster(csv_ids, lookup, kind="item", limit=6):
    """A comp/combo is a comma-separated list of ids (spec ids for group comps,
    item ids for gear combos). Resolve each to {icon, name} so the bar can render
    the same mini-icon cluster the spec/dungeon pages use. Spec icons are .jpg
    (via SpellIconFileId); item/gem/embellishment icons are .png (via `icon`).

    ``kind="npc"`` parses a dungeon pull signature (``<npc_id>:<count>,...``): each
    entry resolves to the dungeon-page portrait icon ``/data/icons/npc_<id>.png``
    (``lookup`` is the ``en_US`` npc-name map) and carries ``count`` so the bar can
    render a per-mob multiplicity badge when count > 1. A pull can contain many mobs,
    so the npc cluster is capped at ``NPC_CLUSTER_CAP`` portraits and a trailing
    ``{"overflow": N}`` chip carries the count of mobs beyond the cap."""
    if kind == "npc":
        parsed = []
        for raw in (csv_ids or "").split(","):
            key = raw.strip()
            if not key:
                continue
            parts = key.split(":")
            npc_id = parts[0].strip()
            if not npc_id:
                continue
            try:
                count = int(parts[1]) if len(parts) > 1 else 1
            except (TypeError, ValueError):
                count = 1
            name = lookup.get(npc_id) or lookup.get(_maybe_int(npc_id))
            if isinstance(name, dict):
                name = _name_str(name.get("name"), f"NPC {npc_id}")
            parsed.append({"icon": f"/data/icons/npc_{npc_id}.png",
                           "name": name or f"NPC {npc_id}", "count": count})
        out = parsed[:NPC_CLUSTER_CAP]
        if len(parsed) > NPC_CLUSTER_CAP:
            out.append({"overflow": len(parsed) - NPC_CLUSTER_CAP})
        return out
    out = []
    for raw in (csv_ids or "").split(","):
        key = raw.strip()
        if not key:
            continue
        meta = lookup.get(key) or lookup.get(_maybe_int(key)) or {}
        if kind == "spec":
            icon = _icon_src(meta.get("SpellIconFileId"))
            name = _name_str(meta.get("name"), f"Spec {key}")
        else:
            icon = _icon_src(meta.get("icon"), ext="png")
            name = _name_str(meta.get("name"), f"Item {key}")
        out.append({"icon": icon, "name": name})
        if len(out) >= limit:
            break
    return out


def _resolve_entry(feed, entity_key, label, lookups):
    """Turn a raw snapshot entity into {label, icon, href, css, comp_specs}."""
    specs = lookups.get("specs", {})
    classes = lookups.get("classes", {})
    dungeons = lookups.get("dungeons", {})
    buffs = lookups.get("buffs", {})
    items = lookups.get("items", {})
    talents = lookups.get("talents", {})
    npcs = lookups.get("npcs", {})
    role_lookup = lookups.get("role_lookup", ROLE_FOLDERS)

    out = {"label": label or str(entity_key), "icon": None, "href": None,
           "css": None, "icons": None}

    # sim (sim tierlist), item_spec (item subpage "used by specs") and flex (comps
    # page Flexibility Index) all key on a spec id and render like the spec feed.
    if feed in ("spec", "sim", "item_spec", "flex"):
        meta = specs.get(str(entity_key)) or {}
        name = _name_str(meta.get("name"), str(entity_key))
        cls = classes.get(str(meta.get("classID"))) or {}
        role = role_lookup.get(str(meta.get("role"))) or role_lookup.get(meta.get("role"))
        out["label"] = name
        out["icon"] = _icon_src(meta.get("SpellIconFileId"))
        if name and cls.get("name") and role:
            out["href"] = f"/classes/{role}/{name}_{cls['name']}"
        out["css"] = (cls.get("name") or "").replace(" ", "") or None
    elif feed == "dungeon":
        meta = dungeons.get(str(entity_key)) or {}
        out["label"] = _name_str(meta.get("name"), str(entity_key))
        out["icon"] = _icon_src(meta.get("icon"))
        if meta.get("slug"):
            out["href"] = f"/dungeons/{meta['slug']}"
    elif feed == "buff":
        meta = buffs.get(entity_key) or buffs.get(_maybe_int(entity_key)) or {}
        out["label"] = meta.get("name") or meta.get("display_name") or out["label"]
        out["icon"] = _icon_src(meta.get("icon") or meta.get("icon_file"))
    elif feed in ("archetype", "archetype_hk"):
        # team-comp archetype: label carries the displayed core comp (a list of spec
        # ids) -> cluster of spec icons, same rendering as a raw group comp.
        # archetype_hk (comps page) is the "best for high keys" family, a plain ranked
        # feed: the rank-1 family is "meta" purely by sitting at #1, shown via its rank.
        out["icons"] = _resolve_icon_cluster(label, specs, kind="spec")
        out["label"] = "Archetype"
    elif feed == "pull":
        # dungeon most-lusted pull: label is the pull signature "<npc>:<count>,..."
        # -> capped cluster of npc portrait icons, each with its per-mob multiplicity.
        # cluster_kind tags the portraits so the template renders them uncropped
        # (rectangular webthumbs) rather than circle-cropped like spec/item icons.
        out["icons"] = _resolve_icon_cluster(label, npcs, kind="npc")
        out["cluster_kind"] = "npc"
        out["label"] = "Lusted pull"
    elif feed == "comp":
        # group comp: a list of spec ids -> cluster of spec icons
        out["icons"] = _resolve_icon_cluster(label, specs, kind="spec")
        out["label"] = _COMBO_LABELS.get(feed, "Comp")
    elif feed in ("set_combo", "embellishment_combo", "crafted_combo", "gem_combo"):
        # gear combo: a list of item ids -> cluster of item icons, exactly like
        # build_comps renders on the spec page.
        out["icons"] = _resolve_icon_cluster(label, items, kind="item")
        out["label"] = _COMBO_LABELS.get(feed, "Build")
    elif feed in ("item", "embellishment", "gem", "crafted", "missive", "loot",
                  "item_gem", "item_embellishment", "item_missive"):
        # any single-item feed: entity_key is the bare id (the global item feed
        # keys it as "<slot>:<item_id>", so take the last segment). Gems come from
        # the enchant catalog and carry itemName/itemIcon instead of name/icon.
        item_id = str(entity_key).split(":")[-1]
        meta = items.get(item_id) or items.get(_maybe_int(item_id)) or {}
        name = _name_str(meta.get("name") or meta.get("itemName"), f"Item {item_id}")
        out["label"] = name
        out["icon"] = _icon_src(meta.get("icon") or meta.get("itemIcon"), ext="png")
        if meta.get("name") or meta.get("itemName"):
            out["href"] = f"/items/{slugify(name)}"
        if str(item_id).isdigit():
            out["wowhead"] = f"item={item_id}"
    elif feed == "item_variant":
        # ilvl variant of one item: entity_key is "<item_id>:<ilvl>". The item page
        # renders these as an ilvl text badge (not an icon), so the bar mirrors that:
        # no icon, the ilvl carried as variant_label; keep the item link + tooltip.
        item_id = str(entity_key).split(":")[0]
        meta = items.get(item_id) or items.get(_maybe_int(item_id)) or {}
        item_name = _name_str(meta.get("name") or meta.get("itemName"), f"Item {item_id}")
        out["label"] = label or item_name
        out["variant_label"] = label or "Variant"
        if meta.get("name") or meta.get("itemName"):
            out["href"] = f"/items/{slugify(item_name)}"
        if str(item_id).isdigit():
            out["wowhead"] = f"item={item_id}"
    elif feed == "talent":
        tid = str(entity_key).split(":")[-1]
        meta = talents.get(tid) or talents.get(_maybe_int(tid))
        if isinstance(meta, dict):
            out["label"] = _name_str(meta.get("name"), f"Talent {tid}")
            out["icon"] = _icon_src(meta.get("icon"), ext="png")
            spell_id = meta.get("spellId")
            if spell_id:
                out["wowhead"] = f"spell={spell_id}"
                # Talents have no internal page, so without an href the entry renders
                # as a <span>, and Wowhead power.js only tooltips ANCHOR elements. Link
                # to the Wowhead spell page (same as the talent-tree node) so the entry
                # is an <a> and the tooltip activates. External => template opens it in
                # a new tab.
                out["href"] = f"https://www.wowhead.com/spell={spell_id}"
        else:
            out["label"] = meta or f"Talent {tid}"
    return out


_LIVE_INDEX_CACHE = (None, None)


def _live_index(live):
    """(feed, group_key) -> {entity_key: record} for a live snapshot, memoized on
    the live object's identity (one entry, since a build reuses one live object)."""
    global _LIVE_INDEX_CACHE
    if _LIVE_INDEX_CACHE[0] is live:
        return _LIVE_INDEX_CACHE[1]
    index = {}
    for r in live.get("records", []):
        index.setdefault((r["feed"], r["group_key"]), {})[r["entity_key"]] = r
    _LIVE_INDEX_CACHE = (live, index)
    return index


def build_trends(conn, cursor, feeds, lookups, limit=TREND_LIMIT, live_records=None):
    """Diff the CURRENT live values against last week's stored snapshot and return
    up to ``limit`` render-ready movers, biggest movement first.

    The "now" side is the live values snapshotTrends computed for this build and
    wrote to TRENDS_LIVE_PATH — never persisted to the DB, so the displayed number
    stays fresh through the week. The "prev" side is the most recent stored weekly
    baseline strictly older than the live week (i.e. the previous reset week).
    Movement = tier change (tiered feeds) or rank change (ranked feeds) + the
    popularity delta. Returns [] until a previous week's snapshot exists.

    ``live_records`` lets a producer that owns a feed the snapshot step can't see
    (the sim tierlist: its DPS results aren't present when snapshotTrends runs)
    pass its own ``{"week_id", "records"}`` "now" side instead of the build-local
    JSON. When omitted, the JSON is used as before."""
    live = live_records if live_records is not None else _load_live_trends()
    if not live:
        return []
    # Own plain (tuple) cursor: the fetchers index rows positionally, but callers
    # may hand us a dictionary=True cursor (e.g. the dungeon generator).
    cursor = conn.cursor()
    prev_week = databaseConnector.fetch_prev_trend_week(conn, cursor, live["week_id"])
    if prev_week is None:
        return []

    # Index the live "now" records by (feed, group_key) -> {entity_key: record}.
    # Cached on the live object's identity so the thousands of per-item build_trends
    # calls on the items build reuse one index instead of rebuilding it each page.
    live_by_group = _live_index(live)

    movers = []
    for feed, group_key in feeds:
        gk = str(group_key)
        now = live_by_group.get((feed, gk), {})
        prev = {
            r["entity_key"]: r
            for r in databaseConnector.fetch_trend_snapshots(conn, cursor, feed, gk, [prev_week])
        }

        tiered = feed in TIERED_FEEDS
        tier_mode = feed in TIER_MODE_FEEDS
        # The comps bar META badge marks the single current #1 high-key family. Pick
        # exactly one entity (the lowest live rank_pos) so ties / a jittered debug
        # live snapshot can't crown several. Production ranks are unique so this is
        # just the rank_pos == 1 family.
        meta_ek = None
        if feed == "archetype_hk" and now:
            meta_ek = min(now, key=lambda k: now[k].get("rank_pos") or 1_000_000)
        for ek, n in now.items():
            p = prev.get(ek)

            if tiered:
                # popularity is "% behind #1" (0 = leader, larger = further behind),
                # so an improvement is the gap shrinking: delta = prev_behind -
                # now_behind. Positive => closed the gap => up/green (toward S / #1).
                delta = (p["popularity"] - n["popularity"]) if p else 0.0
                t_now, t_prev = n["tier"], (p["tier"] if p else None)
                changed = t_prev is not None and t_now != t_prev
                tier_now = TIER_LETTERS[t_now] if t_now is not None and 0 <= t_now < len(TIER_LETTERS) else None
                tier_prev = TIER_LETTERS[t_prev] if t_prev is not None and 0 <= t_prev < len(TIER_LETTERS) else None
                moved = 0 if t_prev is None else (t_prev - t_now)  # +ve = climbed toward S
            else:
                # ranked (share-%) feeds keep their existing semantics: up = larger.
                delta = n["popularity"] - (p["popularity"] if p else 0.0)
                r_now, r_prev = n["rank_pos"], (p["rank_pos"] if p else None)
                changed = r_prev is not None and r_now != r_prev
                tier_now = tier_prev = None
                moved = 0 if r_prev is None else (r_prev - r_now)  # +ve = climbed

            # The current #1 high-key family is the META comp (comps bar); always let
            # it in so the badge shows even when its rank didn't move this week.
            is_meta_entry = meta_ek is not None and ek == meta_ek

            # Item-subpage feeds carry a long tail of ~0% variants/specs; a near-zero
            # entry with no real change is noise, so require a popularity floor OR a
            # nonzero rounded change.
            if (feed in ITEM_SUB_FEEDS and n["popularity"] < ITEM_SUB_MIN_POP
                    and round(delta, 2) == 0):
                continue

            # Tier-mode feeds (spec/dungeon/buff) always render (current tier badge),
            # so they are never gated on a numeric delta; selection prefers tier
            # changers, then fills with the current top entities by score. Every other
            # feed still needs real movement to earn a slot (the META comp is exempt).
            if (not tier_mode and not is_meta_entry
                    and abs(delta) < TREND_MIN_DELTA and not changed):
                continue

            entry = _resolve_entry(feed, ek, n["label"], lookups)
            # The bar is icon-only — never show a raw name/id. Drop anything we can't
            # render as at least one real icon (single or cluster) or an ilvl variant
            # badge (item_variant renders a text badge instead of an icon).
            cluster = entry.get("icons") or []
            if (not entry.get("icon") and not entry.get("variant_label")
                    and not any(c.get("icon") for c in cluster)):
                continue

            if tier_mode:
                # rank tier changers first, then by current score (higher = closer to
                # #1). popularity is "% behind #1", so score_norm = 1 - behind%/100.
                mag = (2.0 if changed else 0.0) + max(0.0, 1.0 - n["popularity"] / 100.0)
            else:
                mag = abs(delta) + (2.0 if (changed and tiered) else 0.0)
            # Guarantee the META comp leads its feed so the badge is always visible.
            if is_meta_entry:
                mag += 100.0

            entry.update({
                "feed": feed,
                # tier: current tier badge only (spec/dungeon/buff). icon: the icon
                # cluster is the whole story, no number (dungeon lusted pull, whose
                # lust % reads oddly next to the mob portraits). pct: "% behind #1"
                # / share %, arrow and delta (everything else, incl. sim).
                "mode": "tier" if tier_mode else ("icon" if feed == "pull" else "pct"),
                "value": round(n["popularity"], 2),
                "delta": round(delta, 2),
                "direction": "up" if delta > TREND_MIN_DELTA else ("down" if delta < -TREND_MIN_DELTA else "flat"),
                "moved": moved,
                "is_new": p is None,
                "tier_now": tier_now,
                "tier_prev": tier_prev,
                "tier_changed": changed if tiered else False,
                "rank_changed": changed if not tiered else False,
                # comps bar META badge: the current rank-1 high-key family.
                "is_meta": is_meta_entry,
                "_mag": mag,
            })
            movers.append(entry)

    # Balanced selection: round-robin across feeds so no single feed monopolises the
    # bar (the spec page carries ~10 feeds and talents used to crowd out missives /
    # crafted). Each feed contributes its biggest movers first; feeds lead in order of
    # their strongest mover. Single-feed pages collapse to a plain magnitude sort.
    by_feed = {}
    for e in movers:
        by_feed.setdefault(e["feed"], []).append(e)
    for lst in by_feed.values():
        lst.sort(key=lambda e: e["_mag"], reverse=True)
    feed_order = sorted(by_feed, key=lambda f: by_feed[f][0]["_mag"], reverse=True)
    top = []
    while len(top) < limit and any(by_feed[f] for f in feed_order):
        for f in feed_order:
            if by_feed[f]:
                top.append(by_feed[f].pop(0))
                if len(top) >= limit:
                    break
    for e in top:
        e.pop("_mag", None)
    return top


_LOOKUP_DIR = os.environ.get("MYTHI_LOOKUP_DIR", "data/static")
_GLOBAL_TRENDS_CACHE = None

# Build-local "current live" trends, written by snapshotTrends every build and
# read here as the fresh "now" side of the diff. Deliberately NOT in the DB — the
# DB only holds the weekly baselines. Same working dir for the snapshot step and
# the generators, so a relative path is fine.
TRENDS_LIVE_PATH = os.environ.get("TRENDS_LIVE_PATH", os.path.join("assets", "json", "trends_live.json"))
_LIVE_TRENDS_CACHE = _GLOBAL_TRENDS_UNSET = object()


def _load_live_trends():
    """Load {week_id, records} written by snapshotTrends this build. Cached; None
    (bar hides) when the file is absent — e.g. the snapshot step didn't run."""
    global _LIVE_TRENDS_CACHE
    if _LIVE_TRENDS_CACHE is not _GLOBAL_TRENDS_UNSET:
        return _LIVE_TRENDS_CACHE
    try:
        with open(TRENDS_LIVE_PATH, encoding="utf-8") as fh:
            _LIVE_TRENDS_CACHE = json.load(fh)
    except (OSError, ValueError) as exc:
        print(f"[trends] no live snapshot at {TRENDS_LIVE_PATH}, hiding bar: {exc}")
        _LIVE_TRENDS_CACHE = None
    return _LIVE_TRENDS_CACHE


def _load_lookup(name):
    with open(os.path.join(_LOOKUP_DIR, name), encoding="utf-8") as fh:
        return json.load(fh)


def build_global_trends():
    """Global spec + dungeon trends for the shared site-wide bar — the feed used
    by pages that don't have their own contextual trends (dashboard, routes,
    comps, items, tierlist, ...). Self-contained: it loads the lookups it needs
    and opens its own pooled connection, so a generator only has to pass
    ``trends=build_global_trends()`` to its template. Cached per process (each
    generator builds many pages from one snapshot). Returns [] — so the bar just
    hides — if the DB/pool isn't available in this particular generator."""
    global _GLOBAL_TRENDS_CACHE
    if _GLOBAL_TRENDS_CACHE is not None:
        return _GLOBAL_TRENDS_CACHE
    try:
        group_buffs = _load_lookup("groupbuffs.json")
        lookups = {
            "specs": _load_lookup("specs.json"),
            "classes": _load_lookup("classes.json"),
            "dungeons": _load_lookup("dungeons.json"),
            "buffs": {b.get("id"): b for b in group_buffs},
        }
        conn = databaseConnector.get_connection()
        try:
            cursor = conn.cursor()
            databaseConnector.configure_read_session(conn, cursor)
            result = build_trends(conn, cursor, trend_feeds_for_index(), lookups)
        finally:
            conn.close()
    except Exception as exc:  # pool not initialised / DB unreachable in this step
        print(f"[trends] global trends unavailable, hiding bar: {exc}")
        result = []
    _GLOBAL_TRENDS_CACHE = result
    return result


def generateSpecNav(spec_lookup, class_lookup):
    # Build a dict mapping role names to lists of specs
    spec_nav = {role_name: [] for role_name in ROLE_FOLDERS.values()}

    for sid, sdata in spec_lookup.items():
        role_key = str(sdata.get("role", 2))
        role_name = ROLE_FOLDERS.get(role_key, "Other")
        class_data = class_lookup.get(str(sdata.get("classID", "")), {})
        filename = f"{sdata['name']}_{class_data.get('name')}"
        spec_nav[role_name].append(
            {
                "name": f"{sdata['name']} {class_data.get('name')}",
                "url": f"/classes/{role_name}/{filename}",
                "icon": sdata.get("SpellIconFileId"),
                "class": class_data.get("name", "Unknown").replace(" ", ""),
            }
        )

    # Optionally sort each list by name:
    for lst in spec_nav.values():
        lst.sort(key=lambda x: x["name"])

    return spec_nav
