"""Shared, dependency-light helpers used across page generators, the
image_generation package and the social_posts package.

Only stdlib + databaseConnector may be imported here; keeping this module
free of jinja2/matplotlib/PIL/openai is what breaks the old circular-import
chains (generateSocialsPost <-> generateSpecPages/generateDashboardPage).
"""

import hashlib
import json
import os

import databaseConnector

LOOKUP_DIR = "data/static"  # Default lookup directory, can be overridden by command line argument


def talent_set_hash(class_rows, spec_rows, hero_rows):
    """Content hash identifying a member's full talent selection across all trees.

    ``class_rows`` / ``spec_rows`` / ``hero_rows`` are iterables of
    ``(talent_id, rank)`` pairs (trees 0=class, 1=spec, 2=hero). Returns the
    16-byte ``BINARY(16)`` digest stored in ``members.talent_set_id`` and keying
    ``talent_sets``, or ``None`` when the member has no talent rows at all.

    Canonical string: each row rendered ``tree:talent_id:rank``, sorted by
    ``(tree, talent_id)`` ascending, joined by ``,``. This MUST stay
    byte-identical to the SQL backfill's
    ``MD5(GROUP_CONCAT(CONCAT_WS(':', tree, talent_id, rank) ORDER BY tree, talent_id SEPARATOR ','))``
    (run with ``SESSION group_concat_max_len = 1000000``) so live collector
    inserts land on the same ``set_id`` the one-time migration produced.
    """
    rows = []
    for tree, tree_rows in ((0, class_rows), (1, spec_rows), (2, hero_rows)):
        for talent_id, rank in tree_rows:
            rows.append((tree, int(talent_id), int(rank)))
    if not rows:
        return None
    rows.sort(key=lambda r: (r[0], r[1]))
    canonical = ",".join(f"{tree}:{tid}:{rank}" for tree, tid, rank in rows)
    return hashlib.md5(canonical.encode("utf-8")).digest()


def bonus_set_hash(bonus_ids):
    """Content hash identifying an equipment item's set of bonus ids.

    ``bonus_ids`` is an iterable of integer bonus ids for one equipped item.
    Returns the 16-byte ``BINARY(16)`` digest stored in
    ``equipment.bonus_set_id`` and keying ``bonus_sets``, or ``None`` when the
    item carries no bonus ids.

    Canonical string: distinct bonus ids ascending, joined by ``,``. This MUST
    stay byte-identical to the SQL backfill's
    ``MD5(GROUP_CONCAT(bonus_id ORDER BY bonus_id SEPARATOR ','))``
    (run with ``SESSION group_concat_max_len = 1000000``) so live collector
    inserts land on the same ``set_id`` the one-time migration produced.
    """
    ids = sorted({int(b) for b in bonus_ids}) if bonus_ids else []
    if not ids:
        return None
    canonical = ",".join(str(b) for b in ids)
    return hashlib.md5(canonical.encode("utf-8")).digest()

SECONDARY_STATS = ["haste", "versatility", "mastery", "crit"]
TERTIARY_STATS = [
    "avoidance",
    "lifesteal",
    "speed",
]
HEALTH_STATS = ["health", "stamina"]

# Friendly labels for the composite/adaptive stat tokens shown on stat badges
# across the site. The adaptive "mainstat" token (stragiint) and the
# multi-primary combos don't title-case cleanly, so they get explicit names;
# plain secondary stats (crit/haste/...) fall through to a title-cased token.
# Single source shared by the spec page (exposed as a Jinja global) and the
# item preview cards, so the mapping is maintained in exactly one place.
STAT_DISPLAY_NAMES = {
    "stragiint": "Mainstat",
    "stragi": "Str/Agi",
    "agiint": "Agi/Int",
    "strint": "Str/Int",
}


def stat_display_name(stat_type):
    """Display label for a stat token, e.g. 'stragiint' -> 'Mainstat',
    'crit' -> 'Crit'. This is the one conversion used everywhere stat badges are
    rendered (spec page + item preview cards)."""
    if not stat_type:
        return ""
    return STAT_DISPLAY_NAMES.get(stat_type, str(stat_type).title())


def resolve_bonus_quality(bonus_ids, bonus_quality_lookup):
    """Item quality (rarity) implied by a variant's bonus ids, or ``None``.

    A specific item variant can carry a quality-setting bonus id (see
    ``processBonusIds.build_bonus_quality_map`` / data/static/bonus_quality_map.json)
    that overrides the base item's canonical quality. This is the single place both
    the spec page (equipped item colour) and the item page (most-used variant
    colour) resolve that override from, so the rarity CSS class matches the actual
    variant rather than the base item.

    ``bonus_ids`` may be a list of ids or a delimited string (comma- or
    colon-separated, matching the two shapes the generators carry). When more than
    one bonus id sets a quality the last one wins, mirroring the spec page's loop.
    """
    if not bonus_ids or not bonus_quality_lookup:
        return None
    if isinstance(bonus_ids, str):
        bonus_ids = bonus_ids.replace(":", ",").split(",")
    quality = None
    for bid in bonus_ids:
        bid = str(bid).strip()
        if bid and bonus_quality_lookup.get(bid) is not None:
            quality = bonus_quality_lookup[bid]
    return quality


def count_item_sockets(bonus_ids, bonus_socket_lookup, socket_info=None):
    """True number of gem sockets an equipped item variant has.

    Sockets come from two independent, additive contributions that never
    overlap, so they add (never ``max()``):

    * inherent sockets — the base item's own sockets, the length of
      ``socket_info["sockets"]`` (the item's ``socketInfo`` block). This is
      context-independent base data and never carries bonus-granted sockets.
    * bonus sockets — sockets granted by the variant's bonus ids, summed from
      ``bonus_socket_lookup``.

    Taking the max of the two undercounts any item carrying both an inherent and
    a bonus socket, slotting one gem too few. Item 268265 (Aqirbane Reliquary) is
    the witness: 1 inherent PRISMATIC socket + 1 from bonus 13668 = 2, exactly
    what Wowhead renders.

    This is the single place the spec page (convert_slots), the simc profile
    builder and the simc BiS gatherer resolve a socket count, so the three cannot
    drift apart again.

    ``bonus_ids`` may be a list of ids or a comma-separated string; blanks are
    ignored. ``bonus_socket_lookup`` maps a bonus id (str) either to its socket
    count directly (int) or to the bonus record dict carrying a ``"socket"`` key,
    so both the spec page's ``bonus_lookup`` and the simc paths'
    ``load_bonus_socket_counts`` fit without reshaping. ``socket_info`` is the
    item's ``socketInfo`` dict (or ``None``); missing/None and non-int socket
    values contribute nothing.
    """
    if isinstance(bonus_ids, str):
        bonus_ids = bonus_ids.split(",")
    lookup = bonus_socket_lookup or {}
    total = 0
    for bid in bonus_ids or []:
        bid = str(bid).strip()
        if not bid:
            continue
        val = lookup.get(bid)
        if isinstance(val, dict):
            val = val.get("socket")
        try:
            total += int(val)
        except (TypeError, ValueError):
            continue
    sockets = (socket_info or {}).get("sockets") or []
    try:
        total += len(sockets)
    except TypeError:
        pass
    return total


# --------------------------------------------------------------------------
# Talent-tree filtering (shared by the spec page tree and the baked analyzer /
# tierlist-modal trees so all three hide the same non-existent nodes)
# --------------------------------------------------------------------------

def _talent_entry_has_identity(entry):
    """True when a talent entry carries a real identity (id / definitionId /
    spellId). The vendored raidbots talents.json pads some single nodes with a
    bare ``{}`` entry; those have none of these. Left in place they inflate a
    node's entry count so a single node is misdetected as a choice node."""
    return bool(entry.get("id") or entry.get("definitionId") or entry.get("spellId"))


def node_has_valid_spellid(node):
    """A talent node is renderable only if at least one of its entries carries a
    nonzero spellId. Nodes that fail this (data padding, removed talents) would
    draw as a stray questionmark, so the spec page tree and the baked analyzer /
    tierlist-modal trees all drop them."""
    for e in node.get("entries", []):
        if e.get("spellId", 0):
            return True
    return False


def strip_empty_talent_entries(talents_tree_data):
    """Drop empty/identity-less entry objects from every talent node.

    The vendored raidbots talents.json pads some single nodes with a stray
    ``{}`` entry (node name ends in " / "). Left in place, ``len(entries) > 1``
    makes ``build_ui_tree`` misdetect the node as a choice node. Strip them so
    the node keeps its true type. Warns per dropped entry so upstream data
    changes stay visible (fail-loudly). Operates on the raidbots list shape
    (one entry per spec, each with ``classNodes`` / ``specNodes`` / ``heroNodes``)
    and mutates it in place.
    """
    NODE_KEYS = ("classNodes", "specNodes", "heroNodes")
    dropped = 0
    for spec in talents_tree_data:
        spec_id = spec.get("specId")
        for key in NODE_KEYS:
            for node in spec.get(key, []):
                entries = node.get("entries")
                if not entries:
                    continue
                # keep only entries carrying an identity; drops `{}`
                clean = [e for e in entries if _talent_entry_has_identity(e)]
                if len(clean) != len(entries):
                    n_dropped = len(entries) - len(clean)
                    dropped += n_dropped
                    print(f"[talents] WARN spec {spec_id} node {node.get('id')} "
                          f"'{node.get('name','')}' dropped {n_dropped} empty "
                          f"entry object(s)")
                    node["entries"] = clean
    if dropped:
        print(f"[talents] WARN stripped {dropped} empty entry object(s) total "
              f"from talents.json")
    return talents_tree_data


def filter_talent_tree_nodes(nodes):
    """Apply the spec page's talent filtering to a baked per-spec node dict
    (``{node_id: {entries, ...}}`` from processTalents), returning a NEW dict.

    Mirrors generateSpecPages: strip padding ``{}`` entries so a single node
    isn't misdetected as a choice node, then drop nodes with no valid spellId in
    any entry (they render as a stray questionmark). Callers MUST keep
    ``fullNodeOrder`` COMPLETE so the Blizzard loadout bitstream decode stays
    aligned; a node absent from this dict but present in ``fullNodeOrder`` is
    simply not drawn (analyzer.js / tierlist-modal.js look each node up and skip
    a null)."""
    out = {}
    for nid, node in (nodes or {}).items():
        entries = node.get("entries")
        if entries:
            clean = [e for e in entries if _talent_entry_has_identity(e)]
            if len(clean) != len(entries):
                node = dict(node)
                node["entries"] = clean
        if node_has_valid_spellid(node):
            out[nid] = node
    return out


# --------------------------------------------------------------------------
# Blizzard "serialization version 2" loadout encoder (test-only: the local
# seeder's synthetic talent strings)
# --------------------------------------------------------------------------
#
# Production no longer synthesizes talent codes: the CI tierlist top50 set now
# uses the REAL Blizzard export string the players used in game
# (top_player_loadouts.loadout_text, see generateSimcProfiles._top50_talents), and
# popular/simcbis use the real most-popular stored code. So this encoder's ONLY
# remaining caller is the local seeder (localDev/loadout_codec.py re-exports it),
# which must synthesize decodable v2 strings for members.loadout and the seeded
# top_player_loadouts.loadout_text when the throwaway DB has no real ones. Keep it:
# it gives the local top50 modal + simc validation a real "use the stored string"
# path. Because it is seeder-only, the choice-node/data-skew caveats below are a
# local-fidelity concern, never a production one.
#
# The live members.loadout / top_player talent selections are consumed two ways:
# the client analyzer (assets/js/analyzer.js decodeLoadout) decodes the string to
# draw a build, and SimulationCraft parses it off the ``talents=`` actor line.
# Both read Blizzard's v2 bitstream, so the encoder here must produce a string
# that BOTH accept. simc is the strict validator: it rejects a choice-index on a
# non-choice node and an out-of-spec node, and it parses (and ignores) the 128-bit
# tree hash, so a zero hash is accepted. That last fact is what lets us synthesize
# a code from per-node data without recomputing Blizzard's checksum.
#
# The bitstream (mirroring decodeLoadout): a 6-bit value per output char, packed
# LSB-first over the base64 alphabet ``A-Za-z0-9+/`` (real base64, so ``+`` / ``/``
# can appear; ``-`` and ``=`` never do); header = 8-bit version (2), 16-bit spec id,
# 128 bits of tree hash (emitted as zeros); then, for every node id in
# ``fullNodeOrder``: a selected bit, when set a purchased bit, then a partial-rank
# flag (1 => a 6-bit rank follows, for a multi-rank node bought below max) and a
# choice flag (1 => a 2-bit entry index follows, ONLY for a genuine choice node).

TALENT_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
_TALENT_CHAR_IDX = {c: i for i, c in enumerate(TALENT_CHARS)}
LOADOUT_VERSION = 2


class _LoadoutBitWriter:
    def __init__(self):
        self.bits = []

    def write(self, value, nbits):
        """Append ``nbits`` of ``value``, least-significant bit first."""
        for i in range(nbits):
            self.bits.append((int(value) >> i) & 1)

    def encode(self):
        # Pad the tail up to a whole char (6 bits); the client and simc tolerate a
        # zero-padded tail, so zeros are safe filler.
        bits = self.bits
        while len(bits) % 6 != 0:
            bits.append(0)
        out = []
        for i in range(0, len(bits), 6):
            v = 0
            for b in range(6):
                v |= bits[i + b] << b
            out.append(TALENT_CHARS[v])
        return "".join(out)


def is_choice_node(node):
    """True when simc treats a node as a choice node (one that carries a 2-bit
    entry index in the bitstream). A ``tiered`` node has multiple entries that are
    rank tiers of the SAME talent, not alternatives, so it is NOT a choice node —
    emitting a choice index on it makes simc fail init ("not a choice node but has
    index selection"). Mirrors analyzer.js ``ntypeOf`` (tiered => passive).

    Only entries carrying a real identity count toward the entry total: the
    vendored raidbots talents.json pads some single nodes with an identity-less
    ``{spellId: 0}`` entry (name ends in " / "), and counting it would misdetect a
    single node (e.g. WW Monk hero node 101235 "Inner Compass") as a choice node,
    emitting a bogus 2-bit index that fails simc init. The baked analyzer /
    tierlist trees already strip these entries, so this keeps the encoder aligned
    with both decoders and simc, which all see the node as single."""
    if not node:
        return False
    if node.get("type") == "tiered":
        return False
    real = [e for e in (node.get("entries") or []) if _talent_entry_has_identity(e)]
    return len(real) > 1


def encode_loadout(spec_id, selected, full_node_order, nodes, ranks=None):
    """Encode a Blizzard v2 loadout string.

    ``selected``      -- {node_id: entry_index} for every purchased node. Free /
                         granted nodes are forced selected regardless (they are
                         part of every build), so callers need not list them.
    ``full_node_order`` -- the spec's flat decode order (INCLUDES ids absent from
                         ``nodes``; those consume a not-selected bit each so the
                         stream stays aligned with the client decoder and simc).
    ``nodes``         -- {str(node_id): {entries, type, maxRanks, free, ...}}.
    ``ranks``         -- optional {node_id: rank}. When a node's rank is below its
                         ``maxRanks`` the partial-rank flag + 6-bit rank are emitted
                         (a full or missing rank emits neither, i.e. full rank).
    """
    sel = {int(k): int(v or 0) for k, v in (selected or {}).items()}
    rank_map = {int(k): v for k, v in (ranks or {}).items()}

    def node_for(nid):
        return nodes.get(str(nid)) or nodes.get(nid)

    w = _LoadoutBitWriter()
    w.write(LOADOUT_VERSION, 8)
    w.write(int(spec_id), 16)
    for _ in range(16):
        w.write(0, 8)  # 128-bit tree hash, parsed-and-ignored by client + simc

    for nid in full_node_order:
        node = node_for(nid)
        is_free = bool(node and node.get("free"))
        is_sel = is_free or int(nid) in sel
        if not is_sel:
            w.write(0, 1)
            continue
        w.write(1, 1)  # selected
        w.write(1, 1)  # purchased
        max_ranks = int((node or {}).get("maxRanks") or 1)
        rank = rank_map.get(int(nid))
        if rank is not None and max_ranks > 1 and 0 < int(rank) < max_ranks:
            w.write(1, 1)          # partial-rank flag
            w.write(int(rank), 6)  # actual rank (< maxRanks)
        else:
            w.write(0, 1)          # full rank, no rank bits follow
        if is_choice_node(node):
            w.write(1, 1)                      # choice flag
            w.write(sel.get(int(nid), 0), 2)   # entry index (0..3)
        else:
            w.write(0, 1)                      # not a choice node
    return w.encode()


def decode_loadout(code, full_node_order, nodes):
    """Round-trip inverse of :func:`encode_loadout`, mirroring analyzer.js.

    Returns {node_id: {"entry_index": int, "rank": int|None, "purchased": bool}}
    for the selected nodes, or ``None`` when the string is malformed. Present so
    callers (and the seeder self-test) can prove encode/decode agree with the
    client contract without a browser."""
    if not code:
        return None
    bits = []
    for ch in code:
        v = _TALENT_CHAR_IDX.get(ch)
        if v is None:
            return None
        for b in range(6):
            bits.append((v >> b) & 1)

    pos = [0]

    def read(n):
        r = 0
        for i in range(n):
            if pos[0] >= len(bits):
                return None
            r |= bits[pos[0]] << i
            pos[0] += 1
        return r

    if read(8) != LOADOUT_VERSION:
        return None
    read(16)  # spec id
    for _ in range(16):
        read(8)  # tree hash

    selected = {}
    for nid in full_node_order:
        is_sel = read(1)
        if is_sel is None:
            break
        if not is_sel:
            continue
        is_purchased = read(1)
        rank = None
        entry_index = 0
        if is_purchased:
            if read(1):
                rank = read(6)
            if read(1):
                entry_index = read(2) or 0
        selected[int(nid)] = {
            "entry_index": entry_index,
            "rank": rank,
            "purchased": bool(is_purchased),
        }
    return selected


def load_talent_tree_geometry(spec_id, static_dir=LOOKUP_DIR):
    """(fullNodeOrder, nodes) from ``<static_dir>/talents/<spec>.json`` — the
    decode order + node metadata (entries/type/maxRanks/free) the loadout encoder
    needs. Returns ([], {}) when the spec has no processed talent file, so callers
    degrade to "no encodable talents" rather than crashing."""
    path = os.path.join(static_dir, "talents", f"{spec_id}.json")
    try:
        doc = load_json(path)
    except FileNotFoundError:
        return [], {}
    return doc.get("fullNodeOrder", []), doc.get("nodes", {})


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_tier_sets(static_dir=LOOKUP_DIR):
    """Load the Raidbots item-sets catalog (``data/static/item-sets.json``) as the
    single source of truth for tier-set membership and names.

    That catalog is a small, curated list of the sets relevant to the *live* game
    (each ``{id, name, items: [item_id, ...], ...}``), unlike the ~968 historical
    ``itemSetId`` values sprinkled across every item in equippable-items.json. Using
    it keeps ``tier_set_items`` (and every tier-set display) scoped to the current
    sets and gives us their proper names.

    Returns ``(item_to_set, set_meta)``:
      * ``item_to_set``: ``{int item_id -> int item_set_id}``
      * ``set_meta``:    ``{int item_set_id -> {"name": str, "items": [int, ...]}}``

    Returns ``({}, {})`` when the file is absent (early season / not yet downloaded)
    so callers degrade to "no tier sets" instead of crashing."""
    path = os.path.join(static_dir, "item-sets.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            sets = json.load(f)
    except (OSError, ValueError):
        return {}, {}
    item_to_set = {}
    set_meta = {}
    for s in sets or []:
        sid = s.get("id")
        if sid is None:
            continue
        sid = int(sid)
        items = [int(i) for i in (s.get("items") or [])]
        set_meta[sid] = {"name": s.get("name"), "items": items}
        for iid in items:
            item_to_set[iid] = sid
    return item_to_set, set_meta


SEASON_INFO_ENV = "MYTHISTONE_SEASON_INFO"


def load_season_info(lookup_dir=None):
    """The seasonInfo the build renders against — id, name, slug and end dates.

    Normally data/static/seasonInfo.json. MYTHISTONE_SEASON_INFO points at an
    alternate file (in practice seasonInfo.prev.json) so a *final* snapshot of the
    outgoing season can still be built after seasonInfo.json has already flipped
    to the new one. That snapshot is what gates the season-rollover DB wipe, and
    it has to carry the outgoing season's whole identity, not just its id —
    otherwise the archived pages would show new-season branding over old-season
    data. See .github/workflows/seasonRolloverWipe.yml.

    Raises rather than returning a partial dict: a generator building against "no
    season" produces a plausible-looking empty page, and the archive step would
    then freeze that as the season's permanent record."""
    path = os.environ.get(SEASON_INFO_ENV, "").strip() or os.path.join(
        lookup_dir or LOOKUP_DIR, "seasonInfo.json"
    )
    info = load_json(path)
    if info.get("blizzard_season_id") is None:
        raise ValueError(f"blizzard_season_id missing from {path}")
    return info


def current_season_id(lookup_dir=None):
    """Blizzard season id the build renders against. See load_season_info."""
    return int(load_season_info(lookup_dir)["blizzard_season_id"])


def current_expansion_id(lookup_dir=None):
    """Current WoW expansion id the build renders against, read offline from
    seasonInfo.json (written by fetchSeasonAndPeriodInfo). Parallels current_season_id."""
    exp = load_season_info(lookup_dir).get("expansion_id")
    if exp is None:
        raise ValueError("expansion_id missing from seasonInfo.json")
    return int(exp)


# Raidbots mirrors the live retail client build; the collector reads the current
# expansion id off it instead of hardcoding a constant each expansion.
RAIDBOTS_METADATA_URL = "https://www.raidbots.com/static/data/live/metadata.json"


def derive_expansion_id():
    """Current WoW expansion id, derived from the live client build.

    The expansion id is the client's major version minus one (e.g. client 12.x
    -> expansion 11). This is the exact value Raider.IO's ``expansion_id``
    parameter and the ``expansion`` field on Raidbots' equippable-items.json both
    use, so every fetcher agrees on one derivation.

    Raises on a missing/malformed build rather than falling back to a stale id.
    """
    # Local import keeps commonUtils' import-time footprint to stdlib +
    # databaseConnector; requests is only needed by the offline data fetchers
    # that call this, never by the page/image generators that import this module.
    import requests

    resp = requests.get(RAIDBOTS_METADATA_URL, timeout=60)
    resp.raise_for_status()
    wow_build = resp.json().get("wowBuild")  # e.g. "12.1.0.68914"
    expansion_id = int(wow_build.split(".", 1)[0]) - 1
    print(f"Derived expansion_id = {expansion_id} (wowBuild {wow_build})")
    return expansion_id


# Weapon itemSubClass values that occupy both hands the way inventoryType 17
# (two-hand) does: bows, guns and crossbows are ranged mainhands with no
# off-hand.
TWO_HAND_SUBCLASSES = {2, 3, 18}

# Specs that dual-wield two-handers (Titan's Grip Fury). For these the "2H main
# hand => no off-hand" rule is wrong: they equip a two-hander in BOTH hands, so
# the off-hand must be kept even though the main hand is a 2H. (Single-Minded
# Fury uses one-handers, so its main hand isn't a 2H and the rule never fires.)
DUAL_WIELD_TWOHAND_SPECS = {72}  # Fury Warrior


def occupies_both_hands(item, spec_id=None):
    """Does this main-hand item leave the given spec no off-hand slot?

    Single source of truth for the "2H main hand => drop the OFF_HAND slot" rule
    the spec page's gear overview applies, so the two-hand marks the analyzer
    reads (baked into both spec_meta picks and the item icon shards) can never
    disagree with the slot list they are rendered against.

    ``spec_id`` carries the Titan's Grip exception: a two-hander occupies both
    hands for every spec *except* the ones in DUAL_WIELD_TWOHAND_SPECS, which
    wield one in each hand. Omit it only where the answer is a property of the
    item alone and no spec is in play (the analyzer's spec-independent icon
    shards) — every spec-scoped call must pass it.
    """
    if spec_id is not None and int(spec_id) in DUAL_WIELD_TWOHAND_SPECS:
        return False
    item = item or {}
    if item.get("inventoryType") == 17:
        return True
    # itemSubClass is only a weapon type on itemClass 2 — on armor the same
    # numbers mean leather/mail, which must not be mistaken for a two-hander.
    return item.get("itemClass") == 2 and item.get("itemSubClass") in TWO_HAND_SUBCLASSES


# --- enchant slot resolution + relevance filter -----------------------------
# The spec page and the item page both read enchant usage as bare enchant ids
# with no slot recorded, so the gear slot has to be recovered from the catalog's
# ``equipRequirements`` (data/static/enchantments.json). Kept here so both pages
# share one implementation and the relevance filter can never diverge between
# them. Pure dict/int logic — no new dependency for this stdlib-only module.

# Blizzard itemClass values seen on an enchant's `equipRequirements`.
ENCHANT_CLASS_WEAPON = 2
ENCHANT_CLASS_ARMOR = 4
ENCHANT_CLASS_PROFESSION_TOOL = 19

# Blizzard inventoryType -> display position matching the gear overview slot
# order (LEFT_ORDER + RIGHT_ORDER + WEAPON_SLOTS + TRINKET_SLOTS, columns
# flattened). Used to sort combo items the same way the overview lists slots.
INVTYPE_DISPLAY_ORDER = {
    1: 0,  # head
    2: 1,  # neck
    3: 2,  # shoulder
    16: 3,  # back
    5: 4,  # chest
    20: 4,  # robe (chest)
    9: 5,  # wrist
    10: 6,  # hands
    6: 7,  # waist
    7: 8,  # legs
    8: 9,  # feet
    11: 10,  # finger
    13: 11, 15: 11, 17: 11, 21: 11, 26: 11,  # main hand / two-hand / ranged
    14: 12, 22: 12, 23: 12,  # off hand / shield / held in off-hand
    12: 13,  # trinket
}

# Sorts anything we deliberately place after every gear slot (profession tools).
NON_GEAR_DISPLAY_ORDER = 99

# Bit index of an armor enchant's invTypeMask (the Blizzard inventoryType) -> the
# slot_group token the page renders that enchant under. Weapon inventoryTypes all
# collapse to WEAPON (main hand / off-hand are indistinguishable in the catalog
# and an enchant on both weapons already collapses to a single tile).
INVTYPE_ENCHANT_SLOT_GROUP = {
    1: "HEAD", 2: "NECK", 3: "SHOULDER", 16: "BACK", 5: "CHEST", 20: "CHEST",
    9: "WRIST", 10: "HANDS", 6: "WAIST", 7: "LEGS", 8: "FEET", 11: "FINGER",
    12: "TRINKET",
    13: "WEAPON", 15: "WEAPON", 17: "WEAPON", 21: "WEAPON", 25: "WEAPON",
    26: "WEAPON", 14: "WEAPON", 22: "WEAPON", 23: "WEAPON",
}

# slot_group token -> gear-overview display position, derived from
# INVTYPE_DISPLAY_ORDER so enchant_slot_pos (ordering) and enchant_slot_groups
# (filtering) are computed from one source and cannot diverge.
SLOT_GROUP_DISPLAY_POS = {}
for _inv, _grp in INVTYPE_ENCHANT_SLOT_GROUP.items():
    _pos = INVTYPE_DISPLAY_ORDER.get(_inv)
    if _pos is not None:
        SLOT_GROUP_DISPLAY_POS[_grp] = min(_pos, SLOT_GROUP_DISPLAY_POS.get(_grp, _pos))
del _inv, _grp, _pos


def enchant_slot_groups(record):
    """Set of slot_group tokens this enchant's equipRequirements allow.
    Empty set => not a gear-slot enchant (profession tool, gem/null reqs, or an
    unknown catalog shape) => drops everywhere. Robust: never raises."""
    req = (record or {}).get("equipRequirements") or {}
    ic = req.get("itemClass")
    if ic == ENCHANT_CLASS_WEAPON:
        return {"WEAPON"}
    if ic == ENCHANT_CLASS_ARMOR:
        mask = int(req.get("invTypeMask") or 0)
        return {INVTYPE_ENCHANT_SLOT_GROUP[b] for b in range(mask.bit_length())
                if (mask >> b) & 1 and b in INVTYPE_ENCHANT_SLOT_GROUP}
    return set()


def is_enchant_relevant(record, current_expansion, slot_group):
    """Show this catalog enchant under slot_group this expansion?
    record is the enchantments.json entry, or None if the id was absent (=> drop,
    preserving the existing 'not in enchantments.json' suppression)."""
    if record is None:
        return False
    exp = record.get("expansion")
    if exp is not None and int(exp) != int(current_expansion):
        return False
    return slot_group in enchant_slot_groups(record)


def enchant_slot_pos(info, enchant_id=None):
    """Gear-overview display position for an enchant, from enchantments.json.

    Enchant comps are multisets of enchant ids with no slot recorded, so the
    slot has to come from the catalog's ``equipRequirements``. For armor
    enchants ``invTypeMask`` is a bitmask whose BIT INDEX is the Blizzard
    inventoryType (bit 1 head, 3 shoulder, {5,20} chest/robe, 7 legs, 8 feet,
    9 wrist, 11 finger, 16 back), so it maps straight through
    INVTYPE_ENCHANT_SLOT_GROUP; the chest/robe pair collapses to one position.

    Weapon enchants (itemClass 2, including death knight runes) carry mask 0 --
    main hand and off-hand are indistinguishable in the catalog, and an enchant
    on both weapons already collapses to a single "x2" tile, so they all take
    the main-hand position.

    Built on enchant_slot_groups so ordering and the relevance filter can never
    disagree about which slot an enchant belongs to. Raises on anything else:
    ids missing from enchantments.json are dropped by the callers before they
    get here (deliberate old-enchant suppression), so a miss at this point means
    the catalog grew a shape we don't model, and silently sorting it to the end
    would just look like the ordering bug this replaces.
    """
    req = (info or {}).get("equipRequirements") or {}
    if req.get("itemClass") == ENCHANT_CLASS_PROFESSION_TOOL:
        return NON_GEAR_DISPLAY_ORDER
    groups = enchant_slot_groups(info)
    if groups:
        return min(SLOT_GROUP_DISPLAY_POS[g] for g in groups)
    raise ValueError(
        f"enchant {enchant_id if enchant_id is not None else (info or {}).get('id')} "
        f"has no known gear slot (itemClass={req.get('itemClass')!r}, "
        f"invTypeMask={req.get('invTypeMask')!r}) - enchantments.json shape changed"
    )


# --- lazily loaded lookup tables --------------------------------------------
# These replace the old import-time loads in generateSocialsPost.py so that
# importing any module is side-effect free; the JSON is read on first use.

_lookup_cache = {}


def _get_lookup(name):
    if name not in _lookup_cache:
        _lookup_cache[name] = load_json(os.path.join(LOOKUP_DIR, f"{name}.json"))
    return _lookup_cache[name]


def get_spec_lookup():
    return _get_lookup("specs")


def get_class_lookup():
    return _get_lookup("classes")


def get_dungeon_lookup():
    return _get_lookup("dungeons")


def find_dungeon_meta(dungeon_id):
    dungeon_lookup = get_dungeon_lookup()
    if isinstance(dungeon_lookup, dict):
        if str(dungeon_id) in dungeon_lookup:
            return dungeon_lookup[str(dungeon_id)]
        for v in dungeon_lookup.values():
            if str(v.get("id")) == str(dungeon_id):
                return v
    elif isinstance(dungeon_lookup, list):
        for d in dungeon_lookup:
            if str(d.get("id")) == str(dungeon_id):
                return d
    return None


def sort_spec_ids_by_role(spec_ids, spec_lookup):
    """Sort spec-id strings by role (tank, healer, dps), then numerically.
    Unknown ids sort last."""
    return sorted(
        spec_ids,
        key=lambda sid: (
            int(spec_lookup[sid]["role"]) if sid in spec_lookup else 99,
            int(sid),
        ),
    )


def format_comp_names(comp_str):
    """Turn a comma-separated spec-id comp string into 'Spec Class, ...' ordered
    by role (tank, healer, dps). Unknown ids are skipped."""
    if not comp_str:
        return ""
    spec_lookup = get_spec_lookup()
    class_lookup = get_class_lookup()
    ids = [s for s in str(comp_str).split(",") if s]
    ids = sorted(
        ids,
        key=lambda sid: (
            int(spec_lookup[sid]["role"]) if sid in spec_lookup else 99,
            int(sid) if sid.isdigit() else 0,
        ),
    )
    names = []
    for sid in ids:
        if sid in spec_lookup:
            sm = spec_lookup[sid]
            cm = class_lookup.get(str(sm.get("classID", "")), {})
            names.append(f"{sm.get('name', '')} {cm.get('name', '')}".strip())
    return ", ".join(names)


# formatters
def upgrade_info(duration, upgrade_map, keystone_level):
    """
    Given:
      - duration: an integer (ms) or something castable to int
      - upgrade_map: a dict whose values are dicts with
          { 'upgrade_level': int, 'qualifying_duration': int }
      - keystone_level: int or str (or None)
    Returns:
      A dict with:
        - text: the '+…' or '-' prefix joined to keystone_level
        - css:  the bootstrap class to use ('text-success' or 'text-danger')
    """
    try:
        dur = int(duration)
    except (TypeError, ValueError):
        # fallback to no upgrade
        return {"text": f"-{keystone_level or ''}", "css": "text-danger"}

    # sort descending by upgrade_level
    levels = sorted(
        upgrade_map.values(), key=lambda e: e["upgrade_level"], reverse=True
    )

    achieved = 0
    for lvl in levels:
        if dur <= lvl["qualifying_duration"]:
            achieved = lvl["upgrade_level"]
            break

    if achieved > 0:
        prefix, css = "+" * achieved, "text-success"
    else:
        prefix, css = "-", "text-danger"

    return {"text": f"{prefix}{keystone_level or ''}", "css": css}


def humanize_number(value):
    """
    Turn 123 → '123', 1500 → '1.5k', 500000 → '500k', 3000000 → '3m', etc.
    """
    try:
        n = int(value)
    except (TypeError, ValueError):
        return value

    if n >= 1_000_000:
        x = n / 1_000_000.0
        # one decimal, strip trailing .0
        s = f"{x:.1f}".rstrip("0").rstrip(".")
        return f"{s} M"
    if n >= 1_000:
        x = n / 1_000.0
        s = f"{x:.1f}".rstrip("0").rstrip(".")
        return f"{s} K"
    return str(n)


def format_duration(ms):
    """
    Turn a millisecond count into:
      - "MM:SS.mmm" if under an hour
      - "HH:MM:SS.mmm" if one hour or more

    Examples:
      34567    → "00:34.567"
      1234567  → "20:34.567"
      3661000  → "01:01:01.000"
    """
    try:
        total_ms = int(ms)
    except (TypeError, ValueError):
        return ms

    # Break into components
    total_seconds = total_ms // 1000
    milliseconds = total_ms % 1000

    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60

    # Zero‑pad each piece
    hh = f"{hours:02d}"
    mm = f"{minutes:02d}"
    ss = f"{seconds:02d}"
    mmm = f"{milliseconds:03d}"

    # Build the string
    base = f"{mm}:{ss}.{mmm}"
    if hours > 0:
        return f"{hh}:{base}"
    return base


def fetch_stat_info(conn, cursor, spec_id, current_season_id, spec_lookup):
    stats = databaseConnector.fetch_stats(conn, cursor, spec_id, current_season_id)
    stat_priority = []
    tertiary_priority = []
    health_priority = []
    for stat, value in stats.items():
        if stat == "mainstat":
            value["name"] = spec_lookup[spec_id].get("primary_stat")
            stat_priority.append(value)
        elif stat in SECONDARY_STATS:
            value["name"] = stat
            stat_priority.append(value)
        elif stat in TERTIARY_STATS:
            value["name"] = stat
            tertiary_priority.append(value)
        elif stat in HEALTH_STATS:
            value["name"] = stat
            health_priority.append(value)
    return stat_priority, tertiary_priority, health_priority
