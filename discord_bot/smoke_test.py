"""Offline smoke test — runs in the docker build with no DB, Discord or network.

Feeds fixtures shaped like the real ``databaseConnector`` / site-artifact outputs
into each cog's pure ``build_*_embed`` function and asserts the result respects
every Discord embed limit, plus exercises the validation and formatting helpers.
A failure here fails the image build.
"""

import os
import sys

import discord

from . import charts, embeds, lookups
from .cogs import analyze, comps, dungeon, items, routes, season, spec, stats
from .errors import ValidationError
from .site_data import RouteIndexes

FAILURES = []


def check(condition, message):
    if not condition:
        FAILURES.append(message)


def assert_embed(embed: discord.Embed, label: str):
    check(isinstance(embed, discord.Embed), f"{label}: not an Embed")
    check(len(embed) <= embeds.MAX_TOTAL, f"{label}: total length {len(embed)} > 6000")
    check(len(embed.fields) <= embeds.MAX_FIELDS, f"{label}: >25 fields")
    check(bool(embed.title), f"{label}: missing title")
    for i, field in enumerate(embed.fields):
        check(len(field.value) <= embeds.MAX_FIELD_VALUE, f"{label}: field {i} value > 1024")
        check(len(field.name) <= embeds.MAX_FIELD_NAME, f"{label}: field {i} name > 256")


# --- fixtures --------------------------------------------------------------
SAMPLE_SPEC_IDS = [int(s) for s in list(lookups.SPECS)[:12]]


def fx_top_run():
    return {
        "run_id": 39910404,
        "dungeon_id": 402,
        "keystone_level": 25,
        "duration": 1500000,
        "timestamp": 1783957342000,
        "faction": 0,
        "region": "eu",
        "season": 17,
        "members": [{"member_id": i, "spec_id": sid} for i, sid in enumerate(SAMPLE_SPEC_IDS[:5])],
    }


def fx_spec_upgrades():
    rows = []
    for i, sid in enumerate(lookups.SPECS):
        for level in range(10, 21):
            rows.append(
                {
                    "spec_id": int(sid),
                    "keystone_level": level,
                    "upgrade_3": (i + 1) * (level % 4),
                    "upgrade_2": (i + 2) * 3,
                    "upgrade_1": (i + 1) * 2,
                    "depleted": (i % 5) + 1,
                    "total_runs": (i + 1) * 20 + level,
                }
            )
    return rows


def fx_runs_per_dungeon_per_level():
    rows = []
    for i, did in enumerate(lookups.DUNGEONS):
        for level in range(10, 21):
            rows.append(
                {
                    "dungeon_id": did,
                    "keystone_level": level,
                    "upgrade_3": (i + 1) * 5,
                    "upgrade_2": (i + 2) * 7,
                    "upgrade_1": (i + 1) * 3,
                    "depleted": (i + 1) * 2,
                    "total_runs": (i + 1) * 100 + level,
                }
            )
    return rows


A_SPEC = next(iter(lookups.SPECS))          # e.g. "62"
A_DUNGEON = next(iter(lookups.DUNGEONS))     # e.g. "402"


def fx_spec_meta():
    return {
        "spec_id": int(A_SPEC),
        "spec": lookups.SPECS[A_SPEC]["name"],
        "class": "Mage",
        "slots": {
            "HEAD": {
                "top": [{"id": 250060, "name": "Voidbreaker's Veil", "icon": "x",
                         "quality": 4, "slug": "voidbreakers-veil", "pcs": [1, 2], "pct": 84.5}],
                "sim": {"id": 250060, "name": "Voidbreaker's Veil", "slug": "voidbreakers-veil", "icon": "x"},
                "common": {"id": 250060, "name": "Voidbreaker's Veil", "slug": "voidbreakers-veil", "icon": "x"},
            },
            "TRINKET_1": {
                "top": [],
                "sim": None,
                "common": {"id": 249343, "name": "Gaze of the Alnseer", "slug": "gaze-of-the-alnseer", "icon": "y"},
            },
        },
    }


def fx_top_loadout():
    return [(40, "BwUAeExampleLoadoutString==", 12000, 25, 20), (39, "BwUAeAnotherString==", 8000, 24, 19)]


def fx_stat_info():
    priority = [
        {"name": "INTELLECT", "avg_percent": None, "avg_raw": 50000.0},
        {"name": "haste", "avg_percent": 30.0, "avg_raw": 9000.0},
        {"name": "crit", "avg_percent": 25.0, "avg_raw": 8000.0},
    ]
    tertiary = [{"name": "speed", "avg_percent": 2.0, "avg_raw": 500.0}]
    health = [{"name": "stamina", "avg_percent": None, "avg_raw": 400000.0}]
    return priority, tertiary, health


def fx_items_index():
    return [
        {"id": 249343, "name": "Gaze of the Alnseer", "icon": "inv_x", "quality": 4,
         "slot": "Trinket", "slotKey": "TRINKET", "slug": "gaze-of-the-alnseer", "runs": 395357, "top_spec": 252},
        {"id": 250060, "name": "Voidbreaker's Veil", "icon": "inv_y", "quality": 4,
         "slot": "Head", "slotKey": "HEAD", "slug": "voidbreakers-veil", "runs": 200000, "top_spec": int(A_SPEC)},
    ]


def fx_lust():
    return [
        {"top_npcs": "190609,191736", "total_pulls_at_index": 100, "lust_count": 80,
         "lust_percentage": 80.0, "max_key_lusted": 25, "max_key_not_lusted": 18}
    ]


def fx_comps_index():
    ids = SAMPLE_SPEC_IDS
    out = []
    for i in range(30):
        c = [ids[i % len(ids)], ids[(i + 1) % len(ids)], ids[(i + 2) % len(ids)],
             ids[(i + 3) % len(ids)], ids[(i + 4) % len(ids)]]
        out.append({
            "c": c, "w": 1_000_000 - i * 1000, "t": 8000 - i * 100, "d": 1000,
            "runs": 9000 - i * 100, "avg_key": 20 - i * 0.1, "mk": 25 - (i % 5),
            "bd": int(A_DUNGEON), "bdr": 1000, "highkey_score": 5000 - i * 50,
            "dungeons": {A_DUNGEON: {"w": 100000 - i * 500, "t": 800, "d": 100,
                                      "mk": 22, "runs": 900, "avg_key": 19.5}},
        })
    return out


def fx_route_indexes():
    routes_map = {
        "aSpUxBJ": {"route_key": "aSpUxBJ", "run_id": 39910404, "dungeon": A_DUNGEON,
                    "level": 24, "duration": 1827770, "timestamp": 1783957342,
                    "specs": SAMPLE_SPEC_IDS[:5], "npcs": [190609], "spells": [390386],
                    "enemy_forces": 460, "usage_count": 158},
    }
    idx = RouteIndexes()
    for key, meta_ in routes_map.items():
        idx.route_meta[key] = meta_
        for s in meta_["specs"]:
            idx.spec_index.setdefault(int(s), set()).add(key)
        idx.dungeon_index.setdefault(str(meta_["dungeon"]), set()).add(key)
    return idx


# --- tests -----------------------------------------------------------------
def test_helpers():
    check(embeds.make_bar(0) == "░" * 12, "make_bar(0) wrong")
    check(embeds.make_bar(100) == "█" * 12, "make_bar(100) wrong")
    check(embeds.make_bar(150).count("█") == 12, "make_bar clamps > 100")
    check(embeds.make_bar(-10).count("█") == 0, "make_bar clamps < 0")
    check(embeds.clamp("abcdef", 4).endswith("…"), "clamp should ellipsize")
    check(len(embeds.clamp("x" * 5000, 1024)) <= 1024, "clamp respects limit")
    check(embeds.esc("*bold*") == "\\*bold\\*", "esc should escape markdown")


def test_resolvers():
    # A known spec: Frost Mage. classID for Mage is 8.
    mage = next(cid for cid, m in lookups.CLASSES.items() if m["name"] == "Mage")
    frost = lookups.resolve_spec(mage, "Frost")
    check(lookups.SPECS[frost]["name"] == "Frost", "resolve_spec Frost Mage failed")
    check(lookups.resolve_spec_full("Frost Mage") == frost, "resolve_spec_full failed")
    for bad in (("999", "Frost"), (mage, "Nonsense")):
        try:
            lookups.resolve_spec(*bad)
            FAILURES.append(f"resolve_spec accepted bad input {bad}")
        except ValidationError:
            pass
    try:
        lookups.resolve_dungeon("nope")
        FAILURES.append("resolve_dungeon accepted bad input")
    except ValidationError:
        pass
    check(len(lookups.CLASS_CHOICES) <= 25, "CLASS_CHOICES > 25")
    check(len(lookups.DUNGEON_CHOICES) <= 25, "DUNGEON_CHOICES > 25")


def test_season():
    assert_embed(season.build_season_embed(1_500_000, fx_top_run()), "season.info")
    assert_embed(season.build_season_embed(0, None), "season.info (empty)")


def test_spec():
    sid = A_SPEC
    assert_embed(spec.build_overview_embed(sid), "spec.overview")
    assert_embed(spec.build_gear_embed(sid, fx_spec_meta()), "spec.gear")
    assert_embed(spec.build_gear_embed(sid, {}), "spec.gear (empty)")
    assert_embed(spec.build_talents_embed(sid, fx_top_loadout()), "spec.talents")
    assert_embed(spec.build_talents_embed(sid, []), "spec.talents (empty)")
    assert_embed(spec.build_stats_embed(sid, fx_stat_info()), "spec.stats")


def test_analyze():
    meta = lookups.SPECS[A_SPEC]
    token = next(t for t, c in analyze._CLASS_TOKENS.items() if str(c) == str(meta["classID"]))
    export = f'{token}="X"\nspec={meta["name"].lower()}\nhead=,id=250060,enchant_id=1,gem_id=5\n'
    parsed = analyze.parse_simc(export)
    check(parsed["class_id"] == str(meta["classID"]), "parse_simc class")
    check(parsed["slots"].get("HEAD", {}).get("id") == 250060, "parse_simc slot id")
    check(analyze.resolve_spec(parsed) == int(A_SPEC), "resolve_spec roundtrip")
    # off-meta trinket (id 999 vs fixture common 249343) + on-meta head (250060)
    off = {"slots": {"HEAD": {"id": 250060, "enchant": None, "gems": []},
                     "TRINKET_1": {"id": 999, "enchant": None, "gems": []}}}
    assert_embed(analyze.build_analyze_embed(A_SPEC, fx_spec_meta(), off), "analyze (off)")
    fully = {"slots": {"HEAD": {"id": 250060, "enchant": None, "gems": []}}}
    assert_embed(analyze.build_analyze_embed(A_SPEC, fx_spec_meta(), fully), "analyze (meta)")


def test_dungeon():
    did = A_DUNGEON
    assert_embed(dungeon.build_overview_embed(did), "dungeon.overview")
    assert_embed(dungeon.build_lust_embed(did, fx_lust()), "dungeon.lust")


def test_comps():
    ci = fx_comps_index()
    assert_embed(comps.build_top_embed(ci), "comps.top")
    assert_embed(comps.build_top_embed(ci, A_DUNGEON), "comps.top (dungeon)")
    ids = SAMPLE_SPEC_IDS[:2]
    assert_embed(comps.build_fill_embed(ci, ids), "comps.fill")
    assert_embed(comps.build_fill_embed(ci, ids, A_DUNGEON), "comps.fill (dungeon)")
    assert_embed(comps.build_buffs_embed(SAMPLE_SPEC_IDS[:4]), "comps.buffs")
    # buff coverage sanity: an Arcane Mage (62) provides Arcane Intellect (1459)
    covered, missing = comps.buff_coverage([62])
    check(any(b["id"] == 1459 for b in covered), "buff_coverage missed Arcane Intellect")


def test_routes():
    idx = fx_route_indexes()
    assert_embed(routes.build_routes_embed(A_DUNGEON, [], idx), "routes (dungeon)")
    assert_embed(routes.build_routes_embed(A_DUNGEON, SAMPLE_SPEC_IDS[:2], idx), "routes (specs)")
    assert_embed(routes.build_routes_embed(A_DUNGEON, [999999], idx), "routes (no match)")


def fx_key_throughput():
    return [{"region": r, "period_id": 1055 + p, "run_count": 10000 + p * 500, "max_ts": 1}
            for r in ("us", "eu") for p in range(5)]


def test_stats():
    assert_embed(stats.build_highest_embed(fx_top_run()), "stats.highest")
    assert_embed(stats.build_highest_embed(None), "stats.highest (empty)")


def test_charts():
    import tempfile

    from image_generation.dungeon_tierlist import create_dungeon_tierlist_img
    from image_generation.spec_popularity_performance import (
        create_spec_popularity_vs_performance_img,
    )
    from image_generation.spec_popularity_tierlist import create_spec_tierlist_img
    from image_generation.tierlist_card import TIER_LETTERS
    from image_generation.tierlist_preview import generate_preview_image

    # Render each image once with pre-fetched fixtures (no DB, no network; the icon
    # files aren't present so tiles/markers are simply skipped) and assert a non-empty
    # PNG lands. Exercises both the matplotlib charts and the shared social renderers.
    total, su, dd = 5_000_000, fx_spec_upgrades(), fx_runs_per_dungeon_per_level()
    dps_rows = [
        {"spec_id": s, "rank": i + 1, "tier": TIER_LETTERS[i % len(TIER_LETTERS)],
         "primary": 1_000_000 - i * 40_000, "name": lookups.SPECS[str(s)]["name"],
         "class_name": "", "icon": None}
        for i, s in enumerate(SAMPLE_SPEC_IDS[:6])
    ]
    builders = [
        ("weekly.png", lambda p: charts.build_keys_per_week(fx_key_throughput(), {}, p)),
        ("keys.png", lambda p: charts.build_key_throughput(fx_key_throughput(), stats._load_periods(), p)),
        ("spec_tier.png", lambda p: create_spec_tierlist_img(p, 0, spec_upgrades=su, total_runs=total)),
        ("dungeon_tier.png", lambda p: create_dungeon_tierlist_img(p, 0, dungeon_data=dd, total_runs=total)),
        ("spec_perf.png", lambda p: create_spec_popularity_vs_performance_img(p, 0, spec_upgrades=su, highest_run=fx_top_run())),
        ("simdps.png", lambda p: generate_preview_image(dps_rows, lookups.SPECS, lookups.CLASSES, "Season 1", 1, out_path=p)),
    ]
    with tempfile.TemporaryDirectory() as tmp_dir:
        for name, builder in builders:
            out = os.path.join(tmp_dir, name)
            builder(out)
            check(os.path.exists(out) and os.path.getsize(out) > 1000, f"chart {name} not rendered")


def test_items():
    idx = fx_items_index()
    by_id = {int(it["id"]): it for it in idx}
    item = items.resolve_item("249343", by_id, idx)
    assert_embed(items.build_item_embed(item), "item.info")
    item2 = items.resolve_item("Voidbreaker's Veil", by_id, idx)
    check(item2["id"] == 250060, "resolve_item by name failed")
    try:
        items.resolve_item("no such item", by_id, idx)
        FAILURES.append("resolve_item accepted bad name")
    except ValidationError:
        pass


TESTS = [
    test_helpers, test_resolvers, test_season, test_spec, test_analyze, test_dungeon,
    test_comps, test_routes, test_items, test_stats, test_charts,
]


def main():
    for test in TESTS:
        try:
            test()
        except Exception as exc:  # noqa: BLE001 — report, don't abort the suite
            FAILURES.append(f"{test.__name__} raised {exc!r}")
    if FAILURES:
        print("SMOKE TEST FAILURES:")
        for failure in FAILURES:
            print(f"  - {failure}")
        return 1
    print(f"smoke test OK: {len(TESTS)} groups passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
