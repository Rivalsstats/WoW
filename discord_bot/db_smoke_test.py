"""DB-backed smoke test — drives the bot's real runtime data paths.

Unlike the old fixture-based check, this exercises the actual queries and
artifact fetches the cogs make, then feeds the real results into every pure
``build_*_embed`` function and asserts each result respects Discord's embed
limits, plus renders every social image from real rows.

Two data sources, mirrored exactly as the cogs use them:

* **Source A — the seeded local test DB.** Every ``db.run(databaseConnector.*)``
  (and ``commonUtils.fetch_stat_info``) is run against the throwaway MySQL that
  ``backend_scripts/localDev/seed_test_db.py`` provisions. Point this test at it by
  exporting the ``DATABASE_*`` vars the seeder prints. A missing/empty core fetch is
  a hard failure (extend ``localDev/seeders.py`` so the seed covers it).
* **Source B — the published site JSON artifacts.** ``comps_index``, ``comp_routes``,
  ``items_index``, ``simdps_tierlist`` and ``spec_meta/<id>`` are fetched live over
  HTTP through the real ``SiteData`` path (``https://mythistone.com/...``). These are
  soft: off-season / unreachable artifacts are skipped (logged), so CI stays green
  between seasons.

Run it (Docker Desktop required for the seed):

    python backend_scripts/localDev/seed_test_db.py     # prints DATABASE_* exports
    # export those DATABASE_* vars, then:
    python -m discord_bot.db_smoke_test

No ``DISCORD_BOT_TOKEN`` is needed. A non-zero exit fails the caller (the CI job).
"""

import asyncio
import json
import os
import sys
import tempfile

import aiohttp
import commonUtils
import databaseConnector
import discord

from . import charts, config, db, embeds, lookups
from .cogs import analyze, comps, dungeon, items, routes, season, spec, stats
from .errors import SiteDataError, ValidationError
from .site_data import SiteData

FAILURES = []
SKIPS = []


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


# Representative seeded ids (same convention the fixtures used).
A_SPEC = next(iter(lookups.SPECS))          # e.g. "62"
A_DUNGEON = next(iter(lookups.DUNGEONS))     # e.g. "402"
SAMPLE_SPEC_IDS = [int(s) for s in list(lookups.SPECS)[:12]]


def _load_periods():
    with open(os.path.join(config.STATIC_DIR, "periods.json"), "r", encoding="utf-8") as fh:
        return json.load(fh)


def _db_env() -> dict:
    """The DATABASE_* the seeder prints, straight from the environment."""
    keys = ["DATABASE_HOST", "DATABASE_USER", "DATABASE_PASSWORD", "DATABASE_NAME", "DATABASE_PORT"]
    env = {k: os.environ.get(k) for k in keys}
    missing = [k for k in keys if not env[k]]
    if missing:
        raise SystemExit(
            "db_smoke_test needs the seeded test DB. Missing " + ", ".join(missing) + ".\n"
            "Seed it: python backend_scripts/localDev/seed_test_db.py, then export the "
            "printed DATABASE_* vars."
        )
    env["BOT_DB_POOL_SIZE"] = int(os.environ.get("BOT_DB_POOL_SIZE") or 5)
    return env


async def _try_site(name, coro):
    """Await a SiteData accessor; on a fetch failure log a skip and return None."""
    try:
        result = await coro
    except SiteDataError as exc:
        SKIPS.append(f"{name}: {exc.user_message}")
        return None
    if not result:
        SKIPS.append(f"{name}: empty artifact")
    return result


# --- pure checks (no DB / no network) --------------------------------------
def test_helpers():
    check(embeds.make_bar(0) == "░" * 12, "make_bar(0) wrong")
    check(embeds.make_bar(100) == "█" * 12, "make_bar(100) wrong")
    check(embeds.make_bar(150).count("█") == 12, "make_bar clamps > 100")
    check(embeds.make_bar(-10).count("█") == 0, "make_bar clamps < 0")
    check(embeds.clamp("abcdef", 4).endswith("…"), "clamp should ellipsize")
    check(len(embeds.clamp("x" * 5000, 1024)) <= 1024, "clamp respects limit")
    check(embeds.esc("*bold*") == "\\*bold\\*", "esc should escape markdown")
    # the periodic support embed is a pure builder; assert it respects the limits.
    assert_embed(embeds.patreon_embed(), "patreon")


def test_support_nudge():
    """The periodic Patreon nudge counts per-guild in a server and per-user
    otherwise, firing exactly once every PATREON_EMBED_EVERY commands per scope."""
    class _FakeInter:
        def __init__(self, client, guild_id=None, user_id=None):
            self.client = client
            self.guild_id = guild_id
            self.user = type("U", (), {"id": user_id})()

    client = type("C", (), {"_support_counts": {}})()
    every = config.PATREON_EMBED_EVERY

    guild = _FakeInter(client, guild_id=123)
    due = [embeds._support_due(guild) for _ in range(every)]
    check(due[-1] is True, "guild nudge should fire on the Nth command")
    check(sum(1 for d in due if d) == 1, "guild nudge should fire exactly once per cycle")

    user = _FakeInter(client, guild_id=None, user_id=999)
    user_due = [embeds._support_due(user) for _ in range(every)]
    check(user_due[-1] is True, "user nudge should fire on the Nth per-user command")
    check(
        ("guild", 123) in client._support_counts and ("user", 999) in client._support_counts,
        "support counters should key per-scope (guild vs user)",
    )
    # no counter on the client => never due, never raises.
    check(embeds._support_due(_FakeInter(object(), guild_id=1)) is False, "missing counter should be inert")


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


def test_analyze_parse():
    meta = lookups.SPECS[A_SPEC]
    token = next(t for t, c in analyze._CLASS_TOKENS.items() if str(c) == str(meta["classID"]))
    export = f'{token}="X"\nspec={meta["name"].lower()}\nhead=,id=250060,enchant_id=1,gem_id=5\n'
    parsed = analyze.parse_simc(export)
    check(parsed["class_id"] == str(meta["classID"]), "parse_simc class")
    check(parsed["slots"].get("HEAD", {}).get("id") == 250060, "parse_simc slot id")
    check(analyze.resolve_spec(parsed) == int(A_SPEC), "resolve_spec roundtrip")
    # buff coverage sanity: an Arcane Mage (62) provides Arcane Intellect (1459)
    covered, _ = comps.buff_coverage([62])
    check(any(b["id"] == 1459 for b in covered), "buff_coverage missed Arcane Intellect")


# --- source A: seeded DB ---------------------------------------------------
async def test_season_db(site):
    total = await db.run(databaseConnector.fetch_total_season_runs, config.SEASON)
    max_run = await db.run(databaseConnector.fetch_max_key_run, config.SEASON)
    check(bool(total), "fetch_total_season_runs returned 0 (seed thin?)")
    check(bool(max_run), "fetch_max_key_run returned no run (seed thin?)")
    assert_embed(season.build_season_embed(total, max_run), "season.info")
    assert_embed(season.build_season_embed(0, None), "season.info (empty)")
    ns = embeds.season_not_started_embed()
    assert_embed(ns, "season.not_started")
    check("<t:" in "".join(f.value for f in ns.fields), "not_started missing discord timestamps")


async def test_spec_db(site):
    sid = A_SPEC
    assert_embed(spec.build_overview_embed(sid), "spec.overview")  # pure (lookups only)
    loadouts = await db.run(databaseConnector.fetch_top_loadout, sid, config.SEASON)
    check(bool(loadouts), "fetch_top_loadout returned no rows (seed thin?)")
    assert_embed(spec.build_talents_embed(sid, loadouts), "spec.talents")
    assert_embed(spec.build_talents_embed(sid, []), "spec.talents (empty)")
    stat_info = await db.run(commonUtils.fetch_stat_info, sid, config.SEASON, lookups.SPECS)
    assert_embed(spec.build_stats_embed(sid, stat_info), "spec.stats")


async def test_dungeon_db(site):
    did = A_DUNGEON
    assert_embed(dungeon.build_overview_embed(did), "dungeon.overview")  # pure
    timeline = await db.run(databaseConnector.fetch_dungeon_lust_timeline, did, dictionary=True)
    check(bool(timeline), "fetch_dungeon_lust_timeline returned no rows (seed thin?)")
    assert_embed(dungeon.build_lust_embed(did, timeline), "dungeon.lust")


async def test_stats_db(site):
    # season-wide records
    max_run = await db.run(databaseConnector.fetch_max_key_run, config.SEASON)
    assert_embed(stats.build_highest_embed(max_run), "stats.highest")
    assert_embed(stats.build_highest_embed(None), "stats.highest (empty)")
    longest = await db.run(databaseConnector.fetch_longest_run, config.SEASON)
    shortest = await db.run(databaseConnector.fetch_shortest_run, config.SEASON)
    check(bool(longest), "fetch_longest_run returned no run (seed thin?)")
    check(bool(shortest), "fetch_shortest_run returned no run (seed thin?)")
    assert_embed(stats.build_run_card("Longest run", longest), "stats.longest")
    assert_embed(stats.build_run_card("Fastest clear", shortest), "stats.shortest")

    # per-dungeon records (member-joined rows, dict cursor — mirrors the cog)
    did = A_DUNGEON
    dmax = await db.run(databaseConnector.fetch_dungeon_max_key_run, did, config.SEASON, dictionary=True)
    check(bool(dmax), "fetch_dungeon_max_key_run returned no run (seed thin?)")
    assert_embed(stats.build_run_card("Highest key", dmax), "stats.dungeon_highest")
    dlong = await db.run(databaseConnector.fetch_dungeon_longest_run, did, config.SEASON, dictionary=True)
    assert_embed(stats.build_run_card("Longest run", dlong), "stats.dungeon_longest")
    dshort = await db.run(databaseConnector.fetch_dungeon_shortest_run, did, config.SEASON, dictionary=True)
    assert_embed(stats.build_run_card("Fastest clear", dshort), "stats.dungeon_shortest")
    # closest/fastest can legitimately be empty for a given dungeon; assert the
    # embed shape either way (build_run_card renders an empty card).
    closest = await db.run(databaseConnector.fetch_dungeon_closest_call_run, did, config.SEASON, dictionary=True)
    assert_embed(stats.build_run_card("Closest call", closest, stats._margin_field(did, closest)), "stats.closest")
    fastest = await db.run(databaseConnector.fetch_dungeon_fastest_top_levels_run, did, config.SEASON, dictionary=True)
    assert_embed(stats.build_run_card("Fastest at top keys", fastest), "stats.fastest")


async def test_charts_db(site):
    from image_generation.dungeon_tierlist import create_dungeon_tierlist_img
    from image_generation.spec_popularity_performance import (
        create_spec_popularity_vs_performance_img,
    )
    from image_generation.spec_popularity_tierlist import create_spec_tierlist_img
    from image_generation.tierlist_preview import generate_preview_image

    spec_upgrades = await db.run(databaseConnector.fetch_spec_upgrades)
    check(bool(spec_upgrades), "fetch_spec_upgrades returned no rows (seed thin?)")
    runs_per = await db.run(databaseConnector.fetch_runs_per_dungeon_per_level, config.SEASON)
    check(bool(runs_per), "fetch_runs_per_dungeon_per_level returned no rows (seed thin?)")
    total = await db.run(databaseConnector.fetch_total_season_runs, config.SEASON)
    max_run = await db.run(databaseConnector.fetch_max_key_run, config.SEASON)
    kt = await db.run(databaseConnector.fetch_key_throughput, config.SEASON)
    check(bool(kt), "fetch_key_throughput returned no rows (seed thin?)")
    periods = _load_periods()

    # Icon files aren't present here, so tiles/markers are simply skipped; a
    # non-empty PNG still lands. Renders both the matplotlib charts and the shared
    # social renderers from real seeded rows.
    builders = [
        ("weekly.png", lambda p: charts.build_keys_per_week(kt, periods, p)),
        ("keys.png", lambda p: charts.build_key_throughput(kt, periods, p)),
        ("spec_tier.png", lambda p: create_spec_tierlist_img(
            p, config.SEASON, spec_upgrades=spec_upgrades, total_runs=total)),
        ("dungeon_tier.png", lambda p: create_dungeon_tierlist_img(
            p, config.SEASON, dungeon_data=runs_per, total_runs=total)),
        ("spec_perf.png", lambda p: create_spec_popularity_vs_performance_img(
            p, config.SEASON, spec_upgrades=spec_upgrades, highest_run=max_run or {})),
    ]
    # Sim DPS preview is driven by the published simdps_tierlist artifact (source B).
    sim = await _try_site("simdps_tierlist", site.simdps_tierlist())
    sim_rows = (sim.get("tabs") or {}).get("1") or [] if sim else []
    if sim_rows:
        builders.append(("simdps.png", lambda p: generate_preview_image(
            sim_rows, lookups.SPECS, lookups.CLASSES, config.SEASON_NAME, 1, out_path=p)))
    else:
        SKIPS.append("simdps.png: no live sim rows for 1 target")

    with tempfile.TemporaryDirectory() as tmp_dir:
        for name, builder in builders:
            out = os.path.join(tmp_dir, name)
            builder(out)
            check(os.path.exists(out) and os.path.getsize(out) > 1000, f"chart {name} not rendered")


# --- source B: live site JSON artifacts (soft) -----------------------------
async def test_comps_site(site):
    ci = await _try_site("comps_index", site.comps_index())
    if ci:
        assert_embed(comps.build_top_embed(ci), "comps.top")
        assert_embed(comps.build_top_embed(ci, A_DUNGEON), "comps.top (dungeon)")
        ids = SAMPLE_SPEC_IDS[:2]
        assert_embed(comps.build_fill_embed(ci, ids), "comps.fill")
        assert_embed(comps.build_fill_embed(ci, ids, A_DUNGEON), "comps.fill (dungeon)")
    # buffs need no artifact — always exercise them.
    assert_embed(comps.build_buffs_embed(SAMPLE_SPEC_IDS[:4]), "comps.buffs")


async def test_items_site(site):
    idx = await _try_site("items_index", site.items_index())
    if not idx:
        return
    by_id = await site.item_by_id()
    first = idx[0]
    item = items.resolve_item(str(first["id"]), by_id, idx)
    assert_embed(items.build_item_embed(item), "item.info")
    item2 = items.resolve_item(first["name"], by_id, idx)
    check(int(item2["id"]) == int(first["id"]), "resolve_item by name failed")
    try:
        items.resolve_item("no such item exists zzz", by_id, idx)
        FAILURES.append("resolve_item accepted bad name")
    except ValidationError:
        pass


async def test_routes_site(site):
    idx = await _try_site("comp_routes", site.comp_routes_indexes())
    if idx is None:
        return
    assert_embed(routes.build_routes_embed(A_DUNGEON, [], idx), "routes (dungeon)")
    assert_embed(routes.build_routes_embed(A_DUNGEON, SAMPLE_SPEC_IDS[:2], idx), "routes (specs)")
    assert_embed(routes.build_routes_embed(A_DUNGEON, [999999], idx), "routes (no match)")


async def test_spec_gear_site(site):
    spec_meta = await _try_site(f"spec_meta/{A_SPEC}", site.spec_meta(A_SPEC))
    assert_embed(spec.build_gear_embed(A_SPEC, {}), "spec.gear (empty)")  # empty path, no artifact
    if not spec_meta:
        return
    assert_embed(spec.build_gear_embed(A_SPEC, spec_meta), "spec.gear")
    # analyzer meta-check with a partly off-meta export against the real spec_meta.
    off = {"slots": {"HEAD": {"id": 250060, "enchant": None, "gems": []},
                     "TRINKET_1": {"id": 999, "enchant": None, "gems": []}}}
    assert_embed(analyze.build_analyze_embed(A_SPEC, spec_meta, off), "analyze (off)")


PURE_TESTS = [test_helpers, test_support_nudge, test_resolvers, test_analyze_parse]
DB_TESTS = [test_season_db, test_spec_db, test_dungeon_db, test_stats_db, test_charts_db]
SITE_TESTS = [test_comps_site, test_items_site, test_routes_site, test_spec_gear_site]


async def _amain():
    env = _db_env()
    db.init_pool(env)
    async with aiohttp.ClientSession() as session:
        site = SiteData(session)
        for test in DB_TESTS + SITE_TESTS:
            try:
                await test(site)
            except Exception as exc:  # noqa: BLE001 — report, don't abort the suite
                FAILURES.append(f"{test.__name__} raised {exc!r}")


def main():
    for test in PURE_TESTS:
        try:
            test()
        except Exception as exc:  # noqa: BLE001
            FAILURES.append(f"{test.__name__} raised {exc!r}")
    try:
        asyncio.run(_amain())
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001
        FAILURES.append(f"db/site suite raised {exc!r}")

    if SKIPS:
        print("skipped (soft):")
        for skip in SKIPS:
            print(f"  - {skip}")
    if FAILURES:
        print("DB SMOKE TEST FAILURES:")
        for failure in FAILURES:
            print(f"  - {failure}")
        return 1
    groups = len(PURE_TESTS) + len(DB_TESTS) + len(SITE_TESTS)
    print(f"db smoke test OK: {groups} groups passed ({len(SKIPS)} soft skips)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
