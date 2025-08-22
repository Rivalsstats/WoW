import argparse
import json, pickle
import os
import time
from collections import defaultdict
from pathlib import Path
import copy


# helpers
def default_int_dict():
    return defaultdict(int)


# socket bucket factory
def default_socket_bucket():
    # note: defaultdict(int) inside is pickleable because it's built from a top‐level function
    return {
        "count": 0,
        "contains": defaultdict(int),
    }


def load_cache(path: Path):
    # Try pickle first
    try:
        with path.open("rb") as f:
            return pickle.load(f)
    except Exception:
        # Fallback to JSON for backward‑compat
        text = path.read_text(encoding="utf-8")
        return json.loads(text) if text.strip() else {}


# normal funcs


def load_all(input_dir):
    """
    Read every JSON under input_dir and accumulate them into a dict keyed by
    (region, season, period, dungeon_id, keystone_level). Uses original realm aggregate
    structure: spec_talents, class_talents, hero_talent_counts.
    """
    agg = {}
    for fn in Path(input_dir).rglob("*.json"):
        data = json.loads(fn.read_text())
        # skip any JSON that isn’t a per-realm aggregate
        if not all(
            k in data
            for k in ("region", "season", "period", "dungeon_id", "keystone_level")
        ):
            print(f"Warning: skipping non-aggregate JSON file {fn}")
            continue
        key = (
            data["region"],
            data["season"],
            data["period"],
            data["dungeon_id"],
            data["keystone_level"],
        )
        # Initialize accumulator if first time
        if key not in agg:
            agg[key] = {
                "total_runs": 0,
                "upgrade_counts": {
                    "depleted": 0,
                    "upgrade_1": 0,
                    "upgrade_2": 0,
                    "upgrade_3": 0,
                },
                "spec_counts": defaultdict(int),
                "spec_upgrade_counts": defaultdict(lambda: defaultdict(int)),
                "data_count": defaultdict(int),
                "class_talents": defaultdict(lambda: defaultdict(int)),
                "spec_talents": defaultdict(lambda: defaultdict(int)),
                "hero_talents": defaultdict(lambda: defaultdict(int)),
                "loadout_codes": defaultdict(lambda: defaultdict(int)),
                "hero_trees": defaultdict(lambda: defaultdict(int)),
                "item_buckets": {},
                "shortest_run": None,
                "longest_run": None,
                "highest_run": None,
                "spec_runs": defaultdict(
                    lambda: {
                        "shortest_run": None,
                        "longest_run": None,
                        "highest_run": None,
                    }
                ),
            }
        rec = agg[key]

        # sum total_runs
        rec["total_runs"] += data.get("total_runs", 0)
        for bkt, cnt in data.get("upgrade_counts", {}).items():
            rec["upgrade_counts"][bkt] += cnt

        if data.get("shortest_run"):
            sr = data["shortest_run"]
            if (
                rec["shortest_run"] is None
                or sr["duration"] < rec["shortest_run"]["duration"]
            ):
                # keep run metadata plus realm info if you want to trace it
                rec["shortest_run"] = dict(
                    sr,
                    **{
                        "region": data["region"],
                        "season": data["season"],
                        "period": data["period"],
                    },
                )
        if data.get("longest_run"):
            lr = data["longest_run"]
            if (
                rec["longest_run"] is None
                or lr["duration"] > rec["longest_run"]["duration"]
            ):
                rec["longest_run"] = dict(
                    lr,
                    **{
                        "region": data["region"],
                        "season": data["season"],
                        "period": data["period"],
                    },
                )
        if data.get("highest_run"):
            hr = data["highest_run"]
            if (
                rec["highest_run"] is None
                or hr["keystone_level"] > rec["highest_run"]["keystone_level"]
                or (
                    hr["keystone_level"] == rec["highest_run"]["keystone_level"]
                    and hr["duration"] < rec["highest_run"]["duration"]
                )
            ):
                rec["highest_run"] = dict(
                    hr,
                    **{
                        "region": data["region"],
                        "season": data["season"],
                        "period": data["period"],
                    },
                )
        # merge specializations & talents
        for spec in data.get("specializations", []):
            sid = spec["spec_id"]
            srec = rec["spec_runs"][sid]
            if spec.get("shortest_run"):
                s_sr = spec["shortest_run"]
                if (
                    srec["shortest_run"] is None
                    or s_sr["duration"] < srec["shortest_run"]["duration"]
                ):
                    for member in s_sr['members']:
                        if member['spec_id'] == sid:
                            srec["shortest_run"] = dict(
                                s_sr,
                                dungeon_id=data["dungeon_id"],
                                keystone_level=data["keystone_level"],
                            )
                            break
                    assert any(m["spec_id"] == sid for m in s_sr["members"]), (
                        f"BUG: spec {sid} not in candidates shortest_run members for region: {data["region"]} dungeon: { data["dungeon_id"]} keylevel: {data["keystone_level"]} "
                    )
            if spec.get("longest_run"):
                s_lr = spec["longest_run"]
                if (
                    srec["longest_run"] is None
                    or s_lr["duration"] > srec["longest_run"]["duration"]
                ):
                    for member in s_lr['members']:
                        if member['spec_id'] == sid:
                            srec["longest_run"] = dict(
                                s_lr,
                                dungeon_id=data["dungeon_id"],
                                keystone_level=data["keystone_level"],
                            )
                            break
                    assert any(m["spec_id"] == sid for m in s_lr["members"]), (
                        f"BUG: spec {sid} not in candidates shortest_run members for region: {data["region"]} dungeon: { data["dungeon_id"]} keylevel: {data["keystone_level"]} "
                    )
            if spec.get("highest_run"):
                s_hr = spec["highest_run"]
                best_s = srec["highest_run"]
                if (
                    best_s is None
                    or s_hr["keystone_level"] > best_s["keystone_level"]
                    or (
                        s_hr["keystone_level"] == best_s["keystone_level"]
                        and s_hr["duration"] < best_s["duration"]
                    )
                ):
                    for member in s_hr['members']:
                        if member['spec_id'] == sid:
                            srec["highest_run"] = dict(
                                s_hr,
                                dungeon_id=data["dungeon_id"],
                                keystone_level=data["keystone_level"],
                            )
                            break
                    assert any(m["spec_id"] == sid for m in s_hr["members"]), (
                        f"BUG: spec {sid} not in candidates shortest_run members for region: {data["region"]} dungeon: { data["dungeon_id"]} keylevel: {data["keystone_level"]} "
                    )
            rec["spec_counts"][sid] += spec.get("picked", 0)
            for uc in spec.get("upgrade_counts", []):
                rec["spec_upgrade_counts"][sid][uc["tier"]] += uc["count"]
            rec["data_count"][sid] += spec.get("data_count", 0)
            for ct in spec.get("class_talents", []):
                rec["class_talents"][sid][ct["talent_id"]] += ct["count"]
            for st in spec.get("spec_talents", []):
                rec["spec_talents"][sid][st["talent_id"]] += st["count"]
            for ht in spec.get("hero_talents", []):
                rec["hero_talents"][sid][ht["talent_id"]] += ht["count"]
            for lo in spec.get("loadout_codes", []):
                rec["loadout_codes"][sid][lo["code"]] += lo["count"]
            for tree in spec.get("hero_talent_trees", []):
                rec["hero_trees"][sid][tree["tree_id"]] += tree["count"]

        for spec_items in data.get("items", []):
            sid = spec_items["spec_id"]
            spec_buckets = rec["item_buckets"].setdefault(sid, {})
            for slot, items in spec_items.get("equipped", {}).items():
                slot_buckets = spec_buckets.setdefault(slot, {})
                for it in items:
                    iid = it["id"]
                    bucket = slot_buckets.setdefault(
                        iid,
                        {
                            "count": 0,
                            "upgrade_counts": defaultdict(int),
                            "enchantments": defaultdict(int),
                            "sockets": defaultdict(
                                lambda: {"count": 0, "contains": defaultdict(int)}
                            ),
                            "levels": defaultdict(int),
                            "bonus_ids": defaultdict(int),
                            "bonus_lists": defaultdict(int),
                        },
                    )
                    for tier, cnt in it.get("upgrade_count", {}).items():
                        bucket["upgrade_counts"][tier] += cnt
                    bucket["count"] += it.get("count", 0)
                    for ench in it.get("enchantments", []):
                        bucket["enchantments"][ench["id"]] += ench["count"]
                    for sock in it.get("sockets", []):
                        st = sock["type"]
                        bucket["sockets"][st]["count"] += sock["count"]
                        for gm in sock.get("contains", []):
                            bucket["sockets"][st]["contains"][gm["id"]] += gm["count"]
                    for lvl in it.get("level", []):
                        bucket["levels"][lvl["ilvl"]] += lvl["count"]
                    for bid in it.get("bonus_ids", []):
                        bucket["bonus_ids"][bid["id"]] += bid["count"]
                    for bl in it.get("bonus_list", []):
                        tpl = tuple(bl["list"])
                        bucket["bonus_lists"][tpl] += bl["count"]

    return agg


def merge_record(agg, data):
    """
    Merge a single realm-aggregate JSON dict `data` into the running `agg` dict,
    using exactly the same logic as load_all does per-file.
    """
    # skip non-aggregate
    if not all(
        k in data
        for k in ("region", "season", "period", "dungeon_id", "keystone_level")
    ):
        print(f"Warning: skipping non-aggregate JSON data {data}")
        return
    key = (
        data["region"],
        data["season"],
        data["period"],
        data["dungeon_id"],
        data["keystone_level"],
    )
    if key not in agg:
        print(f"Initializing aggregate for {key}")
        agg[key] = {
            "total_runs": 0,
            "upgrade_counts": {
                "depleted": 0,
                "upgrade_1": 0,
                "upgrade_2": 0,
                "upgrade_3": 0,
            },
            "spec_counts": defaultdict(int),
            "spec_upgrade_counts": defaultdict(default_int_dict),
            "data_count": defaultdict(int),
            "class_talents": defaultdict(default_int_dict),
            "spec_talents": defaultdict(default_int_dict),
            "hero_talents": defaultdict(default_int_dict),
            "loadout_codes": defaultdict(default_int_dict),
            "hero_trees": defaultdict(default_int_dict),
            "item_buckets": {},
            "shortest_run": None,
            "longest_run": None,
            "highest_run": None,
        }
    rec = agg[key]
    rec["total_runs"] += data.get("total_runs", 0)
    for bkt, cnt in data.get("upgrade_counts", {}).items():
        rec["upgrade_counts"][bkt] += cnt

    if data.get("shortest_run"):
        sr = data["shortest_run"]
        if (
            rec["shortest_run"] is None
            or sr["duration"] < rec["shortest_run"]["duration"]
        ):
            # keep run metadata plus realm info if you want to trace it
            rec["shortest_run"] = dict(
                sr,
                **{
                    "region": data["region"],
                    "season": data["season"],
                    "period": data["period"],
                },
            )
    if data.get("longest_run"):
        lr = data["longest_run"]
        if (
            rec["longest_run"] is None
            or lr["duration"] > rec["longest_run"]["duration"]
        ):
            rec["longest_run"] = dict(
                lr,
                **{
                    "region": data["region"],
                    "season": data["season"],
                    "period": data["period"],
                },
            )
    if data.get("highest_run"):
        hr = data["highest_run"]
        if (
            rec["highest_run"] is None
            or hr["keystone_level"] > rec["highest_run"]["keystone_level"]
            or (
                hr["keystone_level"] == rec["highest_run"]["keystone_level"]
                and hr["duration"] < rec["highest_run"]["duration"]
            )
        ):
            rec["highest_run"] = dict(
                hr,
                **{
                    "region": data["region"],
                    "season": data["season"],
                    "period": data["period"],
                },
            )
    # merge specializations & talents
    for spec in data.get("specializations", []):
        sid = spec["spec_id"]
        srec = agg[key]["spec_runs"][sid]
        if spec.get("shortest_run"):
            s_sr = spec["shortest_run"]
            if (
                srec["shortest_run"] is None
                or s_sr["duration"] < srec["shortest_run"]["duration"]
            ):
                for member in s_sr['members']:
                    if member['spec_id'] == sid:
                        srec["shortest_run"] = dict(
                            s_sr,
                            dungeon_id=data["dungeon_id"],
                            keystone_level=data["keystone_level"],
                        )
                        break
                assert any(m["spec_id"] == sid for m in s_sr["members"]), (
                        f"BUG: spec {sid} not in candidates shortest_run members for region: {data["region"]} dungeon: { data["dungeon_id"]} keylevel: {data["keystone_level"]} "
                    )
        if spec.get("longest_run"):
            s_lr = spec["longest_run"]
            if (
                srec["longest_run"] is None
                or s_lr["duration"] > srec["longest_run"]["duration"]
            ):
                for member in s_lr['members']:
                    if member['spec_id'] == sid:
                        srec["longest_run"] = dict(
                            s_lr,
                            dungeon_id=data["dungeon_id"],
                            keystone_level=data["keystone_level"],
                        )
                        break
                assert any(m["spec_id"] == sid for m in s_lr["members"]), (
                        f"BUG: spec {sid} not in candidates shortest_run members for region: {data["region"]} dungeon: { data["dungeon_id"]} keylevel: {data["keystone_level"]} "
                    )
                
        if spec.get("highest_run"):
            s_hr = spec["highest_run"]
            best_s = srec["highest_run"]
            if (
                best_s is None
                or s_hr["keystone_level"] > best_s["keystone_level"]
                or (
                    s_hr["keystone_level"] == best_s["keystone_level"]
                    and s_hr["duration"] < best_s["duration"]
                )
            ):
                for member in s_hr['members']:
                    if member['spec_id'] == sid:
                        srec["highest_run"] = dict(
                            s_hr,
                            dungeon_id=data["dungeon_id"],
                            keystone_level=data["keystone_level"],
                        )
                        break
                assert any(m["spec_id"] == sid for m in s_hr["members"]), (
                        f"BUG: spec {sid} not in candidates shortest_run members for region: {data["region"]} dungeon: { data["dungeon_id"]} keylevel: {data["keystone_level"]} "
                    )

        rec["spec_counts"][sid] += spec.get("picked", 0)
        for uc in spec.get("upgrade_counts", []):
            rec["spec_upgrade_counts"][sid][uc["tier"]] += uc["count"]
        rec["data_count"][sid] += spec.get("data_count", 0)
        for ct in spec.get("class_talents", []):
            rec["class_talents"][sid][ct["talent_id"]] += ct["count"]
        for st in spec.get("spec_talents", []):
            rec["spec_talents"][sid][st["talent_id"]] += st["count"]
        for ht in spec.get("hero_talents", []):
            rec["hero_talents"][sid][ht["talent_id"]] += ht["count"]
        for lo in spec.get("loadout_codes", []):
            rec["loadout_codes"][sid][lo["code"]] += lo["count"]
        for tree in spec.get("hero_talent_trees", []):
            rec["hero_trees"][sid][tree["tree_id"]] += tree["count"]

    for spec_items in data.get("items", []):
        sid = spec_items["spec_id"]
        spec_buckets = rec["item_buckets"].setdefault(sid, {})
        for slot, items in spec_items.get("equipped", {}).items():
            slot_buckets = spec_buckets.setdefault(slot, {})
            for it in items:
                iid = it["id"]
                bucket = slot_buckets.setdefault(
                    iid,
                    {
                        "count": 0,
                        "upgrade_counts": defaultdict(int),
                        "enchantments": defaultdict(int),
                        "sockets": defaultdict(default_socket_bucket),
                        "levels": defaultdict(int),
                        "bonus_ids": defaultdict(int),
                        "bonus_lists": defaultdict(int),
                    },
                )
                for tier, cnt in it.get("upgrade_count", {}).items():
                    bucket["upgrade_counts"][tier] += cnt
                bucket["count"] += it.get("count", 0)
                for ench in it.get("enchantments", []):
                    bucket["enchantments"][ench["id"]] += ench["count"]
                for sock in it.get("sockets", []):
                    st = sock["type"]
                    bucket["sockets"][st]["count"] += sock["count"]
                    for gm in sock.get("contains", []):
                        bucket["sockets"][st]["contains"][gm["id"]] += gm["count"]
                for lvl in it.get("level", []):
                    bucket["levels"][lvl["ilvl"]] += lvl["count"]
                for bid in it.get("bonus_ids", []):
                    bucket["bonus_ids"][bid["id"]] += bid["count"]
                for bl in it.get("bonus_list", []):
                    tpl = tuple(bl["list"])
                    bucket["bonus_lists"][tpl] += bl["count"]


def dump_final(agg, output_dir):
    """
    Write overall leaderboard JSONs per (region, season, dungeon, period, keystone)
    with original spec/hero talent counts fields.
    """
    for (region, season, period, dungeon, k), rec in agg.items():
        out = {
            "region": region,
            "season": season,
            "period": period,
            "dungeon_id": dungeon,
            "keystone_level": k,
            "total_runs": rec["total_runs"],
            "upgrade_counts": rec["upgrade_counts"],
            "specializations": [],
            "items": [],
        }
        for sid in sorted(rec["spec_counts"]):
            out["specializations"].append(
                {
                    "spec_id": sid,
                    "picked": rec["spec_counts"][sid],
                    "upgrade_counts": [
                        {"tier": tier, "count": cnt}
                        for tier, cnt in rec["spec_upgrade_counts"][sid].items()
                    ],
                    "data_count": rec["data_count"][sid],
                    "class_talents": [
                        {"talent_id": tid, "count": rec["class_talents"][sid][tid]}
                        for tid in sorted(rec["class_talents"][sid])
                    ],
                    "spec_talents": [
                        {"talent_id": tid, "count": rec["spec_talents"][sid][tid]}
                        for tid in sorted(rec["spec_talents"][sid])
                    ],
                    "hero_talents": [
                        {"talent_id": tid, "count": rec["hero_talents"][sid][tid]}
                        for tid in sorted(rec["hero_talents"][sid])
                    ],
                    "loadout_codes": [
                        {"code": c, "count": n}
                        for c, n in rec["loadout_codes"].get(sid, {}).items()
                    ],
                    "hero_talent_trees": [
                        {"tree_id": t, "count": n}
                        for t, n in rec["hero_trees"].get(sid, {}).items()
                    ],
                }
            )
        for sid, buckets in rec["item_buckets"].items():
            equipped = {}
            for slot, items in buckets.items():
                equipped[slot] = [
                    {
                        "id": iid,
                        "count": b["count"],
                        "upgrade_counts": [
                            {"tier": tier, "count": cnt}
                            for tier, cnt in b.get("upgrade_counts", {}).items()
                        ],
                        "enchantments": [
                            {"id": eid, "count": cnt}
                            for eid, cnt in b["enchantments"].items()
                        ],
                        "sockets": [
                            {
                                "type": st,
                                "count": sdat["count"],
                                "contains": [
                                    {"id": gid, "count": gc}
                                    for gid, gc in sdat["contains"].items()
                                ],
                            }
                            for st, sdat in b["sockets"].items()
                        ],
                        "level": [
                            {"ilvl": ilv, "count": cnt}
                            for ilv, cnt in b["levels"].items()
                        ],
                        "bonus_ids": [
                            {"id": bid, "count": cnt}
                            for bid, cnt in b["bonus_ids"].items()
                        ],
                        "bonus_list": [
                            {"list": list(bl), "count": cnt}
                            for bl, cnt in b["bonus_lists"].items()
                        ],
                    }
                    for iid, b in items.items()
                ]
            out["items"].append(
                {
                    "spec_id": sid,
                    "picked": rec["spec_counts"][sid],
                    "equipped": equipped,
                }
            )

        dst = Path(output_dir) / region / season / str(dungeon) / str(period)
        dst.mkdir(parents=True, exist_ok=True)
        with open(dst / f"{k}.json", "w") as f:
            json.dump(out, f, indent=2)


def dump_specs(agg, output_dir):
    """
    Write per-spec aggregates using spec_talents, etc.
    """
    season_spec_agg = defaultdict(lambda: {})  # season -> { spec_id: agg_rec }
    for (_, season, _, dungeon, k), rec in agg.items():
        for sid, _ in rec["spec_counts"].items():
            spec_map = season_spec_agg[season]
            sa = spec_map.setdefault(
                sid,
                {
                    "picked": 0,
                    "data_count": 0,
                    "upgrade_counts": defaultdict(int),
                    "class_talents": defaultdict(int),
                    "spec_talents": defaultdict(int),
                    "hero_talents": defaultdict(int),
                    "loadout_codes": defaultdict(int),
                    "hero_trees": defaultdict(int),
                    "dungeons": {},
                    "item_buckets": defaultdict(
                        lambda: defaultdict(
                            lambda: {
                                "count": 0,
                                "upgrade_counts": defaultdict(int),
                                "enchantments": defaultdict(int),
                                "sockets": defaultdict(
                                    lambda: {"count": 0, "contains": defaultdict(int)}
                                ),
                                "levels": defaultdict(int),
                                "bonus_ids": defaultdict(int),
                                "bonus_lists": defaultdict(int),
                            }
                        )
                    ),
                    "shortest_run": None,
                    "longest_run": None,
                    "highest_run": None,
                },
            )
            sa["picked"] += rec["spec_counts"][sid]
            sa["data_count"] += rec["data_count"][sid]
            spec_sr = rec["spec_runs"][sid]["shortest_run"]
            if spec_sr:
                if (
                    sa["shortest_run"] is None
                    or spec_sr["duration"] < sa["shortest_run"]["duration"]
                ):
                    for member in spec_sr['members']:
                        if member['spec_id'] == sid:
                            sa["shortest_run"] = {
                                **spec_sr,
                                "dungeon_id": dungeon,
                                "keystone_level": k,
                            }
                            break
                    assert any(m["spec_id"] == sid for m in spec_sr["members"]), (
                        f"BUG: spec {sid} not in candidates shortest_run members for dungeon: { dungeon} keylevel: {k} "
                    )
            spec_lr = rec["spec_runs"][sid]["longest_run"]
            if spec_lr:
                if (
                    sa["longest_run"] is None
                    or spec_lr["duration"] > sa["longest_run"]["duration"]
                ):
                    for member in spec_lr['members']:
                        if member['spec_id'] == sid:
                            sa["longest_run"] = {
                                **spec_lr,
                                "dungeon_id": dungeon,
                                "keystone_level": k,
                            }
                            break
                    assert any(m["spec_id"] == sid for m in spec_lr["members"]), (
                        f"BUG: spec {sid} not in candidates shortest_run members for dungeon: { dungeon} keylevel: {k} "
                    )
            spec_hr = rec["spec_runs"][sid]["highest_run"]
            if spec_hr:
                best = sa["highest_run"]
                if (
                    best is None
                    or spec_hr["keystone_level"] > best["keystone_level"]
                    or (
                        spec_hr["keystone_level"] == best["keystone_level"]
                        and spec_hr["duration"] < best["duration"]
                    )
                ):
                    for member in spec_hr['members']:
                        if member['spec_id'] == sid:
                            sa["highest_run"] = {
                                **spec_hr,
                                "dungeon_id": dungeon,
                                "keystone_level": k,
                            }
                            break
                    assert any(m["spec_id"] == sid for m in spec_hr["members"]), (
                        f"BUG: spec {sid} not in candidates shortest_run members for dungeon: { dungeon} keylevel: {k} "
                    )

            for tier, cnt in rec["spec_upgrade_counts"][sid].items():
                sa["upgrade_counts"][tier] += cnt
            for tid, cnt in rec["class_talents"][sid].items():
                sa["class_talents"][tid] += cnt
            for tid, cnt in rec["spec_talents"][sid].items():
                sa["spec_talents"][tid] += cnt
            for tid, cnt in rec["hero_talents"][sid].items():
                sa["hero_talents"][tid] += cnt
            for code, cnt in rec["loadout_codes"].get(sid, {}).items():
                sa["loadout_codes"][code] += cnt
            for tree, cnt in rec["hero_trees"].get(sid, {}).items():
                sa["hero_trees"][tree] += cnt

            dmap = sa["dungeons"].setdefault(dungeon, {})
            ed = dmap.setdefault(
                k,
                {
                    "picked": 0,
                    "class_talents": defaultdict(int),
                    "data_count": 0,
                    "upgrade_counts": defaultdict(int),
                    "spec_talents": defaultdict(int),
                    "hero_talents": defaultdict(int),
                    "loadout_codes": defaultdict(int),
                    "hero_trees": defaultdict(int),
                    "item_buckets": defaultdict(
                        lambda: defaultdict(
                            lambda: {
                                "count": 0,
                                "upgrade_counts": defaultdict(int),
                                "enchantments": defaultdict(int),
                                "sockets": defaultdict(
                                    lambda: {"count": 0, "contains": defaultdict(int)}
                                ),
                                "levels": defaultdict(int),
                                "bonus_ids": defaultdict(int),
                                "bonus_lists": defaultdict(int),
                            }
                        )
                    ),
                },
            )
            ed["picked"] += rec["spec_counts"][sid]
            ed["data_count"] += rec["data_count"][sid]
            for tier, cnt in rec["upgrade_counts"].items():
                ed["upgrade_counts"][tier] += cnt
            for tid, cnt in rec["class_talents"][sid].items():
                ed["class_talents"][tid] += cnt
            for tid, cnt in rec["spec_talents"][sid].items():
                ed["spec_talents"][tid] += cnt
            for tid, cnt in rec["hero_talents"][sid].items():
                ed["hero_talents"][tid] += cnt
            for code, cnt in rec["loadout_codes"].get(sid, {}).items():
                ed["loadout_codes"][code] += cnt
            for tree, cnt in rec["hero_trees"].get(sid, {}).items():
                ed["hero_trees"][tree] += cnt

            for slot, items in rec.get("item_buckets", {}).get(sid, {}).items():
                for iid, b in items.items():
                    dst = sa["item_buckets"][slot][iid]
                    dst["count"] += b["count"]
                    for tier, cnt in b["upgrade_counts"].items():
                        dst["upgrade_counts"][tier] += cnt
                    for eid, cnt in b["enchantments"].items():
                        dst["enchantments"][eid] += cnt
                    for stype, sdat in b["sockets"].items():
                        dst_socket = dst["sockets"][stype]
                        dst_socket["count"] += sdat["count"]
                        for gid, gc in sdat["contains"].items():
                            dst_socket["contains"][gid] += gc
                    for ilvl, cnt in b["levels"].items():
                        dst["levels"][ilvl] += cnt
                    for bid, cnt in b["bonus_ids"].items():
                        dst["bonus_ids"][bid] += cnt
                    for blt, cnt in b["bonus_lists"].items():
                        dst["bonus_lists"][blt] += cnt

            for slot, items in rec.get("item_buckets", {}).get(sid, {}).items():
                for iid, b in items.items():
                    dst = ed["item_buckets"][slot][iid]
                    dst["count"] += b["count"]
                    for tier, cnt in b["upgrade_counts"].items():
                        dst["upgrade_counts"][tier] += cnt
                    for eid, cnt in b["enchantments"].items():
                        dst["enchantments"][eid] += cnt
                    for stype, sdat in b["sockets"].items():
                        dst_socket = dst["sockets"][stype]
                        dst_socket["count"] += sdat["count"]
                        for gid, gc in sdat["contains"].items():
                            dst_socket["contains"][gid] += gc
                    for ilvl, cnt in b["levels"].items():
                        dst["levels"][ilvl] += cnt
                    for bid, cnt in b["bonus_ids"].items():
                        dst["bonus_ids"][bid] += cnt
                    for blt, cnt in b["bonus_lists"].items():
                        dst["bonus_lists"][blt] += cnt

    for season, spec_map in season_spec_agg.items():
        for sid, rec in spec_map.items():
            base = Path(output_dir) / "specs" / str(season) / str(sid)
            base.mkdir(parents=True, exist_ok=True)
            gen_time = int(time.time())
            general = {
                "spec_id": sid,
                "generated_at": gen_time,
                "season": season,
                "picked": rec["picked"],
                "data_count": rec["data_count"],
                "upgrade_counts": [
                    {"tier": t, "count": c}
                    for t, c in sorted(rec["upgrade_counts"].items())
                ],
                "class_talents": [
                    {"talent_id": tid, "count": cnt}
                    for tid, cnt in sorted(rec["class_talents"].items())
                ],
                "spec_talents": [
                    {"talent_id": tid, "count": cnt}
                    for tid, cnt in sorted(rec["spec_talents"].items())
                ],
                "hero_talents": [
                    {"talent_id": tid, "count": cnt}
                    for tid, cnt in sorted(rec["hero_talents"].items())
                ],
                "loadout_codes": [
                    {"code": c, "count": n} for c, n in rec["loadout_codes"].items()
                ],
                "hero_trees": [
                    {"tree_id": t, "count": n} for t, n in rec["hero_trees"].items()
                ],
            }

            general["items"] = []
            for slot, items in rec["item_buckets"].items():
                general["items"].append(
                    {
                        "slot": slot,
                        "equipped": [
                            {
                                "id": iid,
                                "count": b["count"],
                                "upgrade_counts": [
                                    {"tier": t, "count": cnt}
                                    for t, cnt in b["upgrade_counts"].items()
                                ],
                                "enchantments": [
                                    {"id": eid, "count": cnt}
                                    for eid, cnt in b["enchantments"].items()
                                ],
                                "sockets": [
                                    {
                                        "type": st,
                                        "count": sdat["count"],
                                        "contains": [
                                            {"id": gid, "count": gc}
                                            for gid, gc in sdat["contains"].items()
                                        ],
                                    }
                                    for st, sdat in b["sockets"].items()
                                ],
                                "level": [
                                    {"ilvl": ilv, "count": cnt}
                                    for ilv, cnt in b["levels"].items()
                                ],
                                "bonus_ids": [
                                    {"id": bid, "count": cnt}
                                    for bid, cnt in b["bonus_ids"].items()
                                ],
                                "bonus_list": [
                                    {"list": list(bl), "count": cnt}
                                    for bl, cnt in b["bonus_lists"].items()
                                ],
                            }
                            for iid, b in items.items()
                        ],
                    }
                )

            general["shortest_run"] = rec["shortest_run"]
            general["longest_run"] = rec["longest_run"]
            general["highest_run"] = rec["highest_run"]

            with open(base / "general.json", "w") as f:
                json.dump(general, f, indent=2)
            ddir_root = base / "dungeons"
            ddir_root.mkdir(exist_ok=True)
            for dungeon, kmap in rec["dungeons"].items():
                ddir = ddir_root / str(dungeon)
                ddir.mkdir(exist_ok=True)
                for k, ed in kmap.items():
                    out = {
                        "spec_id": sid,
                        "season": season,
                        "generated_at": gen_time,
                        "dungeon_id": dungeon,
                        "keystone_level": k,
                        "picked": ed["picked"],
                        "data_count": ed["data_count"],
                        "upgrade_counts": [
                            {"tier": t, "count": c}
                            for t, c in sorted(ed["upgrade_counts"].items())
                        ],
                        "class_talents": [
                            {"talent_id": tid, "count": cnt}
                            for tid, cnt in sorted(ed["class_talents"].items())
                        ],
                        "spec_talents": [
                            {"talent_id": tid, "count": cnt}
                            for tid, cnt in sorted(ed["spec_talents"].items())
                        ],
                        "hero_talents": [
                            {"talent_id": tid, "count": cnt}
                            for tid, cnt in sorted(ed["hero_talents"].items())
                        ],
                        "loadout_codes": [
                            {"code": c, "count": n}
                            for c, n in ed["loadout_codes"].items()
                        ],
                        "hero_trees": [
                            {"tree_id": t, "count": n}
                            for t, n in ed["hero_trees"].items()
                        ],
                    }
                    out["items"] = []
                    for slot, items in ed["item_buckets"].items():
                        out["items"].append(
                            {
                                "slot": slot,
                                "equipped": [
                                    {
                                        "id": iid,
                                        "count": b["count"],
                                        "upgrade_counts": [
                                            {"tier": t, "count": cnt}
                                            for t, cnt in b["upgrade_counts"].items()
                                        ],
                                        "enchantments": [
                                            {"id": eid, "count": cnt}
                                            for eid, cnt in b["enchantments"].items()
                                        ],
                                        "sockets": [
                                            {
                                                "type": st,
                                                "count": sdat["count"],
                                                "contains": [
                                                    {"id": gid, "count": gc}
                                                    for gid, gc in sdat[
                                                        "contains"
                                                    ].items()
                                                ],
                                            }
                                            for st, sdat in b["sockets"].items()
                                        ],
                                        "level": [
                                            {"ilvl": ilv, "count": cnt}
                                            for ilv, cnt in b["levels"].items()
                                        ],
                                        "bonus_ids": [
                                            {"id": bid, "count": cnt}
                                            for bid, cnt in b["bonus_ids"].items()
                                        ],
                                        "bonus_list": [
                                            {"list": list(bl), "count": cnt}
                                            for bl, cnt in b["bonus_lists"].items()
                                        ],
                                    }
                                    for iid, b in items.items()
                                ],
                            }
                        )

                    with open(ddir / f"{k}.json", "w") as f:
                        json.dump(out, f, indent=2)


def dump_season_summary(agg, output_dir):
    """
    Produce a one‐off summary JSON for the latest season across all regions,
    writing dungeons, specs (with keystones), and periods.
    """
    seasons = {season for (_, season, _, _, _) in agg.keys()}
    # assumes seasons are comparable (e.g. integers or sortable strings)
    current = sorted(seasons)[-1]
    # accumulators
    dungeon_runs = defaultdict(int)
    spec_picks = defaultdict(int)
    spec_keys = defaultdict(lambda: defaultdict(int))
    period_runs = defaultdict(int)
    season_upgrade_counts = defaultdict(int)
    spec_total_upgrade_counts = defaultdict(lambda: defaultdict(int))
    spec_key_upgrade_counts = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    dungeon_upgrade_counts = defaultdict(lambda: defaultdict(int))
    dungeon_keys = defaultdict(lambda: defaultdict(int))
    dungeon_key_upgrade_counts = defaultdict(
        lambda: defaultdict(lambda: defaultdict(int))
    )
    period_upgrade_counts = defaultdict(lambda: defaultdict(int))

    global_shortest = None
    global_longest = None
    global_highest = None

    for (region, season, period, dungeon, k), rec in agg.items():
        if season != current:
            continue

        sr = rec.get("shortest_run")
        lr = rec.get("longest_run")
        hr = rec.get("highest_run")

        if sr:
            if global_shortest is None or sr["duration"] < global_shortest["duration"]:
                global_shortest = {
                    **sr,
                    "region": region,
                    "season": season,
                    "period": period,
                }
        if lr:
            if global_longest is None or lr["duration"] > global_longest["duration"]:
                global_longest = {
                    **lr,
                    "region": region,
                    "season": season,
                    "period": period,
                }

        if hr:
            if (
                global_highest is None
                or hr["keystone_level"] > global_highest["keystone_level"]
                or (
                    hr["keystone_level"] == global_highest["keystone_level"]
                    and hr["duration"] < global_highest["duration"]
                )
            ):
                global_highest = {
                    **hr,
                    "region": region,
                    "season": season,
                    "period": period,
                }

        # total_runs at this key
        runs = rec["total_runs"]

        dungeon_runs[dungeon] += runs
        dungeon_keys[dungeon][k] += runs
        for tier, cnt in rec["upgrade_counts"].items():
            dungeon_upgrade_counts[dungeon][tier] += cnt
            dungeon_key_upgrade_counts[dungeon][k][tier] += cnt
        period_runs[period] += runs
        for tier, cnt in rec["upgrade_counts"].items():
            period_upgrade_counts[period][tier] += cnt
        # specs & their keystone breakdown
        for sid, picked in rec["spec_counts"].items():
            spec_picks[sid] += picked
            spec_keys[sid][k] += picked
            for tier, cnt in rec["spec_upgrade_counts"][sid].items():
                spec_key_upgrade_counts[sid][k][tier] += cnt
                spec_total_upgrade_counts[sid][tier] += cnt
        for tier, cnt in rec["upgrade_counts"].items():
            season_upgrade_counts[tier] += cnt

    # build JSON blob
    summary = {
        "season": current,
        "generated_at": int(time.time()),
        "Dungeons": [
            {
                "id": d,
                "count": dungeon_runs[d],
                "upgrade_counts": [
                    {"tier": tier, "count": dungeon_upgrade_counts[d][tier]}
                    for tier in sorted(dungeon_upgrade_counts[d])
                ],
                "keys": [
                    {
                        "keystone_level": lvl,
                        "count": dungeon_keys[d][lvl],
                        "upgrade_counts": [
                            {
                                "tier": tier,
                                "count": dungeon_key_upgrade_counts[d][lvl][tier],
                            }
                            for tier in sorted(dungeon_key_upgrade_counts[d][lvl])
                        ],
                    }
                    for lvl in sorted(dungeon_keys[d])
                ],
            }
            for d in sorted(dungeon_runs)
        ],
        "Specs": [
            {
                "id": sid,
                "count": spec_picks[sid],
                "upgrade_counts": [
                    {"tier": tier, "count": spec_total_upgrade_counts[sid][tier]}
                    for tier in sorted(spec_total_upgrade_counts[sid])
                ],
                "keys": [
                    {
                        "keystone_level": lvl,
                        "count": cnt,
                        "upgrade_counts": [
                            {
                                "tier": tier,
                                "count": spec_key_upgrade_counts[sid][lvl][tier],
                            }
                            for tier in sorted(spec_key_upgrade_counts[sid][lvl])
                        ],
                    }
                    for lvl, cnt in sorted(spec_keys[sid].items())
                ],
            }
            for sid in sorted(spec_picks)
        ],
        "periods": [
            {
                "id": p,
                "count": period_runs[p],
                "upgrade_counts": [
                    {"tier": tier, "count": period_upgrade_counts[p][tier]}
                    for tier in sorted(period_upgrade_counts[p])
                ],
            }
            for p in sorted(period_runs)
        ],
        "total_runs": sum(dungeon_runs.values()),
        "upgrade_counts": [
            {"tier": t, "count": c} for t, c in sorted(season_upgrade_counts.items())
        ],
        "shortest_run": global_shortest,
        "longest_run": global_longest,
        "highest_run": global_highest,
    }

    # write it out
    out_path = Path(output_dir) / "season_summary.json"
    with open(out_path, "w") as f:
        json.dump(summary, f, indent=2)


def merge_list_of_dicts(base_list, new_list, key_field, nested_fields=None):
    """
    Merge two lists of dicts by key_field:
    - Sum 'count' values for matching keys
    - Include items unique to either list
    - Recursively merge nested list fields specified in nested_fields

    Args:
        base_list (list): existing items
        new_list (list): incoming items to merge
        key_field (str): dict key used to identify items
        nested_fields (dict): mapping of field_name -> (sub_key_field, sub_nested_fields)

    Returns:
        dict: mapping key -> merged dict
    """
    merged = {item[key_field]: dict(item) for item in base_list}
    for item in new_list:
        k = item[key_field]
        if k not in merged:
            # new entry, add entire dict
            merged[k] = dict(item)
        else:
            # merge counts
            merged[k]["count"] = merged[k].get("count", 0) + item.get("count", 0)
            if nested_fields:
                for field, (subkey, subnested) in nested_fields.items():
                    base_sub = merged[k].get(field, [])
                    new_sub = item.get(field, [])
                    # recursively merge sublists
                    merged_sub = merge_list_of_dicts(
                        base_sub, new_sub, subkey, subnested
                    )
                    merged[k][field] = list(merged_sub.values())
    return merged


def merge_item_lists_equipped(base_equipped, new_equipped):
    """
    Merge two lists of equipped‑item dicts (each dict has 'id','count', plus
    nested lists). Returns a merged list.
    """
    # map existing items by id
    by_id = {item["id"]: copy.deepcopy(item) for item in base_equipped}

    for it in new_equipped:
        iid = it["id"]
        if iid not in by_id:
            # brand-new equipped item
            by_id[iid] = copy.deepcopy(it)
        else:
            # merge into existing
            dst = by_id[iid]
            dst["count"] += it["count"]

            # for each simple nested list we can reuse your merge_list_of_dicts helper
            dst["upgrade_counts"] = list(
                merge_list_of_dicts(
                    dst["upgrade_counts"], it["upgrade_counts"], "tier"
                ).values()
            )
            dst["enchantments"] = list(
                merge_list_of_dicts(
                    dst["enchantments"], it["enchantments"], "id"
                ).values()
            )
            dst["level"] = list(
                merge_list_of_dicts(dst["level"], it["level"], "ilvl").values()
            )
            dst["bonus_ids"] = list(
                merge_list_of_dicts(dst["bonus_ids"], it["bonus_ids"], "id").values()
            )

            # sockets is one level deeper: keyed by 'type', then merge their 'contains'
            sock_map = merge_list_of_dicts(dst["sockets"], it["sockets"], "type")
            for sock in sock_map.values():
                # find matching new socket
                new_sock = next(
                    (s for s in it["sockets"] if s["type"] == sock["type"]), None
                )
                if new_sock:
                    sock["contains"] = list(
                        merge_list_of_dicts(
                            sock["contains"], new_sock["contains"], "id"
                        ).values()
                    )
            dst["sockets"] = list(sock_map.values())

            # bonus_list: key by the tuple of the list itself
            def key_bonus(bl):
                return tuple(bl["list"])

            bl_map = {key_bonus(b): copy.deepcopy(b) for b in dst["bonus_list"]}
            for b in it["bonus_list"]:
                k = key_bonus(b)
                if k not in bl_map:
                    bl_map[k] = copy.deepcopy(b)
                else:
                    bl_map[k]["count"] += b["count"]
            dst["bonus_list"] = list(bl_map.values())

    return list(by_id.values())


def merge_items_by_slot(base_items, new_items):
    """
    Merge two 'items' arrays, each a list of slot‑dicts:
      {
        'slot': SLOT_NAME,
        'equipped': [ ... item dicts ... ]
      }
    """
    # map existing slots
    slots = {itm["slot"]: copy.deepcopy(itm) for itm in base_items}

    for itm in new_items:
        slot = itm["slot"]
        if slot not in slots:
            # brand‑new slot
            slots[slot] = copy.deepcopy(itm)
        else:
            # merge the two equipped arrays
            dst = slots[slot]
            dst_equipped = dst.setdefault("equipped", [])
            dst["equipped"] = merge_item_lists_equipped(
                dst_equipped, itm.get("equipped", [])
            )

    return list(slots.values())


def combine_group_data(input_dir):
    """
    Walks through each group folder in input_dir, reads and deep-merges:
    - season_summary.json files
    - specs/<season>/<spec>/general.json
    - specs/<season>/<spec>/dungeons/<dungeon>/<key>.json

    Nested lists (e.g. upgrade_counts, keys) are merged by their key fields,
    summing counts and including any new entries across groups.

    Returns:
        {
            'season_summary': { ... merged summary ... },
            'specs': { (season, spec): { 'general': {...}, 'dungeons': {...} } }
        }
    """
    merged = {"season_summary": None, "specs": {}}
    print(f"Combining data from {input_dir}")

    # Process each group
    for group in os.listdir(input_dir):
        print(f"Checking group: {group}")
        group_path = os.path.join(input_dir, group)
        if group == "dungeons":
            continue
        print(f"Processing group: {group_path}")
        if not os.path.isdir(group_path):
            print(f"Skipping {group_path}, not a directory.")
            continue

        # --- Season Summary ---
        summary_path = os.path.join(group_path, "season_summary.json")
        if os.path.isfile(summary_path):
            with open(summary_path) as f:
                summary = json.load(f)
            if merged["season_summary"] is None:
                # make a clean copy so we don't mutate the original
                merged["season_summary"] = copy.deepcopy(summary)
            else:
                ms = merged["season_summary"]
                # sum totals
                ms["total_runs"] += summary.get("total_runs", 0)
                # merge upgrade_counts
                ms_uc = merge_list_of_dicts(
                    ms["upgrade_counts"], summary["upgrade_counts"], "tier"
                )
                ms["upgrade_counts"] = list(ms_uc.values())
                # min/max runs
                if ms.get("shortest_run") and summary.get("shortest_run"):
                    ms["shortest_run"] = (
                        ms["shortest_run"]
                        if ms["shortest_run"]["duration"]
                        < summary["shortest_run"]["duration"]
                        else summary["shortest_run"]
                    )
                else:
                    # pick whichever is non‑None
                    ms["shortest_run"] = ms.get("shortest_run") or summary.get(
                        "shortest_run"
                    )
                if ms.get("longest_run") and summary.get("longest_run"):
                    ms["longest_run"] = (
                        ms["longest_run"]
                        if ms["longest_run"]["duration"]
                        > summary["longest_run"]["duration"]
                        else summary["longest_run"]
                    )
                else:
                    # pick whichever is non‑None
                    ms["longest_run"] = ms.get("longest_run") or summary.get(
                        "longest_run"
                    )
                if ms.get("highest_run") and summary.get("highest_run"):    
                    ar = ms["highest_run"]
                    br = summary["highest_run"]
                    if (
                        ar["keystone_level"] > br["keystone_level"]
                        or (
                            ar["keystone_level"] == br["keystone_level"]
                            and ar["duration"] < br["duration"]
                        )
                    ):
                        ms["highest_run"] = ar
                    else:
                        ms["highest_run"] = br
                else:
                    # pick whichever is non‑None
                    ms["highest_run"] = ms.get("highest_run") or summary.get(
                        "highest_run"
                    )
                # merge Dungeons, Specs, periods
                ms["Dungeons"] = list(
                    merge_list_of_dicts(
                        ms["Dungeons"],
                        summary["Dungeons"],
                        "id",
                        nested_fields={
                            "upgrade_counts": ("tier", None),
                            "keys": (
                                "keystone_level",
                                {"upgrade_counts": ("tier", None)},
                            ),
                        },
                    ).values()
                )
                ms["Specs"] = list(
                    merge_list_of_dicts(
                        ms["Specs"],
                        summary["Specs"],
                        "id",
                        nested_fields={
                            "upgrade_counts": ("tier", None),
                            "keys": (
                                "keystone_level",
                                {"upgrade_counts": ("tier", None)},
                            ),
                        },
                    ).values()
                )
                ms["periods"] = list(
                    merge_list_of_dicts(
                        ms["periods"],
                        summary["periods"],
                        "id",
                        nested_fields={"upgrade_counts": ("tier", None)},
                    ).values()
                )

        # --- Specs per season/spec ---
        specs_root = os.path.join(group_path, "specs")
        if not os.path.isdir(specs_root):
            continue
        for season_id in os.listdir(specs_root):
            season_path = os.path.join(specs_root, season_id)
            if not os.path.isdir(season_path):
                continue
            for spec_id in os.listdir(season_path):
                spec_path = os.path.join(season_path, spec_id)
                if not os.path.isdir(spec_path):
                    continue
                key = (season_id, spec_id)
                # initialize spec entry
                if key not in merged["specs"]:
                    merged["specs"][key] = {"general": None, "dungeons": {}}

                # general.json
                gen_path = os.path.join(spec_path, "general.json")
                if os.path.isfile(gen_path):
                    with open(gen_path) as f:
                        gen = json.load(f)
                    if merged["specs"][key]["general"] is None:
                        merged["specs"][key]["general"] = copy.deepcopy(gen)
                    else:
                        mg = merged["specs"][key]["general"]
                        mg["picked"] += gen["picked"]
                        mg["data_count"] += gen["data_count"]
                        # merge upgrade_counts
                        uc = merge_list_of_dicts(
                            mg["upgrade_counts"], gen["upgrade_counts"], "tier"
                        )
                        mg["upgrade_counts"] = list(uc.values())
                        # merge talents, codes, trees
                        for field, subkey in [
                            ("class_talents", "talent_id"),
                            ("spec_talents", "talent_id"),
                            ("hero_talents", "talent_id"),
                            ("loadout_codes", "code"),
                            ("hero_trees", "tree_id"),
                        ]:
                            merged_list = merge_list_of_dicts(
                                mg[field], gen[field], subkey
                            )
                            mg[field] = list(merged_list.values())

                        if "items" in gen:
                            mg_items = mg.setdefault("items", [])
                            mg["items"] = merge_items_by_slot(mg_items, gen["items"])

                        if mg.get("shortest_run") and gen.get("shortest_run"):
                            mg["shortest_run"] = (
                                mg["shortest_run"]
                                if mg["shortest_run"]["duration"]
                                < gen["shortest_run"]["duration"]
                                else gen["shortest_run"]
                            )
                        else:
                            mg["shortest_run"] = mg.get("shortest_run") or gen.get(
                                "shortest_run"
                            )

                        # longest_run: pick the larger duration
                        if mg.get("longest_run") and gen.get("longest_run"):
                            mg["longest_run"] = (
                                mg["longest_run"]
                                if mg["longest_run"]["duration"]
                                > gen["longest_run"]["duration"]
                                else gen["longest_run"]
                            )
                        else:
                            mg["longest_run"] = mg.get("longest_run") or gen.get(
                                "longest_run"
                            )

                        # highest_run: highest keystone, tiebreak shorter duration
                        if mg.get("highest_run") and gen.get("highest_run"):
                            ar, br = mg["highest_run"], gen["highest_run"]
                            if ar["keystone_level"] > br["keystone_level"] or (
                                ar["keystone_level"] == br["keystone_level"]
                                and ar["duration"] < br["duration"]
                            ):
                                mg["highest_run"] = ar
                            else:
                                mg["highest_run"] = br
                        else:
                            mg["highest_run"] = mg.get("highest_run") or gen.get(
                                "highest_run"
                            )

                # per-dungeon keystones
                dungeons_root = os.path.join(spec_path, "dungeons")
                if not os.path.isdir(dungeons_root):
                    continue
                for dungeon_id in os.listdir(dungeons_root):
                    dun_path = os.path.join(dungeons_root, dungeon_id)
                    if not os.path.isdir(dun_path):
                        continue
                    for file in os.listdir(dun_path):
                        if not file.endswith(".json"):
                            continue
                        lvl = file[:-5]
                        with open(os.path.join(dun_path, file)) as f:
                            detail = json.load(f)
                        dkey = (dungeon_id, lvl)
                        spec_dun = merged["specs"][key]["dungeons"]
                        if dkey not in spec_dun:
                            spec_dun[dkey] = detail
                        else:
                            md = spec_dun[dkey]
                            md["picked"] += detail["picked"]
                            md["data_count"] += detail["data_count"]
                            uc = merge_list_of_dicts(
                                md["upgrade_counts"], detail["upgrade_counts"], "tier"
                            )
                            md["upgrade_counts"] = list(uc.values())
                            for field, subkey in [
                                ("class_talents", "talent_id"),
                                ("spec_talents", "talent_id"),
                                ("hero_talents", "talent_id"),
                                ("loadout_codes", "code"),
                                ("hero_trees", "tree_id"),
                            ]:
                                merged_list = merge_list_of_dicts(
                                    md[field], detail[field], subkey
                                )
                                md[field] = list(merged_list.values())

                            if "items" in detail:
                                dun_items = md.setdefault("items", [])
                                md["items"] = merge_items_by_slot(
                                    dun_items, detail["items"]
                                )
    return merged


def main():
    parser = argparse.ArgumentParser(
        description="Combine per-realm aggregate JSONs into group data"
    )
    parser.add_argument(
        "--input-dir",
        "-i",
        required=True,
        help="Directory containing per-realm outputs (data/aggregated/realms)",
    )
    parser.add_argument(
        "--output-dir",
        "-o",
        required=True,
        help="Where to write final JSONs (data/aggregated/final)",
    )
    args = parser.parse_args()

    agg = load_all(args.input_dir)

    dump_specs(agg, args.output_dir)
    dump_season_summary(agg, args.output_dir)
    print(f"Dumped {len(agg)} entries")


if __name__ == "__main__":
    main()
