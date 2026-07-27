import databaseConnector
import json
from collections import defaultdict
from pathlib import Path
import requests

API_BASE = "https://{region}.api.blizzard.com"
OAUTH_BASE = "https://oauth.battle.net/token"
LOCALE = "en_US"
NAMESPACE_DYNAMIC = "dynamic-{region}"

SPEC_PATH = Path("data", "static", "specs.json")
TALENT_POINTS_PATH = Path("data", "static", "spendableTalents.json")


def load_existing_json(path: Path) -> dict:
    return json.loads(path.read_text()) if path.exists() else {}


def get_access_token(CLIENT_ID: str, CLIENT_SECRET: str) -> str:
    url = "https://oauth.battle.net/token"
    resp = requests.post(
        url, data={"grant_type": "client_credentials"}, auth=(CLIENT_ID, CLIENT_SECRET)
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def fetch_json(url: str, params: dict, token: str) -> dict | None:
    try:
        headers = {"Authorization": f"Bearer {token}"}
        resp = requests.get(url, params=params, headers=headers)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        print(f"Error fetching JSON from {url}: {e}")
        return None


def get_current_season_id(token: str) -> int:
    region = "us"
    url = f"{API_BASE.format(region=region)}/data/wow/mythic-keystone/season/index"
    params = {"namespace": NAMESPACE_DYNAMIC.format(region=region), "locale": LOCALE}
    data = fetch_json(url, params, token)
    if not data or not data.get("seasons"):
        return None
    return data["current_season"]["id"]


def get_items_for_slot(conn, cursor, spec_id, current_season_id, slot: str):
    item_list = databaseConnector.fetch_top_items_for_slot(
        conn, cursor, spec_id, current_season_id, slot
    )
    item_data = []
    for item, count in item_list:
        bonus_list = databaseConnector.fetch_top_bonus_ids_for_item(
            conn, cursor, spec_id, current_season_id, item
        )
        if bonus_list and bonus_list[0]:
            bonus_ids, bonus_count = bonus_list[0]
        item_data.append(
            {
                "item": item,
                "count": int(count),
                "bonus": {"ids": bonus_ids, "count": int(bonus_count)}
                if bonus_list and bonus_list[0]
                else None,
            }
        )
    return item_data


def get_items_for_slot_group(conn, cursor, spec_id, current_season_id, slot_group: str):
    item_list = databaseConnector.fetch_top_items_for_slot_group(
        conn, cursor, spec_id, current_season_id, slot_group
    )
    item_data = []
    for item, count in item_list:
        bonus_list = databaseConnector.fetch_top_bonus_ids_for_item(
            conn, cursor, spec_id, current_season_id, item
        )
        if bonus_list and bonus_list[0]:
            bonus_ids, bonus_count = bonus_list[0]
        item_data.append(
            {
                "item": item,
                "count": int(count),
                "bonus": {"ids": bonus_ids, "count": int(bonus_count)}
                if bonus_list and bonus_list[0]
                else None,
            }
        )
    return item_data


def get_hero_trees(conn, cursor, spec_id, current_season_id, valid_subtrees=None):
    top_hero_trees = databaseConnector.fetch_hero_tree_overview(
        conn, cursor, spec_id
    )
    overall_hero_trees = []
    for hero_tree_id, count, max_timed_key, max_depleted_key in top_hero_trees:
        # Skip hero trees that don't belong to this spec (e.g. cross-spec
        # contaminated loadouts) so downstream subTrees lookups never miss.
        if valid_subtrees is not None and int(hero_tree_id) not in valid_subtrees:
            print(
                f"WARNING: spec {spec_id} returned hero tree {hero_tree_id} "
                f"(count={count}) not in its subTrees {sorted(valid_subtrees)}; "
                f"skipping contaminated loadout."
            )
            continue
        overall_hero_trees.append({"id": hero_tree_id, "count": int(count), "max_timed_key": int(max_timed_key), "max_depleted_key": int(max_depleted_key)})
    return overall_hero_trees


def get_enchants_for_slot(conn, cursor, spec_id, current_season_id, slot_group):
    top_enchants = databaseConnector.fetch_top_enchant_for_slot(
        conn, cursor, spec_id, current_season_id, slot_group, 10
    )
    overall_enchants = []
    for enchant_item, count, max_timed_key, max_depleted_key in top_enchants:
        overall_enchants.append({"id": enchant_item, "count": int(count), "max_timed_key": int(max_timed_key), "max_depleted_key": int(max_depleted_key)})
    overall_enchants.sort(key=lambda x: x["count"], reverse=True)
    return overall_enchants


def get_sockets(conn, cursor, spec_id, current_season_id):
    top_sockets = databaseConnector.fetch_top_sockets(
        conn, cursor, spec_id, current_season_id
    )
    overall_sockets = []
    for socket, count, max_timed_key, max_depleted_key in top_sockets:
        overall_sockets.append({"id": socket, "count": int(count), "max_timed_key": int(max_timed_key), "max_depleted_key": int(max_depleted_key)})

    return overall_sockets


def get_loadout(conn, cursor, spec_id, current_season_id):
    top_loadouts = databaseConnector.fetch_top_loadout(
        conn, cursor, spec_id, current_season_id
    )
    overall_loadouts = {}
    for hero_talent_id, loadout, count, max_timed_key, max_depleted_key in top_loadouts:
        overall_loadouts[hero_talent_id] = {"loadout": loadout, "count": int(count), "max_timed_key": int(max_timed_key), "max_depleted_key": int(max_depleted_key)}
    return overall_loadouts


def get_loadout_per_dungeon(conn, cursor, spec_id, current_season_id):
    """Most-run talent loadout string per dungeon, keyed {dungeon: {hero_tree: ...}}.

    Same shape as `get_loadout` one level deeper, so the spec page can offer a
    "copy the build for this dungeon" next to the season-wide export.
    """
    rows = databaseConnector.fetch_top_loadout_per_dungeon(
        conn, cursor, spec_id, current_season_id
    )
    per_dungeon = {}
    for dungeon_id, hero_talent_id, loadout, run_count in rows:
        per_dungeon.setdefault(str(dungeon_id), {})[int(hero_talent_id)] = {
            "loadout": loadout,
            "count": int(run_count),
        }
    return per_dungeon


def get_talent_differences(talent_diffs, points_available, valid_talents):
    overall_talent_diffs = {}
    dungeon_counts = {}
    total_count = 0
    talent_counts = {}
    talent_ranks = {}
    dungeon_talent_counts = {}
    for row in talent_diffs:
        hero_talent_id, dungeon, talent_id, count = row[:4]
        avg_rank = row[4] if len(row)>4 else 1.0

        if int(talent_id) not in valid_talents:
            continue
        dungeon_counts[dungeon] = dungeon_counts.get(dungeon, 0) + int(count)
        talent_counts[talent_id] = talent_counts.get(talent_id, 0) + int(count)
        total_count += int(count)
        talent_ranks[talent_id] = float(avg_rank or 1.0)
        dungeon_talent_counts[dungeon] = dungeon_talent_counts.get(dungeon, {})
        dungeon_talent_counts[dungeon][talent_id] = dungeon_talent_counts[dungeon].get(
            talent_id, 0
        ) + int(count)
    overall_talent_diffs["total_count"] = total_count
    data_count = max(talent_counts.values()) if talent_counts else 1
    overall_talent_diffs["data_count"] = data_count
    enriched_talent_counts = []
    for talent, count in talent_counts.items():
        enriched_talent_counts.append(
            {"id": talent, "count": int(count), "pct": (int(count) / data_count) * 100, "avg_rank": talent_ranks.get(talent, 1.0)}
        )
    overall_talent_diffs["overall_dungeon_talents"] = enriched_talent_counts

    enriched_dungeon_talent_counts = {}
    for dungeon, talents in dungeon_talent_counts.items():
        enriched_dungeon_talents = []
        dungeon_data_count = max(talents.values()) if talents else 1
        for talent, count in talents.items():
            enriched_dungeon_talents.append(
                {
                    "id": talent,
                    "count": int(count),
                    "pct": (int(count) / dungeon_data_count) * 100,
                    "avg_rank": talent_ranks.get(talent, 1.0)
                }
            )
        enriched_dungeon_talent_counts[dungeon] = enriched_dungeon_talents
    overall_talent_diffs["dungeon_talent_counts"] = enriched_dungeon_talent_counts
    return overall_talent_diffs


def get_hero_talent_differences(
    conn, cursor, spec_id, current_season_id, valid_talents
):
    spendable_talents = load_existing_json(TALENT_POINTS_PATH)
    top_hero_talent_diffs = databaseConnector.fetch_hero_talents_differences(
        conn, cursor, spec_id, current_season_id
    )
    hero_talent_points_available = spendable_talents.get("hero", 0)

    return get_talent_differences(
        top_hero_talent_diffs, hero_talent_points_available, valid_talents
    )


def get_spec_talent_differences(
    conn, cursor, spec_id, current_season_id, valid_talents, rows=None
):
    spendable_talents = load_existing_json(TALENT_POINTS_PATH)
    if rows is None:
        rows = databaseConnector.fetch_spec_talents_differences(
            conn, cursor, spec_id, current_season_id
        )
    spec_talent_points_available = spendable_talents.get("spec", 0)

    return get_talent_differences(
        rows, spec_talent_points_available, valid_talents
    )


def get_class_talent_differences(
    conn, cursor, spec_id, current_season_id, valid_talents
):
    spendable_talents = load_existing_json(TALENT_POINTS_PATH)
    top_class_talent_diffs = databaseConnector.fetch_class_talents_differences(
        conn, cursor, spec_id, current_season_id
    )
    class_talent_points_available = spendable_talents.get("class", 0)

    return get_talent_differences(
        top_class_talent_diffs, class_talent_points_available, valid_talents
    )


def _by_hero_tree(rows, valid_talents):
    """Partition raw talent-difference rows by hero tree.

    rows: iterable of (hero_talent_id, dungeon, talent_id, count, avg_rank).
    Returns {hero_tree_id: <same dict shape as get_talent_differences>}.

    get_talent_differences already reads hero_talent_id as row[0] and ignores
    it, so running it on a single-tree slice produces tree-relative stats.
    """
    buckets = defaultdict(list)
    for row in rows:
        buckets[int(row[0])].append(row)
    return {
        tid: get_talent_differences(tree_rows, 0, valid_talents)
        for tid, tree_rows in buckets.items()
    }


def get_hero_talent_differences_by_hero_tree(
    conn, cursor, spec_id, current_season_id, valid_talents
):
    return _by_hero_tree(
        databaseConnector.fetch_hero_talents_differences(
            conn, cursor, spec_id, current_season_id
        ),
        valid_talents,
    )


def get_spec_talent_differences_by_hero_tree(
    conn, cursor, spec_id, current_season_id, valid_talents, rows=None
):
    if rows is None:
        rows = databaseConnector.fetch_spec_talents_differences(
            conn, cursor, spec_id, current_season_id
        )
    return _by_hero_tree(rows, valid_talents)


def get_class_talent_differences_by_hero_tree(
    conn, cursor, spec_id, current_season_id, valid_talents
):
    return _by_hero_tree(
        databaseConnector.fetch_class_talents_differences(
            conn, cursor, spec_id, current_season_id
        ),
        valid_talents,
    )


def dungeon_talent_deviations_from_top(
    stats,
    node_ids=None,
    top_n=4,
    min_loadouts=5,
    min_pct_points=10.0,
    recommend_min_pct=50.0,
    drop_max_pct=20.0,
):
    """Per-dungeon talent deviations computed from verified top-player loadouts.

    `stats` is one hero tree's branch of `compute_bis_from_top_loadouts`'s
    `talent_dungeon_stats`:

        {"total": <loadouts>,
         "nodes": {node_id: <loadouts that took it>},
         "dungeons": {"<dungeon_id>": {"total": ..., "nodes": {...}}}}

    Every loadout here is a full, verified talent build tied to one dungeon, so
    a node's share is a true adoption rate: `count / loadouts`. That makes the
    dungeon-vs-overall comparison a plain percentage-POINT difference ("70% of
    the top players take this in Dawnbreaker vs 15% across all dungeons"), and
    percentage points keep rarely-taken talents from dominating the ranking the
    way a relative change off a near-zero baseline does. This replaced a ranking
    built on the general-population aggregation, whose per-dungeon slices come
    from partially sampled loadouts and were too noisy to trust.

    `node_ids` restricts the comparison to renderable nodes. Dungeons with fewer
    than `min_loadouts` loadouts are omitted entirely, and a talent needs to move
    at least `min_pct_points` to be listed at all, so a thin sample yields no
    rows instead of noise.

    A row is called out as a recommendation only where the adoption rate is
    decisive on its own: `take` once a majority (`recommend_min_pct`) of the
    dungeon's top loadouts run it, `drop` once almost none (`drop_max_pct`) do.
    A talent that merely shifted stays an unlabelled row.

    Output: {"<dungeon_id>": {"gains": [...], "losses": [...], "sample": <loadouts>}}
    where each row carries talent_id, dungeon_pct, overall_pct, pct_point_diff,
    dungeon_count, dungeon_total and recommendation ("take"/"drop"/None).
    """
    overall_total = int(stats.get("total", 0) or 0)
    if overall_total <= 0:
        return {}
    overall_nodes = stats.get("nodes", {}) or {}

    results = {}
    for dungeon, dungeon_stats in (stats.get("dungeons") or {}).items():
        dungeon_total = int(dungeon_stats.get("total", 0) or 0)
        if dungeon_total < min_loadouts:
            continue
        dungeon_nodes = dungeon_stats.get("nodes", {}) or {}

        rows = []
        # Union of both sides: a talent dropped entirely for this dungeon has no
        # per-dungeon count but is still the most interesting loss.
        for node_id in set(dungeon_nodes) | set(overall_nodes):
            nid = int(node_id)
            if node_ids is not None and nid not in node_ids:
                continue
            dungeon_count = int(dungeon_nodes.get(node_id, 0))
            dungeon_pct = (dungeon_count / dungeon_total) * 100.0
            overall_pct = (int(overall_nodes.get(node_id, 0)) / overall_total) * 100.0
            pct_point_diff = dungeon_pct - overall_pct
            if abs(pct_point_diff) < min_pct_points:
                continue
            if pct_point_diff > 0:
                recommendation = "take" if dungeon_pct >= recommend_min_pct else None
            else:
                recommendation = "drop" if dungeon_pct <= drop_max_pct else None
            rows.append(
                {
                    "talent_id": nid,
                    "overall_pct": overall_pct,
                    "dungeon_pct": dungeon_pct,
                    "pct_point_diff": pct_point_diff,  # ranking score
                    "dungeon_count": dungeon_count,
                    "dungeon_total": dungeon_total,
                    "recommendation": recommendation,
                }
            )

        gains = sorted(
            (r for r in rows if r["pct_point_diff"] > 0),
            key=lambda r: r["pct_point_diff"],
            reverse=True,
        )[:top_n]
        losses = sorted(
            (r for r in rows if r["pct_point_diff"] < 0),
            key=lambda r: r["pct_point_diff"],
        )[:top_n]
        if not gains and not losses:
            continue
        results[str(dungeon)] = {
            "gains": gains,
            "losses": losses,
            "sample": dungeon_total,
        }

    return results


def get_hero_tree_differences(conn, cursor, spec_id, current_season_id, valid_subtrees=None):
    top_hero_tree_differences = databaseConnector.fetch_hero_tree_differences(
        conn, cursor, spec_id, current_season_id
    )
    overall_counts = {}
    total_count = 0
    dungeon_counts = {}
    data = {}
    for hero_tree, dungeon, count, avg_rank in top_hero_tree_differences:
        # Skip hero trees that don't belong to this spec (e.g. cross-spec
        # contaminated loadouts) so downstream subtree-id lookups never miss.
        if valid_subtrees is not None and int(hero_tree) not in valid_subtrees:
            continue
        overall_counts[hero_tree] = overall_counts.get(hero_tree, 0) + int(count)
        total_count += int(count)
        dungeon_counts[dungeon] = dungeon_counts.get(dungeon, 0) + int(count)
        data[dungeon] = data.get(dungeon, {})
        data[dungeon][hero_tree] = data[dungeon].get(hero_tree, 0) + int(count)

    enriched_data = {}
    for hero_tree in overall_counts:
        enriched_data["overall"] = enriched_data.get("overall", {})
        count = enriched_data["overall"].get(hero_tree, 0) + overall_counts[hero_tree]
        enriched_data["overall"][hero_tree] = {
            "count": count,
            "pct": (count / total_count if total_count > 0 else 1) * 100,
        }
    for dungeon, hero_trees in data.items():
        for hero_tree in hero_trees:
            enriched_data["dungeons"] = enriched_data.get("dungeons", {})
            enriched_data["dungeons"][dungeon] = enriched_data["dungeons"].get(
                dungeon, {}
            )
            count = (
                enriched_data["dungeons"][dungeon].get(hero_tree, 0)
                + hero_trees[hero_tree]
            )
            enriched_data["dungeons"][dungeon][hero_tree] = {
                "count": count,
                "pct": (count / dungeon_counts[dungeon]) * 100
                if dungeon_counts[dungeon] > 0
                else 1,
                "diff": (count / dungeon_counts[dungeon]) * 100
                - enriched_data["overall"][hero_tree]["pct"],
            }

    return enriched_data
