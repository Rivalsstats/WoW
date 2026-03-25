import json
import os
from itertools import chain
import databaseConnector
from contextlib import closing

databaseConnector.init_connection_pool(
    os.environ.get("DATABASE_HOST"),
    os.environ.get("DATABASE_USER"),
    os.environ.get("DATABASE_PASSWORD"),
    os.environ.get("DATABASE_NAME"),
    os.environ.get("DATABASE_PORT"),
    1,
)

CRAFTING_JSON = "data/static/crafting.json"
OUT_DIR = "data/static"
BONUSES_JSON = "data/static/bonuses.json"

# slot names → output filenames
SLOTS = {
    "Add Embellishment": "embellishments.json",
    "Customize Secondary Stats": "missives.json",
}


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def gather_item_ids(crafting, slot_name):
    slots = crafting.get("slots", {}).values()
    matching = [s for s in slots if s.get("name") == slot_name]
    # Blizzard payloads use reagentIds for slot members; keep itemIds fallback.
    return set(
        chain.from_iterable(
            (s.get("reagentIds") or s.get("itemIds") or []) for s in matching
        )
    )


def index_reagents_by_id(crafting, item_ids):
    reagents = crafting.get("reagents", [])
    by_id = {r["id"]: r for r in reagents}
    return {iid: by_id[iid] for iid in item_ids if iid in by_id}


def compute_bonus_freq(reagents):
    freq = {}
    for r in reagents.values():
        for b in r.get("craftingBonusIds", []):
            freq[b] = freq.get(b, 0) + 1
    return freq


def pick_best_reagent_for_bonus(reagents, bonus_id):
    """
    Among all reagents that include bonus_id, return the itemId
    of the one with highest craftingQuality, then itemLevel, then id.
    """
    candidates = [
        r for r in reagents.values() if bonus_id in r.get("craftingBonusIds", [])
    ]
    best = max(
        candidates,
        key=lambda r: (r.get("craftingQuality", 0), r.get("itemLevel", 0), r["id"]),
    )
    return best["id"]


def build_lookup_for_slot(crafting, slot_name):
    item_ids = gather_item_ids(crafting, slot_name)
    reagents = index_reagents_by_id(crafting, item_ids)
    bonus_freq = compute_bonus_freq(reagents)

    lookup = {}
    for iid, r in reagents.items():
        bonuses = r.get("craftingBonusIds", [])
        # find unique bonus_ids (freq == 1)
        uniques = [b for b in bonuses if bonus_freq.get(b, 0) == 1]

        if uniques:
            # map each unique bonus → this item
            for b in uniques:
                lookup[b] = iid
        else:
            # pick the bonus with smallest freq (then lowest bonus ID)
            least = min(bonuses, key=lambda b: (bonus_freq.get(b, 0), b))
            # among all sharers of that bonus, pick best by quality→level→id
            chosen_iid = pick_best_reagent_for_bonus(reagents, least)
            lookup[least] = chosen_iid

    return lookup


def build_bonus_quality_map(bonuses):
    """
    Build a dict of bonusId → quality for all entries that have a quality field.
    """
    quality_map = {}
    for bid_str, info in bonuses.items():
        if "quality" in info:
            try:
                bid = int(bid_str)
            except ValueError:
                continue
            quality_map[bid] = info["quality"]
    return quality_map


def write_json(data, out_path):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)


def main():
    crafting = load_json(CRAFTING_JSON)
    os.makedirs(OUT_DIR, exist_ok=True)
    with closing(databaseConnector.get_connection()) as conn:
        cursor = conn.cursor()

        for slot_name, out_file in SLOTS.items():
            lookup = build_lookup_for_slot(crafting, slot_name)
            if out_file == "embellishments.json":
                for bonus_id, item_id in lookup.items():
                    databaseConnector.insert_embellishment(
                        conn, cursor, bonus_id, item_id
                    )
            elif out_file == "missives.json":
                for bonus_id, item_id in lookup.items():
                    databaseConnector.insert_missive(conn, cursor, bonus_id, item_id)

            out_path = os.path.join(OUT_DIR, out_file)
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(lookup, f, indent=2, sort_keys=True)
            print(f"Wrote {len(lookup)} mappings for “{slot_name}” → {out_path}")
        databaseConnector.commit_changes(conn)
        bonuses = load_json(BONUSES_JSON)
        quality_map = build_bonus_quality_map(bonuses)
        bonus_map_path = os.path.join(OUT_DIR, "bonus_quality_map.json")
        write_json(quality_map, bonus_map_path)
        print(
            f"Wrote bonus-quality map with {len(quality_map)} entries to {bonus_map_path}"
        )


if __name__ == "__main__":
    main()
