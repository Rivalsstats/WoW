"""Classification of every table in database.sql into a seeding strategy.

The whole point of this file is the guarantee the task asked for: after the schema is
loaded, ``classify_all()`` walks every base table the DB actually has and **raises on any
table it cannot place**. When someone adds a new raw collector table to database.sql, the
seeder fails loudly here until they register it -- it never silently ships pages built on a
half-populated schema.

Categories:
  REFERENCE  -- lookup / FK-target tables seeded with real values from data/static.
  RAW        -- collector detail tables seeded with randomized-but-plausible data.
  STANDALONE -- read tables the aggregation pipeline does NOT build (top_player_*,
                simc_bis_*, trend_snapshot); seeded directly for full page coverage.
  CONTROL    -- single-purpose control/watermark tables seeded with a minimal row set.
  PIPELINE   -- aggregated_* / global_aggregated_*; left empty here and built by
                CALL sp_run_agg_pipeline() from the seeded RAW tables.
  IGNORE     -- diagnostics/log tables and the *_new / *_old shadow tables the swap
                procedure creates; intentionally left empty.
"""

REFERENCE = "REFERENCE"
RAW = "RAW"
STANDALONE = "STANDALONE"
CONTROL = "CONTROL"
PIPELINE = "PIPELINE"
IGNORE = "IGNORE"


class UnknownTableError(RuntimeError):
    """Raised when database.sql grows a table this seeder doesn't know how to handle."""


REFERENCE_TABLES = {
    "dungeon_data",
    "slot_group_map",
    "season_periods",
    "bloodlust_spells",
    "embellishments",
    "missives",
    "crafted_item_ids",
    "tier_set_items",
}

RAW_TABLES = {
    "runs",
    "members",
    "run_members",
    "equipment",
    "sockets",
    "enchantments",
    "bonus_ids",
    "character_stats",
    "class_talents",
    "spec_talents",
    "hero_talents",
    "route_data",
    "route_pulls",
    "route_specs",
    "pull_enemies",
    "pull_spells",
}

STANDALONE_TABLES = {
    "top_player_loadouts",
    "top_player_loadout_items",
    "top_player_loadout_enchants",
    "top_player_loadout_gems",
    "top_player_loadout_talents",
    "simc_bis_meta",
    "simc_bis_items",
    "simc_bis_progress_meta",
    "simc_bis_progress",
    "trend_snapshot",
}

CONTROL_TABLES = {
    "summary_meta",
    "wipe_control",
}

# Diagnostics / logging tables that are correct empty.
IGNORE_TABLES = {
    "agg_pipeline_log",
    "agg_lock_diag",
}


def classify(table_name):
    """Return the seeding category for one table, or raise UnknownTableError."""
    if table_name in REFERENCE_TABLES:
        return REFERENCE
    if table_name in RAW_TABLES:
        return RAW
    if table_name in STANDALONE_TABLES:
        return STANDALONE
    if table_name in CONTROL_TABLES:
        return CONTROL
    if table_name in IGNORE_TABLES:
        return IGNORE
    if table_name.endswith("_new") or table_name.endswith("_old"):
        return IGNORE  # sp_swap_public_table shadow tables
    if table_name.startswith("aggregated_") or table_name.startswith("global_aggregated_"):
        return PIPELINE
    raise UnknownTableError(
        f"database.sql has a table '{table_name}' that localDev/table_registry.py does not "
        f"know how to seed. Add it to one of the *_TABLES sets (and give it a seeder in "
        f"seeders.py if it is a RAW/REFERENCE/STANDALONE table) so test renders stay complete."
    )


def classify_all(table_names):
    """Classify a full list of tables; raises on the first unknown one.

    Returns a dict category -> sorted list of table names.
    """
    buckets = {REFERENCE: [], RAW: [], STANDALONE: [], CONTROL: [], PIPELINE: [], IGNORE: []}
    for name in table_names:
        buckets[classify(name)].append(name)
    for names in buckets.values():
        names.sort()
    return buckets


# --------------------------------------------------------------------------------------
# Canonical equipment geometry (no static source file exists for slot_group_map).
# Mirrors generateSpecPages.py's LEFT_ORDER + RIGHT_ORDER + WEAPON/TRINKET slots and its
# SLOT_GROUPS tokens, so the aggregates the pipeline builds line up with what the page reads.
# --------------------------------------------------------------------------------------

# The 16 character equipment slots, in gear-overview order.
EQUIPMENT_SLOTS = [
    "HEAD", "NECK", "SHOULDER", "BACK", "CHEST", "WRIST",
    "HANDS", "WAIST", "LEGS", "FEET", "FINGER_1", "FINGER_2",
    "MAIN_HAND", "OFF_HAND", "TRINKET_1", "TRINKET_2",
]

# slot -> slot_group (the slot_group_map reference table). Paired slots collapse to one
# group; weapons collapse to WEAPON. Matches generateSpecPages.SLOT_GROUPS.
SLOT_GROUP_MAP = {
    "HEAD": "HEAD", "NECK": "NECK", "SHOULDER": "SHOULDER", "BACK": "BACK",
    "CHEST": "CHEST", "WRIST": "WRIST", "HANDS": "HANDS", "WAIST": "WAIST",
    "LEGS": "LEGS", "FEET": "FEET",
    "FINGER_1": "FINGER", "FINGER_2": "FINGER",
    "TRINKET_1": "TRINKET", "TRINKET_2": "TRINKET",
    "MAIN_HAND": "WEAPON", "OFF_HAND": "WEAPON",
}

# Blizzard inventoryType values acceptable per slot, used to build per-slot item pools from
# equippable-items.json. Derived from generateSpecPages.INVTYPE_DISPLAY_ORDER.
SLOT_INVENTORY_TYPES = {
    "HEAD": {1}, "NECK": {2}, "SHOULDER": {3}, "BACK": {16}, "CHEST": {5, 20},
    "WRIST": {9}, "HANDS": {10}, "WAIST": {6}, "LEGS": {7}, "FEET": {8},
    "FINGER_1": {11}, "FINGER_2": {11},
    "TRINKET_1": {12}, "TRINKET_2": {12},
    "MAIN_HAND": {13, 15, 17, 21, 26}, "OFF_HAND": {14, 22, 23},
}

# Slots that carry an enchant (feeds the enchant aggregates). Weapon enchant lives on
# MAIN_HAND; rings and the usual armor pieces are enchantable in modern WoW.
ENCHANTABLE_SLOTS = {
    "BACK", "CHEST", "WRIST", "LEGS", "FEET", "FINGER_1", "FINGER_2", "MAIN_HAND",
}

# Slots that can carry a gem socket (feeds sockets + gem comps).
SOCKETED_SLOTS = {"NECK", "FINGER_1", "FINGER_2", "WAIST"}
