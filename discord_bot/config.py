"""Environment parsing, static constants and the nightly cache-cycle key.

Secrets and tuning come from environment variables (populated from ``.env`` in
the container, or the local shell). Season identity comes from the baked
``data/static/seasonInfo.json`` — the same file the page generators read — so the
bot never needs to hit the Blizzard API to know which season it is serving.
"""

import json
import os
from datetime import datetime, timedelta, timezone

SITE_BASE = "https://mythistone.com"

REQUIRED_ENV = [
    "DISCORD_BOT_TOKEN",
    "DATABASE_HOST",
    "DATABASE_USER",
    "DATABASE_PASSWORD",
    "DATABASE_NAME",
    "DATABASE_PORT",
]

# CWD-relative, matching commonUtils.LOOKUP_DIR. The bot must run with its
# working directory at the repo root (locally) or /app (container).
STATIC_DIR = "data/static"


def _clean(value):
    """Strip trailing CR/whitespace that Windows-edited .env files leave behind."""
    if value is None:
        return None
    return value.strip().strip("\r\n").strip()


def load_env() -> dict:
    """Return the required + optional configuration as a dict.

    Raises RuntimeError listing every missing required variable. entrypoint_bot.sh
    preflights the same list, so in the container this is a belt-and-suspenders
    check; locally it is the only one. When DISCORD_BOT_TOKEN is absent we first
    try a .env file via python-dotenv so `python -m discord_bot` works from a
    checkout without exporting anything.
    """
    if not os.environ.get("DISCORD_BOT_TOKEN"):
        try:
            from dotenv import load_dotenv

            load_dotenv()
        except Exception:
            # python-dotenv is optional locally; the missing-var check below is
            # what actually enforces configuration.
            pass

    env = {}
    missing = []
    for key in REQUIRED_ENV:
        val = _clean(os.environ.get(key))
        if not val:
            missing.append(key)
        env[key] = val
    if missing:
        raise RuntimeError(
            "Missing required environment variables: " + ", ".join(missing)
        )

    env["WEBHOOK_URL"] = _clean(os.environ.get("WEBHOOK_URL"))
    env["BOT_DB_POOL_SIZE"] = int(_clean(os.environ.get("BOT_DB_POOL_SIZE")) or 5)
    env["BOT_DATA_ROLLOVER_UTC_HOUR"] = int(
        _clean(os.environ.get("BOT_DATA_ROLLOVER_UTC_HOUR")) or 22
    )
    env["BOT_FORCE_SYNC"] = (_clean(os.environ.get("BOT_FORCE_SYNC")) or "0") == "1"
    return env


def load_season_info() -> dict:
    path = os.path.join(STATIC_DIR, "seasonInfo.json")
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


SEASON_INFO = load_season_info()
SEASON = int(SEASON_INFO["blizzard_season_id"])
SEASON_SLUG = SEASON_INFO["slug"]
SEASON_NAME = SEASON_INFO.get("name", SEASON_SLUG)
SEASON_SHORT = SEASON_INFO.get("short_name", "")

# Disk locations (relative to CWD). bot_cache is a docker volume in production.
CHART_CACHE_DIR = os.path.join("data", "bot_cache", "charts")
TREE_HASH_FILE = os.path.join("data", "bot_cache", "tree_hash.txt")


def _rollover_hour() -> int:
    return int(_clean(os.environ.get("BOT_DATA_ROLLOVER_UTC_HOUR")) or 22)


def data_date(now: datetime | None = None) -> str:
    """A YYYYMMDD key that rolls over once the nightly aggregation pipeline has
    run, so a rendered chart cached under this key stays valid for the whole day.

    The pipeline runs at 20:00 server time; the rollover hour (UTC) defaults to
    22 to sit safely after it plus buffer. Charts rendered before the rollover
    share the previous day's key.
    """
    now = now or datetime.now(timezone.utc)
    shifted = now - timedelta(hours=_rollover_hour())
    return shifted.strftime("%Y%m%d")


# --- site JSON artifact map ------------------------------------------------
# name -> (url, ttl_seconds). spec_meta is parameterised by spec id, see below.
ARTIFACTS = {
    "comps_index": (SITE_BASE + "/assets/json/comps_index.json", 6 * 3600),
    "comp_routes": (SITE_BASE + "/assets/json/compRoutes.json", 6 * 3600),
    "items_index": (SITE_BASE + "/assets/json/items_index.json", 24 * 3600),
    "gem_enchant_index": (SITE_BASE + "/assets/json/gem_enchant_index.json", 24 * 3600),
    "simdps_tierlist": (SITE_BASE + "/assets/json/simdps_tierlist.json", 6 * 3600),
}
SPEC_META_TTL = 6 * 3600


def spec_meta_url(spec_id) -> str:
    return f"{SITE_BASE}/assets/json/spec_meta/{spec_id}.json"


def artifact_url_ttl(name: str) -> tuple[str, int]:
    """Resolve an artifact name (incl. 'spec_meta/<id>') to (url, ttl_seconds)."""
    if name.startswith("spec_meta/"):
        spec_id = name.split("/", 1)[1]
        return spec_meta_url(spec_id), SPEC_META_TTL
    return ARTIFACTS[name]
