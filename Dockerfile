FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    APP_DIR=/app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR ${APP_DIR}

# copy only required runtime files into the image
COPY backend_scripts/collectLeaderboardData.py ${APP_DIR}/collectLeaderboardData.py
COPY backend_scripts/stats.py ${APP_DIR}/stats.py
COPY backend_scripts/discordHandler.py ${APP_DIR}/discordHandler.py
COPY backend_scripts/databaseConnector.py ${APP_DIR}/databaseConnector.py
COPY backend_scripts/simcBis.py ${APP_DIR}/simcBis.py
# Shared dependency-light helpers. simcBis.py imports DUAL_WIELD_TWOHAND_SPECS
# from here (the Titan's Grip exception to the "2H main hand => no off-hand"
# rule) so the page generators and the BiS collector can never disagree on it.
COPY backend_scripts/commonUtils.py ${APP_DIR}/commonUtils.py

RUN mkdir -p ${APP_DIR}/data/static
COPY data/static/dungeons.json ${APP_DIR}/data/static/dungeons.json
COPY data/static/specs.json ${APP_DIR}/data/static/specs.json
COPY data/static/talents.json ${APP_DIR}/data/static/talents.json
COPY data/static/classes.json ${APP_DIR}/data/static/classes.json
# equippable-items.json provides inventoryType + itemSetId for dynamic tier-set
# detection in the SimulationCraft BiS collector (simcBis.py).
COPY data/static/equippable-items.json ${APP_DIR}/data/static/equippable-items.json
# embellishments.json maps embellishment bonus_id -> reagent; simcBis.py needs it
# to enforce the <=2 embellishment equip cap. Without it the cap is silently
# disabled (every set treated as 0 embellishments) so illegal over-embellished
# combos bloat the profileset count and skew the BiS results.
COPY data/static/embellishments.json ${APP_DIR}/data/static/embellishments.json
# seasonInfo.json provides the derived max_character_level used by simcBis.py.
COPY data/static/seasonInfo.json ${APP_DIR}/data/static/seasonInfo.json
# crafting.json carries each embellishment reagent's real itemLimit, which is
# what the <=2 cap above is actually enforced from; simcBis.py raises without it.
COPY data/static/crafting.json ${APP_DIR}/data/static/crafting.json
# bonuses.json (socket counts per bonus_id) and enchantments.json (valid enchant
# ids + gem itemLimitCategory) are what let simcBis.py put enchants and gems on
# its candidates. Their loaders swallow a missing file, so without these the sims
# run bare — deflating baseline_dps and with it the cross-spec tierlist.
COPY data/static/bonuses.json ${APP_DIR}/data/static/bonuses.json
COPY data/static/enchantments.json ${APP_DIR}/data/static/enchantments.json
# item-sets.json is the curated live tier-set catalog simcBis.py resolves tier
# combos against (commonUtils.load_tier_sets). Without it the collector falls back
# to equippable-items.json's raw itemSetId; baking it keeps tier-set membership and
# names identical to the spec page. NOTE: load_tier_sets reads it via os.path.join,
# which verifyImageImports.py does NOT detect (it only tracks STATIC_DIR / "x.json"
# literals), so this COPY is not build-guarded -- keep it by hand.
COPY data/static/item-sets.json ${APP_DIR}/data/static/item-sets.json

# entrypoint and executable
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Build-time guard against a module being imported but never COPYed above (kept
# in the image: entrypoint.sh re-runs it as a startup preflight).
COPY backend_scripts/verifyImageImports.py ${APP_DIR}/verifyImageImports.py

# python deps
RUN pip install --no-cache-dir \
    aiohttp \
    aiohttp_retry \
    aiolimiter \
    python-dotenv \
    mysql-connector-python \
    aiomysql \
    pymysql \
    requests \
    discord.py \
    docker

# Runs last, after pip install, so installed packages resolve. Fails the build
# if any module in /app imports something the image doesn't ship.
RUN python ${APP_DIR}/verifyImageImports.py ${APP_DIR}

ENTRYPOINT ["/entrypoint.sh"]
