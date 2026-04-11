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

RUN mkdir -p ${APP_DIR}/data/static
COPY data/static/dungeons.json ${APP_DIR}/data/static/dungeons.json
COPY data/static/specs.json ${APP_DIR}/data/static/specs.json
COPY data/static/talents.json ${APP_DIR}/data/static/talents.json

# entrypoint and executable
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

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
    discord.py

ENTRYPOINT ["/entrypoint.sh"]
