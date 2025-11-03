FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1

# system deps
RUN apt-get update \
 && apt-get install -y --no-install-recommends git curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# repo clone (keeps .git so container can pull updates)
WORKDIR /opt/repo
RUN git clone https://github.com/MythiStone/mythistone.github.io . \
 && git config --global --add safe.directory /opt/repo

# app folder contains only the runtime files used by the worker
RUN mkdir -p /app

COPY backend_scripts/collectLeaderboardData.py /app/collectLeaderboardData.py
COPY backend_scripts/databaseConnector.py /app/databaseConnector.py

RUN mkdir -p /app/data/static
COPY data/static/dungeons.json /app/data/static/dungeons.json

WORKDIR /app

# copy entrypoint which manages pulls, webhooks and restarts
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

RUN pip install --no-cache-dir \
    aiohttp \
    aiohttp_retry \
    aiolimiter \
    python-dotenv \
    mysql-connector-python \
    aiomysql \
    pymysql

ENTRYPOINT ["/entrypoint.sh"]
CMD ["python","/app/collectLeaderboardData.py"]
