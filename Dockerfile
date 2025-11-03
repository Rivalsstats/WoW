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
RUN mkdir -p /app \
 && cp -f backend_scripts/collectLeaderboardData.py /app/ \
 && cp -f data/static/dungeons.json /app/ || true

WORKDIR /app

# copy entrypoint which manages pulls, webhooks and restarts
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

RUN pip install --no-cache-dir aiohttp aiolimiter mysql-connector-python  aiohttp_retry || true

ENTRYPOINT ["/entrypoint.sh"]
CMD ["python","/app/collectLeaderboardData.py"]
