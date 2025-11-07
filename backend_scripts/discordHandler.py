# discordHandler.py
import os
import json
import asyncio
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional
import discord

STATUS_FILE = Path("data/discord_status.json")

class DiscordReporter:
    """
    Reports periodic stats in a single embed message using discord.py.
    Supports two modes:
      - WEBHOOK_URL: uses discord.Webhook.from_url + aiohttp session
      - WEBHOOK_TOKEN + WEBHOOK_CHANNEL: uses a discord.Client bot instance

    Usage:
      reporter = DiscordReporter(stats_collector, session, interval_seconds=300)
      await reporter.start()
      ...
      await reporter.stop()
    """
    def __init__(self, a, b=None, interval_seconds: int = 300):
        """
        Accept either:
        DiscordReporter(stats_collector, session, interval_seconds=...)
        or
        DiscordReporter(session, stats_collector, interval_seconds=...)
        or use named args.
        """
        # Resolve args: detect which is stats_collector vs aiohttp session
        stats = None
        session = None

        # If called with named args, Python will have set a and b appropriately.
        if b is None:
            raise TypeError("DiscordReporter requires two positional args: stats_collector and session (order flexible). Use named args to be explicit.")
        # detect by duck-typing
        if hasattr(a, "snapshot") and callable(getattr(a, "snapshot")):
            stats = a
            session = b
        elif hasattr(b, "snapshot") and callable(getattr(b, "snapshot")):
            stats = b
            session = a
        else:
            # neither looks like a StatsCollector; choose sensible defaults
            stats = a
            session = b

        # validate session looks like an aiohttp session
        if not hasattr(session, "request") and not hasattr(session, "get"):
            raise TypeError("Provided session does not look like an aiohttp session. Pass the aiohttp ClientSession / RetryClient as the 'session' argument.")

        self.stats = stats
        self.session = session
        self.interval = interval_seconds

        # config
        self.webhook_url = os.getenv("WEBHOOK_URL")
        self.bot_token = os.getenv("WEBHOOK_TOKEN")
        self.channel_id = os.getenv("WEBHOOK_CHANNEL")

        # mode selection
        if self.webhook_url:
            self.mode = "webhook"
        elif self.bot_token and self.channel_id:
            self.mode = "bot"
        else:
            self.mode = "none"

        # persisted state
        self.status_file = STATUS_FILE
        self.message_id: Optional[int] = None
        self.thread_task: Optional[asyncio.Task] = None

        # bot internals
        self._client: Optional[discord.Client] = None
        self._ready_event = asyncio.Event()
        self._created_message_id: Optional[int] = None

        # webhook object cached (only for webhook mode)
        self._webhook: Optional[discord.Webhook] = None

    # -------------------------
    # Public lifecycle
    # -------------------------
    async def start(self):
        if self.mode == "none":
            return
        # restore persisted message id if possible
        self._load_persisted()
        if self.mode == "webhook":
            await self._ensure_webhook_message()
        else:
            await self._ensure_bot_message()
        # start periodic updater
        self.thread_task = asyncio.create_task(self._periodic())

    async def stop(self):
        # cancel periodic updater
        if self.thread_task:
            self.thread_task.cancel()
            try:
                await self.thread_task
            except asyncio.CancelledError:
                pass
        # final update
        if self.mode == "webhook":
            if self._webhook:
                await self._update_embed(final=True)
        elif self.mode == "bot":
            await self._update_embed(final=True)
            # gracefully close bot client
            if self._client:
                try:
                    await self._client.close()
                except Exception:
                    pass

    # -------------------------
    # Persistence
    # -------------------------
    def _load_persisted(self):
        if not self.status_file.exists():
            return
        try:
            data = json.loads(self.status_file.read_text())
            if data.get("mode") == self.mode and data.get("message_id"):
                self.message_id = int(data["message_id"])
        except Exception:
            self.message_id = None

    def _persist(self):
        try:
            self.status_file.parent.mkdir(parents=True, exist_ok=True)
            self.status_file.write_text(json.dumps({"mode": self.mode, "message_id": self.message_id}))
        except Exception:
            pass

    # -------------------------
    # Ensure initial message exists (webhook / bot)
    # -------------------------
    async def _ensure_webhook_message(self):
        # create webhook object using discord.py
        # discord.Webhook.from_url accepts an aiohttp session object for async operations
        try:
            self._webhook = discord.Webhook.from_url(self.webhook_url, session=self.session)
        except Exception as e:
            # fallback: try basic construction (older versions)
            try:
                self._webhook = discord.Webhook.from_url(self.webhook_url, session=self.session)
            except Exception as e:
                print(f"Failed to create webhook object: {e}")
                self._webhook = None
        # try to edit existing message if we have id
        if self._webhook and self.message_id:
            try:
                embed = self._build_embed(probe=True)
                await self._webhook.edit_message(message_id=self.message_id, embed=embed)
                return
            except Exception:
                self.message_id = None
        # create new message
        if self._webhook:
            embed = self._build_embed()
            # create message; wait=True returns created message
            try:
                msg = await self._webhook.send(embed=embed, wait=True)
                if msg and getattr(msg, "id", None):
                    self.message_id = int(msg.id)
                    self._persist()
            except Exception as e:
                # swallow error but don't crash reporter
                print(f"Failed to create webhook message: {e}")
                pass

    async def _ensure_bot_message(self):
        # create and start a lightweight discord.Client in background
        intents = discord.Intents.default()
        self._client = discord.Client(intents=intents)

        @self._client.event
        async def on_ready():
            self._ready_event.set()

        # start client in background
        # client.start(token) is a coroutine that doesn't return until closed, so we run it in a separate task
        loop = asyncio.get_event_loop()
        self._client_task = asyncio.create_task(self._client.start(self.bot_token))

        # wait up to 30s for ready
        try:
            await asyncio.wait_for(self._ready_event.wait(), timeout=30.0)
        except Exception:
            # if client didn't come up, leave mode disabled
            return

        # get channel and try to edit existing message
        try:
            channel = self._client.get_channel(int(self.channel_id))
            if channel is None:
                # maybe not cached yet, fetch
                channel = await self._client.fetch_channel(int(self.channel_id))
        except Exception:
            channel = None

        if channel and self.message_id:
            try:
                msg = await channel.fetch_message(self.message_id)
                # Try to edit (probe)
                await msg.edit(embed=self._build_embed(probe=True))
                return
            except Exception:
                self.message_id = None

        # create message
        if channel:
            try:
                msg = await channel.send(embed=self._build_embed())
                if msg and getattr(msg, "id", None):
                    self.message_id = int(msg.id)
                    self._persist()
            except Exception:
                pass

    # -------------------------
    # Periodic update loop
    # -------------------------
    async def _periodic(self):
        while True:
            try:
                await self._update_embed()
            except Exception:
                # swallow to avoid killing loop
                pass
            await asyncio.sleep(self.interval)

    # -------------------------
    # Build embed
    # -------------------------
    def _build_embed(self, probe: bool = False, final: bool = False) -> discord.Embed:
        # snapshot is async so caller must get snapshot before calling this if desired.
        # But for webhook and bot both flows call build via _update_embed which obtains snapshot.
        # This function expects to be passed data via closure below; for simplicity we will
        # build in _update_embed and not use this method with live fetching.
        # Keep for compatibility if needed.
        title = "Collector status"
        desc = "Rolling stats"
        embed = discord.Embed(title=title, description=desc, timestamp=datetime.now(timezone.utc))
        return embed

    # -------------------------
    # Update embed (core)
    # -------------------------
    async def _update_embed(self, probe: bool = False, final: bool = False):
        # get snapshot
        window_counts, totals = await self.stats.snapshot()
        timestamp = datetime.now(timezone.utc).isoformat()

        # build discord.Embed using discord.py classes
        embed = discord.Embed(
            title="Collector status",
            description="Rolling 5 minute stats" if not final else "Final stats snapshot",
            timestamp=datetime.fromisoformat(timestamp)
        )

        # fields
        embed.add_field(name="checked_realms (5m)", value=str(window_counts.get("checked_realm", 0)), inline=True)
        embed.add_field(name="checked_runs (5m)", value=str(window_counts.get("checked_runs", 0)), inline=True)
        embed.add_field(name="enqueued_runs (5m)", value=str(window_counts.get("enqueued_runs", 0)), inline=True)
        embed.add_field(name="fetched_profiles (5m)", value=str(window_counts.get("fetched_profile", 0)), inline=True)
        embed.add_field(name="db_runs_inserted (5m)", value=str(window_counts.get("db_insert_run", 0)), inline=True)
        embed.add_field(name="db_members_inserted (5m)", value=str(window_counts.get("db_insert_member", 0)), inline=True)
        embed.add_field(name="timestamp", value=timestamp, inline=False)
        # totals as one field (stringified)
        embed.add_field(name="totals (since start)", value=json.dumps(totals, default=str), inline=False)

        # send/edit depending on mode
        if self.mode == "webhook":
            if not self._webhook:
                # attempt to create webhook object if missing
                try:
                    self._webhook = discord.Webhook.from_url(self.webhook_url, session=self.session)
                except Exception:
                    self._webhook = None
            if not self._webhook:
                return
            # try edit first if we have message id
            if self.message_id:
                try:
                    await self._webhook.edit_message(message_id=self.message_id, embed=embed)
                    return
                except Exception:
                    # fallback to creating a new webhook message
                    try:
                        msg = await self._webhook.send(embed=embed, wait=True)
                        if msg and getattr(msg, "id", None):
                            self.message_id = int(msg.id)
                            self._persist()
                        return
                    except Exception:
                        return
            else:
                try:
                    msg = await self._webhook.send(embed=embed, wait=True)
                    if msg and getattr(msg, "id", None):
                        self.message_id = int(msg.id)
                        self._persist()
                except Exception:
                    return

        elif self.mode == "bot":
            if not self._client or not self._ready_event.is_set():
                return
            try:
                channel = self._client.get_channel(int(self.channel_id))
                if channel is None:
                    channel = await self._client.fetch_channel(int(self.channel_id))
            except Exception:
                channel = None

            if channel is None:
                return

            # edit existing or send new
            if self.message_id:
                try:
                    msg = await channel.fetch_message(self.message_id)
                    await msg.edit(embed=embed)
                    return
                except Exception:
                    self.message_id = None

            # send new
            try:
                new_msg = await channel.send(embed=embed)
                if new_msg and getattr(new_msg, "id", None):
                    self.message_id = int(new_msg.id)
                    self._persist()
            except Exception:
                return
