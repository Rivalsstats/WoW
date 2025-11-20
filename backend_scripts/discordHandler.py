import os
import json
import asyncio
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional
import discord
import shutil

STATUS_FILE = Path("data/discord_status.json")


def _format_duration(seconds: int) -> str:
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    if days:
        return f"{days}d {hours:02}:{minutes:02}:{secs:02}"
    return f"{hours:02}:{minutes:02}:{secs:02}"


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

        if b is None:
            raise TypeError(
                "DiscordReporter requires two positional args: stats_collector and session (order flexible). Use named args to be explicit."
            )
        if hasattr(a, "snapshot") and callable(getattr(a, "snapshot")):
            stats = a
            session = b
        elif hasattr(b, "snapshot") and callable(getattr(b, "snapshot")):
            stats = b
            session = a
        else:
            stats = a
            session = b

        if not hasattr(session, "request") and not hasattr(session, "get"):
            raise TypeError(
                "Provided session does not look like an aiohttp session. Pass the aiohttp ClientSession / RetryClient as the 'session' argument."
            )

        self.stats = stats
        self.session = session
        self.interval = interval_seconds

        self.startTime = datetime.now(timezone.utc)
        # config
        self.webhook_url = os.getenv("WEBHOOK_URL")
        self.bot_token = os.getenv("WEBHOOK_TOKEN")
        self.channel_id = os.getenv("WEBHOOK_CHANNEL")
        self._low_space_warn_sent: dict[str, bool] = {}
        self._disk_warn_pct = float(os.getenv("DISK_WARN_PCT", "5.0"))  # percent free
        self._disk_warn_bytes = int(
            os.getenv("DISK_WARN_BYTES", str(1 * 1024**3))
        )  # bytes

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

    def _disk_usage_info(self, path: str | None = None):
        """Return (mount, total, used, free, free_pct, total_hr, free_hr)."""
        try:
            root = Path(path or Path.cwd().anchor or "/")
            du = shutil.disk_usage(str(root))
            total, used, free = du.total, du.used, du.free
            free_pct = (free / total) * 100 if total else 0.0

            def _hr(n: int) -> str:
                suf = ("B", "KB", "MB", "GB", "TB")
                f = float(n)
                i = 0
                while f >= 1024.0 and i < len(suf) - 1:
                    f /= 1024.0
                    i += 1
                return f"{f:.1f}{suf[i]}"

            return str(root), total, used, free, free_pct, _hr(total), _hr(free)
        except Exception:
            return "/", 0, 0, 0, 0.0, "0B", "0B"

    def _iter_mounts(self) -> list[str]:
        """
        Read /proc/mounts and return a deduped list of real mountpoints
        (skipping pseudo filesystems). Order is preserved (will be re-ordered
        later to prefer the app data mount).
        """
        mounts = []
        try:
            with open("/proc/mounts", "r", encoding="utf-8") as f:
                for line in f:
                    parts = line.split()
                    if len(parts) < 3:
                        continue
                    device, mnt, fstype = parts[0], parts[1], parts[2]
                    if fstype in (
                        "proc",
                        "sysfs",
                        "tmpfs",
                        "devtmpfs",
                        "devpts",
                        "overlay",
                        "securityfs",
                        "pstore",
                        "efivarfs",
                        "mqueue",
                        "hugetlbfs",
                        "tracefs",
                        "configfs",
                        "cgroup",
                        "cgroup2",
                    ):
                        continue
                    if not mnt.startswith("/"):
                        continue
                    mounts.append(mnt)
        except Exception:
            mounts = ["/"]
        # dedupe while preserving order
        seen = set()
        out = []
        for m in mounts:
            if m in seen:
                continue
            seen.add(m)
            out.append(m)
        return out

    def _collect_disks_info(self) -> list[dict]:
        """
        Return list of disk dicts. First entry is the filesystem *mountpoint*
        containing STATUS_FILE.parent (the app data dir). Then include other mounts
        discovered. Works on Linux and Windows. Falls back to root if nothing detected.
        """
        out: list[dict] = []

        # preferred app path
        try:
            app_path = STATUS_FILE.parent.resolve()
        except Exception:
            app_path = Path("/").resolve()

        # helper to humanize bytes
        def _hr(n: int) -> str:
            suf = ("B", "KB", "MB", "GB", "TB")
            f = float(n)
            i = 0
            while f >= 1024.0 and i < len(suf) - 1:
                f /= 1024.0
                i += 1
            return f"{f:.1f}{suf[i]}"

        # Get candidate mounts. Prefer using _iter_mounts() which parses /proc/mounts on Linux.
        mounts = []
        try:
            mounts = self._iter_mounts()
        except Exception:
            mounts = ["/"]

        # Determine which mount contains app_path by picking the longest mount that is a prefix.
        app_mount = None
        try:
            ap = str(app_path)
            # sort mounts by length desc so first match is the longest prefix
            for m in sorted(mounts, key=lambda s: -len(s)):
                if ap == m or ap.startswith(m.rstrip("/") + "/") or ap.startswith(m):
                    app_mount = m
                    break
        except Exception:
            app_mount = None

        seen = set()

        # If we found an app_mount, add it first (use it for disk_usage)
        if app_mount:
            try:
                du = shutil.disk_usage(app_mount)
                total, used, free = du.total, du.used, du.free
                free_pct = (free / total) * 100 if total else 0.0
                norm = str(Path(app_mount).resolve())
                seen.add(norm)
                out.append(
                    {
                        "mount": app_mount,
                        "total": total,
                        "used": used,
                        "free": free,
                        "free_pct": free_pct,
                        "total_hr": _hr(total),
                        "free_hr": _hr(free),
                    }
                )
            except Exception:
                # Fall back to using the exact app_path if mount read fails
                try:
                    du = shutil.disk_usage(str(app_path))
                    total, used, free = du.total, du.used, du.free
                    free_pct = (free / total) * 100 if total else 0.0
                    norm = str(app_path)
                    seen.add(norm)
                    out.append(
                        {
                            "mount": str(app_path),
                            "total": total,
                            "used": used,
                            "free": free,
                            "free_pct": free_pct,
                            "total_hr": _hr(total),
                            "free_hr": _hr(free),
                        }
                    )
                except Exception:
                    pass

        # Enumerate all mounts and add remaining (skip pseudo and duplicates)
        for m in mounts:
            try:
                norm = str(Path(m).resolve())
            except Exception:
                norm = str(m)
            if norm in seen:
                continue
            # skip if this mount equals the raw app_path (already added)
            if app_mount and norm == str(Path(app_mount).resolve()):
                continue
            try:
                du = shutil.disk_usage(m)
                total, used, free = du.total, du.used, du.free
                free_pct = (free / total) * 100 if total else 0.0
                out.append(
                    {
                        "mount": m,
                        "total": total,
                        "used": used,
                        "free": free,
                        "free_pct": free_pct,
                        "total_hr": _hr(total),
                        "free_hr": _hr(free),
                    }
                )
                seen.add(norm)
            except Exception:
                # skip unreadable mounts (permissions, fuse mounts, etc.)
                continue

        # Windows fallback if nothing found and running on Windows
        if not out and os.name == "nt":
            for letter in (chr(x) + ":/" for x in range(ord("A"), ord("Z") + 1)):
                if os.path.isdir(letter):
                    try:
                        du = shutil.disk_usage(letter)
                        total, used, free = du.total, du.used, du.free
                        free_pct = (free / total) * 100 if total else 0.0
                        norm = str(Path(letter).resolve())
                        if norm in seen:
                            continue
                        out.append(
                            {
                                "mount": letter,
                                "total": total,
                                "used": used,
                                "free": free,
                                "free_pct": free_pct,
                                "total_hr": _hr(total),
                                "free_hr": _hr(free),
                            }
                        )
                        seen.add(norm)
                    except Exception:
                        continue

        # Final fallback: if nothing at all, use disk usage for root/current
        if not out:
            root, total, used, free, free_pct, total_hr, free_hr = (
                self._disk_usage_info()
            )
            out.append(
                {
                    "mount": root,
                    "total": total,
                    "used": used,
                    "free": free,
                    "free_pct": free_pct,
                    "total_hr": total_hr,
                    "free_hr": free_hr,
                }
            )

        return out

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
            self.status_file.write_text(
                json.dumps({"mode": self.mode, "message_id": self.message_id})
            )
        except Exception:
            pass

    # -------------------------
    # Ensure initial message exists (webhook / bot)
    # -------------------------
    async def _ensure_webhook_message(self):
        # create webhook object using discord.py
        # discord.Webhook.from_url accepts an aiohttp session object for async operations
        try:
            self._webhook = discord.Webhook.from_url(
                self.webhook_url, session=self.session
            )
        except Exception as e:
            # fallback: try basic construction (older versions)
            try:
                self._webhook = discord.Webhook.from_url(
                    self.webhook_url, session=self.session
                )
            except Exception as e:
                print(f"Failed to create webhook object: {e}")
                self._webhook = None
        # try to edit existing message if we have id
        if self._webhook and self.message_id:
            try:
                embed = self._build_embed(probe=True)
                await self._webhook.edit_message(
                    message_id=self.message_id, embed=embed
                )
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
        embed = discord.Embed(
            title=title, description=desc, timestamp=datetime.now(timezone.utc)
        )
        return embed

    # -------------------------
    # Update embed (core)
    # -------------------------
    async def _update_embed(self, probe: bool = False, final: bool = False):
        # get snapshot
        window_counts, totals, queue_sizes = await self.stats.snapshot()
        now = datetime.now(timezone.utc)
        timestamp = now.isoformat()
        epoch = int(datetime.now(timezone.utc).timestamp())

        start_epoch = int(self.startTime.timestamp())
        uptime_seconds = int((now - self.startTime).total_seconds())
        uptime_str = _format_duration(uptime_seconds)
        # build discord.Embed using discord.py classes
        embedlist = []
        embed = discord.Embed(
            title="Collector status",
            description=f"Rolling 5 minute stats. Next update in: <t:{epoch + 300}:R>"
            if not final
            else "Final stats snapshot",
            timestamp=datetime.fromisoformat(timestamp),
            url="https://mythistone.com",
            color=discord.Color.gold(),
        )
        embed.set_thumbnail(
            url="https://mythistone.com/assets/img/favicon/favicon-96x96.png"
        )
        embed.set_footer(
            text="Mythistone Collector",
            icon_url="https://mythistone.com/assets/img/favicon/favicon-96x96.png",
        )
        embedlist.append(embed)

        # fields
        embed.add_field(
            name="Checked Realms",
            value=str(window_counts.get("checked_realm", 0)),
            inline=True,
        )
        embed.add_field(
            name="Checked Runs",
            value=str(window_counts.get("checked_runs", 0)),
            inline=True,
        )
        embed.add_field(
            name="Enqueued Runs",
            value=str(window_counts.get("enqueued_runs", 0)),
            inline=True,
        )
        embed.add_field(
            name="Fetched Profiles",
            value=str(window_counts.get("fetched_profile", 0)),
            inline=True,
        )
        embed.add_field(
            name="Runs", value=str(window_counts.get("db_insert_run", 0)), inline=True
        )
        embed.add_field(
            name="Members",
            value=str(window_counts.get("db_insert_member", 0)),
            inline=True,
        )
        embed.add_field(
            name="No Active Spec",
            value=str(window_counts.get("no_active_spec", 0)),
            inline=True,
        )
        embed.add_field(
            name="Class Talents",
            value=str(window_counts.get("class_talents", 0)),
            inline=True,
        )
        embed.add_field(
            name="Spec Talents",
            value=str(window_counts.get("spec_talents", 0)),
            inline=True,
        )
        embed.add_field(
            name="Hero Talents",
            value=str(window_counts.get("hero_talents", 0)),
            inline=True,
        )
        embed.add_field(
            name="Enchantments",
            value=str(window_counts.get("enchantments", 0)),
            inline=True,
        )
        embed.add_field(
            name="Sockets", value=str(window_counts.get("sockets", 0)), inline=True
        )
        embed.add_field(
            name="Bonuses", value=str(window_counts.get("bonuses", 0)), inline=True
        )
        embed.add_field(
            name="Stats", value=str(window_counts.get("stats", 0)), inline=True
        )
        embed.add_field(
            name="Hunter Pets",
            value=str(window_counts.get("hunter_pets", 0)),
            inline=True,
        )
        embed.add_field(
            name="Simple Queue",
            value=str(queue_sizes.get("simple_queue", 0)),
            inline=True,
        )
        embed.add_field(
            name="Advanced Queue",
            value=str(queue_sizes.get("advanced_queue", 0)),
            inline=True,
        )
        embed.add_field(
            name="Database Queue",
            value=str(queue_sizes.get("database_queue", 0)),
            inline=True,
        )
        embed.add_field(name="Timestamp", value=f"<t:{epoch}:R>", inline=False)

        # totals

        totals_embed = discord.Embed(
            title="Total Values (added since the start)",
            description=f"Started <t:{start_epoch}:R>. Uptime (when last updated): ({uptime_str})."
            if not final
            else f"Was up for ({uptime_str}).",
            timestamp=datetime.fromisoformat(timestamp),
            color=discord.Color.dark_gold(),
        )
        totals_embed.set_thumbnail(
            url="https://mythistone.com/assets/img/favicon/favicon-96x96.png"
        )
        totals_embed.set_footer(
            text="Mythistone Collector",
            icon_url="https://mythistone.com/assets/img/favicon/favicon-96x96.png",
        )

        embedlist.append(totals_embed)
        totals_embed.add_field(
            name="Checked Realms",
            value=str(totals.get("checked_realm", 0)),
            inline=True,
        )
        totals_embed.add_field(
            name="Checked Runs", value=str(totals.get("checked_runs", 0)), inline=True
        )
        totals_embed.add_field(
            name="Enqueued Runs", value=str(totals.get("enqueued_runs", 0)), inline=True
        )
        totals_embed.add_field(
            name="Profiles", value=str(totals.get("fetched_profile", 0)), inline=True
        )
        totals_embed.add_field(
            name="Runs", value=str(totals.get("db_insert_run", 0)), inline=True
        )
        totals_embed.add_field(
            name="Members", value=str(totals.get("db_insert_member", 0)), inline=True
        )
        totals_embed.add_field(
            name="No Active Spec",
            value=str(totals.get("no_active_spec", 0)),
            inline=True,
        )
        totals_embed.add_field(
            name="Class Talents", value=str(totals.get("class_talents", 0)), inline=True
        )
        totals_embed.add_field(
            name="Spec Talents", value=str(totals.get("spec_talents", 0)), inline=True
        )
        totals_embed.add_field(
            name="Hero Talents", value=str(totals.get("hero_talents", 0)), inline=True
        )
        totals_embed.add_field(
            name="Enchantments", value=str(totals.get("enchantments", 0)), inline=True
        )
        totals_embed.add_field(
            name="Sockets", value=str(totals.get("sockets", 0)), inline=True
        )
        totals_embed.add_field(
            name="Bonuses", value=str(totals.get("bonuses", 0)), inline=True
        )
        totals_embed.add_field(
            name="Stats", value=str(totals.get("stats", 0)), inline=True
        )
        totals_embed.add_field(
            name="Hunter Pets", value=str(totals.get("hunter_pets", 0)), inline=True
        )

        # -------------------------
        # Recent console lines (from stats.get_last_lines)
        # -------------------------
        try:
            last_lines = self.stats.get_last_lines(5)
            if last_lines:
                joined = "\n".join(last_lines)
                if len(joined) > 900:
                    joined = "…(truncated)\n" + joined[-900:]
                embed.add_field(
                    name="Recent console", value=f"```{joined}```", inline=False
                )
        except Exception as e:
            try:
                self.stats.console_log(
                    "discordHandler: failed to read recent console lines:", e
                )
            except Exception:
                pass

        # -------------------------
        # Disk usage across mounts
        # -------------------------
        low_mounts = []
        try:
            disks = self._collect_disks_info()
            disk_lines = []
            for d in disks:
                disk_lines.append(
                    f"{d['mount']}: {d['free_hr']} free ({d['free_pct']:.1f}%)"
                )
                if d["total"] and (
                    d["free_pct"] <= self._disk_warn_pct
                    or d["free"] <= self._disk_warn_bytes
                ):
                    low_mounts.append(d)
            disk_text = "\n".join(disk_lines)
            if len(disk_text) > 900:
                disk_text = disk_text[:900] + "\n…"
            embed.add_field(
                name="Disk usage", value=f"```\n{disk_text}\n```", inline=False
            )
        except Exception as e:
            try:
                self.stats.console_log(
                    "discordHandler: failed to collect disk info:", e
                )
            except Exception:
                pass

        # -------------------------
        # Low-disk warning: per-mount one-time messages
        # -------------------------
        if low_mounts:
            # compose warning description
            lines = [
                f"{m['mount']} — {m['free_hr']} free ({m['free_pct']:.1f}%)"
                for m in low_mounts
            ]
            warn_desc = "\n".join(lines)
            warning_embed = discord.Embed(
                title="⚠️ Low disk space",
                description=warn_desc,
                color=discord.Color.red(),
                timestamp=datetime.now(timezone.utc),
            )
            # append the warning embed to the embed list so it appears in the status update
            try:
                embedlist.append(warning_embed)
            except Exception:
                pass

            # send one-time message per mount (webhook or bot)
            for m in low_mounts:
                mount_key = str(Path(m["mount"]).resolve())
                already_sent = bool(self._low_space_warn_sent.get(mount_key))
                if already_sent:
                    continue

                # send as a separate message so it is visible in channel history
                try:
                    if (
                        self.mode == "webhook"
                        and getattr(self, "_webhook", None) is not None
                    ):
                        try:
                            await self._webhook.send(embed=warning_embed, wait=True)
                        except Exception:
                            try:
                                await self._webhook.send(
                                    embeds=[warning_embed], wait=True
                                )
                            except Exception:
                                pass
                    elif self.mode == "bot":
                        try:
                            # channel variable may not be in scope here; fetch it
                            channel = None
                            try:
                                channel = self._client.get_channel(int(self.channel_id))
                                if channel is None:
                                    channel = await self._client.fetch_channel(
                                        int(self.channel_id)
                                    )
                            except Exception:
                                channel = None
                            if channel:
                                await channel.send(embed=warning_embed)
                        except Exception:
                            pass
                except Exception:
                    pass

                # mark this mount as warned
                self._low_space_warn_sent[mount_key] = True
        else:
            # reset any previously-sent flags for mounts that are no longer low
            if self._low_space_warn_sent:
                # create list to avoid mutation during iteration
                for mount in list(self._low_space_warn_sent.keys()):
                    # assume it recovered unless still present in current low_mounts
                    self._low_space_warn_sent.pop(mount, None)

        # send/edit depending on mode
        if self.mode == "webhook":
            if not self._webhook:
                # attempt to create webhook object if missing
                try:
                    self._webhook = discord.Webhook.from_url(
                        self.webhook_url, session=self.session
                    )
                except Exception:
                    self._webhook = None
            if not self._webhook:
                return
            # try edit first if we have message id
            if self.message_id:
                try:
                    await self._webhook.edit_message(
                        message_id=self.message_id, embeds=embedlist
                    )
                    return
                except Exception:
                    # fallback to creating a new webhook message
                    try:
                        msg = await self._webhook.send(embeds=embedlist, wait=True)
                        if msg and getattr(msg, "id", None):
                            self.message_id = int(msg.id)
                            self._persist()
                        return
                    except Exception:
                        return
            else:
                try:
                    msg = await self._webhook.send(embeds=embedlist, wait=True)
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
                    await msg.edit(embeds=embedlist)
                    return
                except Exception:
                    self.message_id = None

            # send new
            try:
                new_msg = await channel.send(embeds=embedlist)
                if new_msg and getattr(new_msg, "id", None):
                    self.message_id = int(new_msg.id)
                    self._persist()
            except Exception:
                return
