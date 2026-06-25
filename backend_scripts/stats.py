# stats.py
import time
import asyncio
from collections import deque, Counter
import threading
from datetime import datetime, timezone
from typing import List

_CONSOLE_MAX_LINES = 500
_console_buf = deque(maxlen=_CONSOLE_MAX_LINES)
_console_lock = threading.Lock()


class StatsCollector:
    """
    Collect timestamped events. Provide counts over a sliding window (seconds).
    Async-safe.
    """

    def __init__(
        self,
        window_seconds: int = 300,
        simple_queue: asyncio.Queue[tuple] = asyncio.Queue(maxsize=1),
        advanced_queue: asyncio.Queue[tuple] = asyncio.Queue(maxsize=1),
        database_queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=1),
        route_db_queue: asyncio.Queue[tuple] = asyncio.Queue(maxsize=1),
    ):
        self.window = window_seconds
        self.events = deque()  # (timestamp, name)
        self.totals = Counter()  # cumulative totals since process start
        self.status: dict[str, str] = {}  # free-form gauges/labels (e.g. simc current spec)
        self.lock = asyncio.Lock()
        self.queues = {
            "simple_queue": simple_queue,
            "advanced_queue": advanced_queue,
            "database_queue": database_queue,
            "route_db_queue": route_db_queue,
                }

    async def increment(self, name: str, amount: int = 1):
        ts = time.time()
        async with self.lock:
            for _ in range(amount):
                self.events.append((ts, name))
            self.totals[name] += amount

    async def snapshot(self):
        """
        Return (window_counts: Counter, totals: dict). Also prunes old events.
        """
        cutoff = time.time() - self.window
        async with self.lock:
            while self.events and self.events[0][0] < cutoff:
                self.events.popleft()
            window_counts = Counter(e[1] for e in self.events)
            return (
                window_counts,
                dict(self.totals),
                {k: q.qsize() for k, q in self.queues.items()},
            )

    def set_status(self, key: str, value: str) -> None:
        """Set a free-form status label (synchronous, best-effort).

        Used for non-counter gauges such as the spec SimulationCraft is currently
        simulating. Read back via get_status() when building the Discord embed.
        """
        try:
            self.status[key] = value
        except Exception:
            pass

    def get_status(self, key: str, default: str = "") -> str:
        return self.status.get(key, default)

    def console_log(
        self, *args, sep: str = " ", end: str = "\n", file=None, flush: bool = False
    ) -> None:
        try:
            text = sep.join(str(a) for a in args)
            timestamped = f"[{datetime.now(timezone.utc).isoformat()}] {text}"
            with _console_lock:
                _console_buf.append(timestamped)
        except Exception:
            pass
        print(*args, sep=sep, end=end, file=file, flush=flush)

    def get_last_lines(self, n: int = 5) -> List[str]:
        with _console_lock:
            return list(_console_buf)[-n:]
