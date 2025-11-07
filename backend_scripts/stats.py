# stats.py
import time
import asyncio
from collections import deque, Counter

class StatsCollector:
    """
    Collect timestamped events. Provide counts over a sliding window (seconds).
    Async-safe.
    """
    def __init__(self, window_seconds: int = 300, simple_queue: asyncio.Queue[tuple] = asyncio.Queue(maxsize=1), advanced_queue: asyncio.Queue[tuple] = asyncio.Queue(maxsize=1), database_queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=1)):
        self.window = window_seconds
        self.events = deque()  # (timestamp, name)
        self.totals = Counter()  # cumulative totals since process start
        self.lock = asyncio.Lock()
        self.queues = {'simple_queue': simple_queue, 'advanced_queue': advanced_queue, 'database_queue': database_queue}

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
            return window_counts, dict(self.totals), {k: q.qsize() for k, q in self.queues.items()}
