"""Per-user rate limiting. In-memory by default; implement RateLimiter for Redis etc."""

from __future__ import annotations

import threading
import time
from abc import ABC, abstractmethod
from collections import defaultdict, deque

from natquery.errors import RateLimitExceeded


class RateLimiter(ABC):
    @abstractmethod
    def hit(self, key: str, limit: int, window_seconds: int) -> None:
        """Record one event; raise RateLimitExceeded if over *limit* within the window."""


class SlidingWindowLimiter(RateLimiter):
    def __init__(self):
        self._events: dict[str, deque] = defaultdict(deque)
        self._lock = threading.Lock()

    def hit(self, key: str, limit: int, window_seconds: int) -> None:
        now = time.monotonic()
        with self._lock:
            q = self._events[key]
            while q and now - q[0] > window_seconds:
                q.popleft()
            if len(q) >= limit:
                raise RateLimitExceeded(
                    internal=f"{key}: {limit}/{window_seconds}s exceeded",
                    reason="rate_limited",
                )
            q.append(now)
