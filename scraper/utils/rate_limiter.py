"""Rate limiter utilities: simple delay + sliding-window RPM limiter."""

from __future__ import annotations

import time
import random
import threading
from collections import deque
from dataclasses import dataclass, field


@dataclass
class SimpleDelayLimiter:
    """Sleep `delay ± jitter` seconds between calls."""

    delay: float = 1.0
    jitter: float = 0.3

    def wait(self) -> None:
        jitter_offset = random.uniform(-self.jitter, self.jitter)
        sleep_time = max(0.0, self.delay + jitter_offset)
        time.sleep(sleep_time)


@dataclass
class SlidingWindowLimiter:
    """Enforce a maximum number of requests within a rolling time window.

    Example: max_calls=60, window_seconds=60 → max 60 req/min.
    """

    max_calls: int = 60
    window_seconds: float = 60.0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False, compare=False)
    _timestamps: deque = field(default_factory=deque, repr=False, compare=False)

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            # Evict timestamps outside the window
            while self._timestamps and self._timestamps[0] < now - self.window_seconds:
                self._timestamps.popleft()

            if len(self._timestamps) >= self.max_calls:
                # Sleep until the oldest timestamp falls outside the window
                sleep_until = self._timestamps[0] + self.window_seconds
                sleep_time = sleep_until - now
                if sleep_time > 0:
                    time.sleep(sleep_time)

            self._timestamps.append(time.monotonic())


def make_rate_limiter(
    delay: float = 1.0,
    jitter: float = 0.3,
    rpm: int | None = None,
) -> SimpleDelayLimiter | SlidingWindowLimiter:
    """Factory: returns a SlidingWindowLimiter if rpm is set, else SimpleDelayLimiter."""
    if rpm is not None:
        return SlidingWindowLimiter(max_calls=rpm, window_seconds=60.0)
    return SimpleDelayLimiter(delay=delay, jitter=jitter)
