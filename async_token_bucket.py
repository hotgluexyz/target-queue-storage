import asyncio
import time


class AsyncTokenBucket:
    """
    Token bucket limiter for asyncio.
    rate: tokens added per second
    capacity: bucket size (burst). Default = rate (1 second of burst).
    """
    def __init__(self, rate: float, capacity: float | None = None):
        self.rate = float(rate)
        self.capacity = float(capacity or rate)
        self._tokens = self.capacity
        self._last = time.monotonic_ns()
        self._lock = asyncio.Lock()

    def _refill(self):
        now = time.monotonic_ns()
        elapsed = (now - self._last) / 1e9  # seconds
        self._last = now
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)

    async def acquire(self, n: float = 1.0):
        # Fast path without loop churn
        self._refill()
        if self._tokens >= n:
            self._tokens -= n
            return

        # Slow path: wait until enough tokens accumulate
        async with self._lock:
            while True:
                self._refill()
                if self._tokens >= n:
                    self._tokens -= n
                    return
                # time until next token(s)
                deficit = n - self._tokens
                wait_s = deficit / self.rate
                # sleep a bit less than needed to reduce oversleep drift
                await asyncio.sleep(max(wait_s, 0.00005))  # ≥50 µs to yield loop