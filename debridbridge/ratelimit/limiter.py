"""Token bucket rate limiter for Real-Debrid API calls.

Every single RD API call MUST go through this limiter. No exceptions.
"""

import asyncio
import enum
import logging
import time
import uuid
from collections import deque

logger = logging.getLogger(__name__)


class Priority(enum.Enum):
    CRITICAL = "critical"  # Unrestrict on first stream access — not rate-limited but logged
    NORMAL = "normal"      # Imports, cache checks — up to 160 tokens/min
    REPAIR = "repair"      # Background repair worker — capped at 40 tokens/min


# Budget allocation per priority tier (tokens per minute)
PRIORITY_BUDGETS = {
    Priority.CRITICAL: None,  # No limit
    Priority.NORMAL: 160,
    Priority.REPAIR: 40,
}

# How long to sleep on a 429 before retrying
BACKOFF_429_SECONDS = 30


class RDRateLimiter:
    """Token bucket rate limiter shared across all Real-Debrid API calls.

    Uses an in-memory deque for the hot path. The rd_request_log DB table
    is written to asynchronously for monitoring/UI purposes only.
    """

    def __init__(self, max_tokens_per_minute: int = 200, db=None):
        self.max_tokens = max_tokens_per_minute
        self.db = db
        self._lock = asyncio.Lock()
        self._deque: deque[tuple[float, int, Priority]] = deque()

    def _prune(self, now: float):
        """Remove entries older than 60 seconds."""
        cutoff = now - 60.0
        while self._deque and self._deque[0][0] < cutoff:
            self._deque.popleft()

    def _sum_tokens(self, priority: Priority | None = None) -> int:
        """Sum tokens used in the current window, optionally filtered by priority."""
        if priority is None:
            return sum(t[1] for t in self._deque)
        return sum(t[1] for t in self._deque if t[2] == priority)

    @property
    def tokens_used(self) -> int:
        """Current tokens used in the sliding window (for health endpoint).

        Note: reads a snapshot of the deque. Not perfectly atomic but safe
        enough for monitoring — avoids blocking the event loop with a lock.
        """
        now = time.time()
        # Take a snapshot to avoid mutation during iteration (BUG-006 fix)
        snapshot = list(self._deque)
        cutoff = now - 60.0
        return sum(t[1] for t in snapshot if t[0] >= cutoff)

    async def acquire(self, tokens: int = 1, priority: Priority = Priority.NORMAL):
        """Acquire tokens before making an RD API call. Blocks until budget is available.

        Args:
            tokens: Number of tokens to consume (usually 1).
            priority: Priority tier controlling budget allocation.
        """
        while True:
            async with self._lock:
                now = time.time()
                self._prune(now)

                total_used = self._sum_tokens()
                tier_used = self._sum_tokens(priority)
                budget = PRIORITY_BUDGETS.get(priority)

                # CRITICAL priority is never rate-limited (but still logged)
                if priority == Priority.CRITICAL:
                    self._record(now, tokens, priority)
                    return

                # Check global budget
                if total_used + tokens > self.max_tokens:
                    logger.debug(
                        "Global rate limit: %d/%d used, waiting...",
                        total_used, self.max_tokens,
                    )
                    # Fall through to sleep and retry
                elif budget is not None and tier_used + tokens > budget:
                    logger.debug(
                        "Tier %s rate limit: %d/%d used, waiting...",
                        priority.value, tier_used, budget,
                    )
                    # Fall through to sleep and retry
                else:
                    # Budget available — record and return
                    self._record(now, tokens, priority)
                    return

            # Sleep outside the lock so other priorities can proceed
            await asyncio.sleep(0.1)

    def _record(self, now: float, tokens: int, priority: Priority):
        """Record token usage in-memory and asynchronously to DB."""
        self._deque.append((now, tokens, priority))

        if self.db is not None:
            try:
                with self.db.get_db() as conn:
                    conn.execute(
                        "INSERT INTO rd_request_log (id, endpoint, tokens_used, requested_at) "
                        "VALUES (?, ?, ?, ?)",
                        (str(uuid.uuid4()), priority.value, tokens, now),
                    )
            except Exception:
                # DB write failure must never block API calls
                logger.debug("Failed to write to rd_request_log", exc_info=True)

    async def on_429(self):
        """Handle a 429 response from Real-Debrid.

        This is a serious event. Sleep first (back off), then drain the window.
        """
        logger.error(
            "429 TOO MANY REQUESTS from Real-Debrid! "
            "Sleeping %d seconds then draining rate limiter window.",
            BACKOFF_429_SECONDS,
        )
        await asyncio.sleep(BACKOFF_429_SECONDS)
        async with self._lock:
            self._deque.clear()
