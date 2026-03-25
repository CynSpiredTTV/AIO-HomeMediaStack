"""Tests for the RD rate limiter."""

import asyncio
import time

import pytest
import pytest_asyncio

from debridbridge.ratelimit.limiter import BACKOFF_429_SECONDS, Priority, RDRateLimiter


@pytest.fixture
def limiter():
    return RDRateLimiter(max_tokens_per_minute=10, db=None)


@pytest.mark.asyncio
async def test_acquire_single_token(limiter):
    """Basic acquire should succeed immediately."""
    await limiter.acquire(tokens=1, priority=Priority.NORMAL)
    assert limiter.tokens_used == 1


@pytest.mark.asyncio
async def test_acquire_multiple_tokens(limiter):
    """Multiple acquires should accumulate."""
    for _ in range(5):
        await limiter.acquire(tokens=1, priority=Priority.NORMAL)
    assert limiter.tokens_used == 5


@pytest.mark.asyncio
async def test_global_rate_limit_blocks():
    """When global budget is exhausted, acquire should block."""
    limiter = RDRateLimiter(max_tokens_per_minute=3, db=None)

    # Use up all tokens
    for _ in range(3):
        await limiter.acquire(tokens=1, priority=Priority.NORMAL)

    assert limiter.tokens_used == 3

    # Next acquire should block — test with a timeout
    acquired = False

    async def try_acquire():
        nonlocal acquired
        await limiter.acquire(tokens=1, priority=Priority.NORMAL)
        acquired = True

    task = asyncio.create_task(try_acquire())
    await asyncio.sleep(0.3)  # Give it time to attempt
    assert not acquired, "Should have blocked due to rate limit"
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_priority_budget_normal():
    """NORMAL priority should be capped at its tier budget."""
    # 10 total, NORMAL gets 160/200 * 10 = 8 proportionally, but the actual
    # budget is hardcoded: NORMAL=160, REPAIR=40. With max_tokens=200,
    # NORMAL can use up to 160.
    limiter = RDRateLimiter(max_tokens_per_minute=200, db=None)

    # Use up 160 NORMAL tokens
    for _ in range(160):
        await limiter.acquire(tokens=1, priority=Priority.NORMAL)

    assert limiter.tokens_used == 160

    # 161st NORMAL should block
    acquired = False

    async def try_acquire():
        nonlocal acquired
        await limiter.acquire(tokens=1, priority=Priority.NORMAL)
        acquired = True

    task = asyncio.create_task(try_acquire())
    await asyncio.sleep(0.3)
    assert not acquired, "NORMAL should be blocked at tier budget"
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_priority_budget_repair():
    """REPAIR priority should be capped at 40 tokens/min."""
    limiter = RDRateLimiter(max_tokens_per_minute=200, db=None)

    for _ in range(40):
        await limiter.acquire(tokens=1, priority=Priority.REPAIR)

    # 41st REPAIR should block
    acquired = False

    async def try_acquire():
        nonlocal acquired
        await limiter.acquire(tokens=1, priority=Priority.REPAIR)
        acquired = True

    task = asyncio.create_task(try_acquire())
    await asyncio.sleep(0.3)
    assert not acquired, "REPAIR should be blocked at tier budget"
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_critical_bypasses_limit():
    """CRITICAL priority should never be rate-limited."""
    limiter = RDRateLimiter(max_tokens_per_minute=5, db=None)

    # Exhaust the global budget
    for _ in range(5):
        await limiter.acquire(tokens=1, priority=Priority.NORMAL)

    # CRITICAL should still succeed
    await limiter.acquire(tokens=1, priority=Priority.CRITICAL)
    assert limiter.tokens_used == 6  # Over budget, but CRITICAL is exempt


@pytest.mark.asyncio
async def test_tokens_expire_after_window():
    """Tokens older than 60 seconds should be pruned."""
    limiter = RDRateLimiter(max_tokens_per_minute=2, db=None)

    # Manually insert an old entry
    old_time = time.time() - 61
    limiter._deque.append((old_time, 1, Priority.NORMAL))
    assert limiter.tokens_used == 0  # Should be pruned on access

    # Should be able to acquire the full budget
    await limiter.acquire(tokens=1, priority=Priority.NORMAL)
    await limiter.acquire(tokens=1, priority=Priority.NORMAL)
    assert limiter.tokens_used == 2


@pytest.mark.asyncio
async def test_on_429_drains_window():
    """A 429 response should sleep then clear the deque."""
    import unittest.mock

    limiter = RDRateLimiter(max_tokens_per_minute=200, db=None)

    for _ in range(100):
        await limiter.acquire(tokens=1, priority=Priority.NORMAL)

    assert limiter.tokens_used == 100

    # Patch asyncio.sleep to avoid waiting 30s in tests
    sleep_called_with = None
    original_sleep = asyncio.sleep

    async def mock_sleep(seconds):
        nonlocal sleep_called_with
        sleep_called_with = seconds
        # Don't actually sleep

    with unittest.mock.patch("debridbridge.ratelimit.limiter.asyncio.sleep", side_effect=mock_sleep):
        await limiter.on_429()

    # Verify sleep was called with BACKOFF_429_SECONDS
    assert sleep_called_with == BACKOFF_429_SECONDS
    # Deque should be drained after on_429
    assert limiter.tokens_used == 0


@pytest.mark.asyncio
async def test_concurrent_acquires():
    """Multiple concurrent acquires should not exceed budget."""
    limiter = RDRateLimiter(max_tokens_per_minute=10, db=None)

    async def acquire_one():
        await limiter.acquire(tokens=1, priority=Priority.NORMAL)

    # Launch 10 concurrent acquires
    tasks = [asyncio.create_task(acquire_one()) for _ in range(10)]
    await asyncio.gather(*tasks)

    assert limiter.tokens_used == 10
