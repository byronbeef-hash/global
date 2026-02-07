"""Per-domain async rate limiter using token bucket."""

import asyncio
import time
from urllib.parse import urlparse

from app.config import DEFAULT_RATE_LIMIT, SEARCH_RATE_LIMIT, DIRECTORY_RATE_LIMIT


class RateLimiter:
    """Token-bucket rate limiter, keyed by domain."""

    CUSTOM_LIMITS = {
        "duckduckgo.com": SEARCH_RATE_LIMIT,
        "google.com": SEARCH_RATE_LIMIT,
        "www.google.com": SEARCH_RATE_LIMIT,
        "www.yellowpages.com": DIRECTORY_RATE_LIMIT,
        "www.yelp.com": DIRECTORY_RATE_LIMIT,
        "www.manta.com": DIRECTORY_RATE_LIMIT,
    }

    def __init__(self):
        self._last_request: dict[str, float] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    def _get_domain(self, url: str) -> str:
        return urlparse(url).netloc.lower()

    def _get_delay(self, domain: str) -> float:
        return self.CUSTOM_LIMITS.get(domain, DEFAULT_RATE_LIMIT)

    def _get_lock(self, domain: str) -> asyncio.Lock:
        if domain not in self._locks:
            self._locks[domain] = asyncio.Lock()
        return self._locks[domain]

    async def wait(self, url: str) -> None:
        """Wait until it's safe to make a request to the given URL."""
        domain = self._get_domain(url)
        lock = self._get_lock(domain)

        async with lock:
            delay = self._get_delay(domain)
            last = self._last_request.get(domain, 0)
            elapsed = time.monotonic() - last
            if elapsed < delay:
                await asyncio.sleep(delay - elapsed)
            self._last_request[domain] = time.monotonic()
