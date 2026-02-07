"""Check robots.txt before scraping a URL."""

import asyncio
import logging
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import httpx

logger = logging.getLogger(__name__)

BOT_USER_AGENT = "CattleScraper/1.0"


class RobotsChecker:
    """Caches and checks robots.txt for each domain."""

    def __init__(self):
        self._parsers: dict[str, RobotFileParser | None] = {}
        self._lock = asyncio.Lock()

    def _get_robots_url(self, url: str) -> str:
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}/robots.txt"

    async def _fetch_robots(self, url: str) -> RobotFileParser | None:
        robots_url = self._get_robots_url(url)
        domain = urlparse(url).netloc

        if domain in self._parsers:
            return self._parsers[domain]

        try:
            async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
                resp = await client.get(robots_url)
                if resp.status_code == 200:
                    parser = RobotFileParser()
                    parser.parse(resp.text.splitlines())
                    self._parsers[domain] = parser
                    return parser
                else:
                    self._parsers[domain] = None
                    return None
        except Exception as e:
            logger.debug(f"Could not fetch robots.txt for {domain}: {e}")
            self._parsers[domain] = None
            return None

    async def is_allowed(self, url: str) -> bool:
        """Check if we're allowed to fetch this URL per robots.txt."""
        async with self._lock:
            parser = await self._fetch_robots(url)

        if parser is None:
            return True

        return parser.can_fetch(BOT_USER_AGENT, url)
