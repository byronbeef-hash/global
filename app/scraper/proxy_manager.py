"""Proxy rotation using ScraperAPI."""

import logging
from urllib.parse import quote

from app.config import SCRAPER_API_KEY, USE_PROXY

logger = logging.getLogger(__name__)


class ProxyManager:
    """Manages proxy rotation via ScraperAPI.

    ScraperAPI works by prepending their API endpoint to the target URL.
    They handle rotation, retries, and CAPTCHA solving on their end.
    """

    SCRAPERAPI_ENDPOINT = "http://api.scraperapi.com"

    def __init__(self):
        self.enabled = USE_PROXY and bool(SCRAPER_API_KEY)
        self.request_count = 0
        if self.enabled:
            logger.info("ProxyManager initialized with ScraperAPI")
        else:
            logger.info("ProxyManager disabled (no SCRAPER_API_KEY)")

    def get_proxy_url(self, target_url: str, render_js: bool = False) -> str:
        """Get the proxied URL for a target.

        Args:
            target_url: The URL to fetch through the proxy.
            render_js: Whether to enable JS rendering on the proxy side.

        Returns:
            The proxied URL string.
        """
        if not self.enabled:
            return target_url

        params = f"api_key={SCRAPER_API_KEY}&url={quote(target_url, safe='')}"
        if render_js:
            params += "&render=true"

        self.request_count += 1
        return f"{self.SCRAPERAPI_ENDPOINT}?{params}"

    def get_httpx_proxy(self) -> str | None:
        """Get proxy URL for httpx client configuration.

        ScraperAPI also supports standard HTTP proxy mode:
        http://scraperapi:{api_key}@proxy-server.scraperapi.com:8001
        """
        if not self.enabled:
            return None
        return f"http://scraperapi:{SCRAPER_API_KEY}@proxy-server.scraperapi.com:8001"

    def get_playwright_proxy(self) -> dict | None:
        """Get proxy config for Playwright browser launch.

        Returns dict compatible with playwright.launch(proxy=...).
        """
        if not self.enabled:
            return None
        return {
            "server": "http://proxy-server.scraperapi.com:8001",
            "username": "scraperapi",
            "password": SCRAPER_API_KEY,
        }

    @property
    def is_enabled(self) -> bool:
        return self.enabled

    @property
    def total_requests(self) -> int:
        return self.request_count
