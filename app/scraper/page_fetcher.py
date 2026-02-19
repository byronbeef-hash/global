"""Smart page fetcher: httpx first, Playwright fallback for JS-heavy pages.

Adapted for production: supports proxy rotation via ProxyManager.
"""

import asyncio
import logging
from dataclasses import dataclass

import httpx

from app.utils.rate_limiter import RateLimiter
from app.utils.robots_checker import RobotsChecker
from app.utils.user_agents import get_headers
from app.scraper.proxy_manager import ProxyManager
from app.config import (
    REQUEST_TIMEOUT, MAX_RETRIES, RETRY_BACKOFF,
    PLAYWRIGHT_HEADLESS, PLAYWRIGHT_TIMEOUT,
)

logger = logging.getLogger(__name__)

# Markers that suggest a page needs JS rendering
JS_MARKERS = [
    "window.__NEXT_DATA__",
    "window.__NUXT__",
    '<div id="app"></div>',
    '<div id="root"></div>',
    "React.createElement",
    "ng-app",
    "__GATSBY",
    "Loading...</",
]


@dataclass
class FetchResult:
    url: str
    html: str
    status_code: int
    used_playwright: bool
    success: bool
    error: str | None = None


class PageFetcher:
    """Fetches web pages with optional proxy support.

    Tries httpx first (fast), falls back to Playwright for JS-heavy pages.
    When proxy is available, routes all requests through ScraperAPI.
    """

    def __init__(
        self,
        rate_limiter: RateLimiter,
        robots_checker: RobotsChecker,
        proxy_manager: ProxyManager | None = None,
    ):
        self.rate_limiter = rate_limiter
        self.robots = robots_checker
        self.proxy = proxy_manager or ProxyManager()
        self._playwright = None
        self._browser = None

    async def fetch(self, url: str) -> FetchResult:
        """Fetch a page. Returns FetchResult with HTML content."""
        # Check robots.txt
        if not await self.robots.is_allowed(url):
            logger.info(f"Blocked by robots.txt: {url}")
            return FetchResult(
                url=url, html="", status_code=0, used_playwright=False,
                success=False, error="Blocked by robots.txt",
            )

        # Rate limit (skip if using proxy — proxy handles its own rate limiting)
        if not self.proxy.is_enabled:
            await self.rate_limiter.wait(url)

        # Try httpx first
        result = await self._fetch_httpx(url)

        # Check if page needs JS rendering (offload to thread — BS4 parse is CPU-heavy)
        needs_js = False
        if result.success:
            needs_js = await asyncio.to_thread(self._needs_js_rendering, result.html)

        if result.success and not needs_js:
            return result

        # Fall back to Playwright if httpx failed or page needs JS
        if not result.success or needs_js:
            logger.debug(f"Falling back to Playwright for: {url}")
            if not self.proxy.is_enabled:
                await self.rate_limiter.wait(url)
            pw_result = await self._fetch_playwright(url)
            if pw_result.success:
                return pw_result

        return result

    async def _fetch_httpx(self, url: str, retries: int = 0) -> FetchResult:
        """Fetch using httpx (fast, lightweight)."""
        try:
            # Build client kwargs
            client_kwargs = {
                "timeout": REQUEST_TIMEOUT,
                "follow_redirects": True,
                "headers": get_headers(),
                "http2": True,
            }

            # Add proxy if available
            proxy_url = self.proxy.get_httpx_proxy()
            if proxy_url:
                client_kwargs["proxy"] = proxy_url

            async with httpx.AsyncClient(**client_kwargs) as client:
                resp = await client.get(url)
                if resp.status_code == 200:
                    return FetchResult(
                        url=url, html=resp.text, status_code=resp.status_code,
                        used_playwright=False, success=True,
                    )
                elif resp.status_code == 429 and retries < MAX_RETRIES:
                    wait = RETRY_BACKOFF ** (retries + 1)
                    logger.warning(f"429 from {url}, retrying in {wait}s")
                    await asyncio.sleep(wait)
                    return await self._fetch_httpx(url, retries + 1)
                else:
                    return FetchResult(
                        url=url, html=resp.text, status_code=resp.status_code,
                        used_playwright=False, success=False,
                        error=f"HTTP {resp.status_code}",
                    )
        except Exception as e:
            if retries < MAX_RETRIES:
                wait = RETRY_BACKOFF ** (retries + 1)
                await asyncio.sleep(wait)
                return await self._fetch_httpx(url, retries + 1)
            return FetchResult(
                url=url, html="", status_code=0, used_playwright=False,
                success=False, error=str(e),
            )

    async def _fetch_playwright(self, url: str) -> FetchResult:
        """Fetch using Playwright headless browser for JS rendering."""
        try:
            browser = await self._get_browser()
            page = await browser.new_page()
            try:
                await page.goto(url, timeout=PLAYWRIGHT_TIMEOUT, wait_until="networkidle")
                html = await page.content()
                return FetchResult(
                    url=url, html=html, status_code=200,
                    used_playwright=True, success=True,
                )
            finally:
                await page.close()
        except Exception as e:
            logger.warning(f"Playwright failed for {url}: {e}")
            return FetchResult(
                url=url, html="", status_code=0, used_playwright=True,
                success=False, error=str(e),
            )

    async def _get_browser(self):
        """Lazy-initialize Playwright browser with optional proxy."""
        if self._browser is None:
            from playwright.async_api import async_playwright

            self._playwright = await async_playwright().start()

            launch_kwargs = {"headless": PLAYWRIGHT_HEADLESS}
            proxy_config = self.proxy.get_playwright_proxy()
            if proxy_config:
                launch_kwargs["proxy"] = proxy_config

            self._browser = await self._playwright.chromium.launch(**launch_kwargs)
        return self._browser

    def _needs_js_rendering(self, html: str) -> bool:
        """Check if the HTML suggests the page needs JS to render content."""
        if len(html.strip()) < 500:
            return True
        for marker in JS_MARKERS:
            if marker in html:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(html, "lxml")
                text = soup.get_text(strip=True)
                if len(text) < 200:
                    return True
                break
        return False

    async def close(self) -> None:
        """Clean up Playwright resources."""
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()
