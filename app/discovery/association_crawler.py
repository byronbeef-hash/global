"""Crawl cattlemen's association and breed association directories."""

import asyncio
import logging
from urllib.parse import quote_plus, urlparse, urljoin

from bs4 import BeautifulSoup

from app.scraper.page_fetcher import PageFetcher
from app.scraper.contact_extractor import ContactExtractor

logger = logging.getLogger(__name__)

ASSOCIATION_URLS = {
    "American Angus Association": "https://www.angus.org/find-a-breeder",
    "American Hereford Association": "https://hereford.org/find-a-breeder/",
    "American Simmental Association": "https://simmental.org/find-a-breeder",
    "North American Limousin Foundation": "https://nalf.org/find-a-breeder/",
    "American Shorthorn Association": "https://shorthorn.org/find-a-breeder/",
    "American Brahman Breeders Association": "https://brahman.org/find-a-breeder/",
    "Red Angus Association": "https://redangus.org/find-a-breeder/",
    "American Gelbvieh Association": "https://gelbvieh.org/find-a-breeder/",
    "Beefmaster Breeders United": "https://beefmasters.org/find-a-breeder/",
    "Santa Gertrudis Breeders International": "https://santagertrudis.com/find-a-breeder/",
}

STATE_CATTLEMEN_URLS = {
    "Texas": "https://www.texascattleraisers.org",
    "Nebraska": "https://www.necattlemen.org",
    "Kansas": "https://www.kla.org",
    "Oklahoma": "https://www.okcattlemen.org",
    "South Dakota": "https://www.sdcattlemen.org",
    "Montana": "https://www.mtbeef.org",
    "Colorado": "https://www.coloradocattle.org",
    "Iowa": "https://www.iacattlemen.org",
    "Missouri": "https://www.mocattle.com",
    "North Dakota": "https://www.ndstockmen.org",
    "Wyoming": "https://www.wyocattle.org",
    "Idaho": "https://www.idahocattle.org",
    "Florida": "https://www.floridacattlemen.org",
    "California": "https://www.calcattlemen.org",
    "Oregon": "https://www.orcattle.com",
    "Virginia": "https://www.vacattlemen.org",
    "Kentucky": "https://www.kycattle.org",
    "Wisconsin": "https://www.wicattlemen.org",
    "Minnesota": "https://www.mncattle.org",
    "Georgia": "https://www.gabeef.org",
}


class AssociationCrawler:
    """Crawl breed association and state cattlemen's association directories."""

    def __init__(self, fetcher: PageFetcher):
        self.fetcher = fetcher
        self.contact_extractor = ContactExtractor()

    async def crawl_breed_associations(self) -> list[str]:
        """Crawl breed association 'Find a Breeder' pages."""
        all_urls = []

        for name, url in ASSOCIATION_URLS.items():
            logger.info(f"Crawling {name}: {url}")
            result = await self.fetcher.fetch(url)
            if not result.success:
                logger.warning(f"Failed to fetch {name}: {result.error}")
                continue

            urls = self._extract_breeder_links(result.html, url)
            all_urls.extend(urls)
            logger.info(f"{name}: found {len(urls)} breeder links")
            await asyncio.sleep(3)

        return all_urls

    async def crawl_state_associations(self, states: list[str] | None = None) -> list[str]:
        """Crawl state cattlemen's association sites for member directories."""
        all_urls = []
        target_urls = STATE_CATTLEMEN_URLS
        if states:
            target_urls = {s: u for s, u in STATE_CATTLEMEN_URLS.items() if s in states}

        for state, base_url in target_urls.items():
            for path in ["/members", "/directory", "/member-directory",
                         "/find-a-member", "/ranchers", "/producers"]:
                url = base_url.rstrip("/") + path
                result = await self.fetcher.fetch(url)
                if result.success and len(result.html) > 1000:
                    urls = self._extract_member_links(result.html, url)
                    if urls:
                        all_urls.extend(urls)
                        logger.info(f"{state} cattlemen ({path}): {len(urls)} member links")
                        break
                await asyncio.sleep(2)

        return all_urls

    async def crawl_all(self, states: list[str] | None = None) -> list[str]:
        """Crawl all association sources. Returns discovered URLs."""
        breed_urls = await self.crawl_breed_associations()
        state_urls = await self.crawl_state_associations(states)

        all_urls = list(set(breed_urls + state_urls))
        logger.info(f"Association crawl complete: {len(all_urls)} total URLs")
        return all_urls

    def _extract_breeder_links(self, html: str, base_url: str) -> list[str]:
        soup = BeautifulSoup(html, "lxml")
        urls = []

        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True).lower()

            if any(kw in href.lower() for kw in ["breeder", "ranch", "farm", "member", "profile"]):
                full_url = self._resolve_url(href, base_url)
                if full_url:
                    urls.append(full_url)
            elif any(kw in text for kw in ["ranch", "farm", "cattle", "angus", "hereford"]):
                full_url = self._resolve_url(href, base_url)
                if full_url:
                    urls.append(full_url)

        return list(set(urls))

    def _extract_member_links(self, html: str, base_url: str) -> list[str]:
        soup = BeautifulSoup(html, "lxml")
        urls = []

        for a in soup.find_all("a", href=True):
            href = a["href"]
            if any(kw in href.lower() for kw in ["member", "ranch", "farm", "profile", "detail"]):
                full_url = self._resolve_url(href, base_url)
                if full_url:
                    urls.append(full_url)

        return list(set(urls))

    @staticmethod
    def _resolve_url(href: str, base_url: str) -> str | None:
        if href.startswith("http"):
            return href
        if href.startswith("/"):
            parsed = urlparse(base_url)
            return f"{parsed.scheme}://{parsed.netloc}{href}"
        if href.startswith("#") or href.startswith("javascript:") or href.startswith("mailto:"):
            return None
        return urljoin(base_url, href)
