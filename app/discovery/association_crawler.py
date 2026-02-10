"""Crawl cattlemen's association and breed association directories.

Extracts both URLs (for later processing) and emails inline from every
page fetched, so we never waste a page fetch without trying to harvest data.
"""

import asyncio
import logging
import re
from urllib.parse import quote_plus, urlparse, urljoin

from bs4 import BeautifulSoup

from app.scraper.page_fetcher import PageFetcher
from app.scraper.contact_extractor import ContactExtractor
from app.db import queries as db

logger = logging.getLogger(__name__)

# Quick email regex for inline extraction (same as ContactExtractor)
_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
_JUNK_DOMAINS = {
    "example.com", "sentry.io", "wixpress.com", "googleapis.com",
    "w3.org", "schema.org", "facebook.com", "twitter.com",
    "instagram.com", "google.com", "googleusercontent.com",
    "gstatic.com", "cloudflare.com", "jquery.com", "wordpress.com",
    "wp.com", "gravatar.com", "bootstrapcdn.com",
}

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
    """Crawl breed association and state cattlemen's association directories.

    Now extracts emails inline from every page fetched, saving contacts
    immediately rather than waiting for Phase 2 URL processing.
    """

    def __init__(self, fetcher: PageFetcher, country: str = "US"):
        self.fetcher = fetcher
        self.contact_extractor = ContactExtractor()
        self.country = country
        self._inline_emails_saved = 0

        # Cache country regions for state validation
        from app.config import COUNTRY_CONFIG
        config = COUNTRY_CONFIG.get(country, {})
        self._valid_regions = [r.lower() for r in config.get("regions", [])]

    def _validate_state(self, state: str) -> str:
        """Validate state belongs to this country. Clear if mismatch."""
        if not state:
            return ""
        state_lower = state.lower().strip()
        if self._valid_regions:
            if state_lower in self._valid_regions:
                return state
            # For non-US countries, reject US state abbreviations and names
            if self.country != "US":
                if len(state) == 2 and state.isalpha():
                    return ""
                from app.config import COUNTRY_CONFIG
                us_regions = [r.lower() for r in COUNTRY_CONFIG.get("US", {}).get("regions", [])]
                if state_lower in us_regions:
                    return ""
        return state

    async def crawl_breed_associations(self) -> list[str]:
        """Crawl breed association 'Find a Breeder' pages."""
        all_urls = []

        for name, url in ASSOCIATION_URLS.items():
            logger.info(f"Crawling {name}: {url}")
            result = await self.fetcher.fetch(url)
            if not result.success:
                logger.warning(f"Failed to fetch {name}: {result.error}")
                continue

            # Extract emails inline from this page
            self._extract_and_save_emails(result.html, url, source=name)

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
                    # Extract emails inline from this page
                    self._extract_and_save_emails(
                        result.html, url, source=f"cattlemen-{state}", state=state,
                    )

                    urls = self._extract_member_links(result.html, url)
                    if urls:
                        all_urls.extend(urls)
                        logger.info(f"{state} cattlemen ({path}): {len(urls)} member links")
                        break
                await asyncio.sleep(2)

        return all_urls

    async def crawl_all(self, states: list[str] | None = None) -> list[str]:
        """Crawl all association sources. Returns discovered URLs."""
        self._inline_emails_saved = 0
        breed_urls = await self.crawl_breed_associations()
        state_urls = await self.crawl_state_associations(states)

        all_urls = list(set(breed_urls + state_urls))
        logger.info(
            f"Association crawl complete: {len(all_urls)} URLs, "
            f"{self._inline_emails_saved} emails saved inline"
        )
        return all_urls

    def _extract_and_save_emails(
        self, html: str, source_url: str,
        source: str = "association", state: str = "",
    ) -> int:
        """Extract emails from page HTML and save them immediately.

        Returns number of new emails saved.
        """
        # Use the full ContactExtractor for thorough extraction
        contact = self.contact_extractor.extract(html, source_url)

        # Validate state belongs to this country
        raw_state = state or contact.state
        validated_state = self._validate_state(raw_state)

        saved = 0
        for email in contact.emails:
            record = {
                "email": email,
                "farm_name": contact.farm_name,
                "owner_name": contact.owner_name,
                "phone": contact.phones[0] if contact.phones else "",
                "address": contact.address,
                "city": contact.city,
                "state": validated_state,
                "zip_code": contact.zip_code,
                "country": self.country,
                "website": contact.website,
                "facebook": contact.facebook,
                "instagram": contact.instagram,
                "source_url": source_url,
            }
            if db.upsert_contact(record):
                saved += 1
                logger.debug(f"Inline email from {source}: {email}")

        if saved:
            logger.info(f"[{source}] Saved {saved} emails inline from {source_url}")
        self._inline_emails_saved += saved
        return saved

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
