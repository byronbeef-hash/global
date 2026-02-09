"""Crawl business directories (YellowPages, Yelp, Manta) for cattle farm listings."""

import asyncio
import logging
import re
from urllib.parse import quote_plus, urljoin

from bs4 import BeautifulSoup

from app.scraper.page_fetcher import PageFetcher

logger = logging.getLogger(__name__)


class DirectoryCrawler:
    """Base class for directory site crawlers."""

    def __init__(self, fetcher: PageFetcher):
        self.fetcher = fetcher

    async def crawl_all_states(self, states: list[str]) -> list[dict]:
        """Crawl directory for all given states. Returns list of contact dicts."""
        raise NotImplementedError


class YellowPagesCrawler(DirectoryCrawler):
    """Crawl YellowPages for cattle ranch/farm listings.

    Supports multiple countries with country-specific base URLs and search patterns.
    """

    # Country-specific YellowPages configurations
    COUNTRY_CONFIGS = {
        "US": {
            "base_url": "https://www.yellowpages.com",
            "url_template": "/search?search_terms={term}&geo_location_terms={region}&page={page}",
            "terms": ["cattle ranch", "cattle farm", "livestock ranch", "beef ranch"],
        },
        "NZ": {
            "base_url": "https://www.yellow.co.nz",
            "url_template": "/search/search?clue={term}&location={region}&page={page}",
            "terms": ["cattle farm", "beef farm", "dairy farm", "cattle breeder", "livestock"],
        },
        "UK": {
            "base_url": "https://www.yell.com",
            "url_template": "/ucs/UcsSearchAction.do?keywords={term}&location={region}&pageNum={page}",
            "terms": ["cattle farm", "beef farm", "dairy farm", "cattle breeder", "livestock farm"],
        },
        "CA": {
            "base_url": "https://www.yellowpages.ca",
            "url_template": "/search/si/{page}/{term}/{region}",
            "terms": ["cattle ranch", "cattle farm", "beef farm", "livestock ranch", "dairy farm"],
        },
        "AU": {
            "base_url": "https://www.yellowpages.com.au",
            "url_template": "/find/{term}/{region}?pageNumber={page}",
            "terms": ["cattle farm", "beef farm", "cattle station", "livestock farm", "dairy farm"],
        },
    }

    def __init__(self, fetcher: PageFetcher, country: str = "US"):
        super().__init__(fetcher)
        self.country = country
        cfg = self.COUNTRY_CONFIGS.get(country, self.COUNTRY_CONFIGS["US"])
        self.base_url = cfg["base_url"]
        self.url_template = cfg["url_template"]
        self.search_terms = cfg["terms"]

    async def crawl_all_states(self, states: list[str]) -> list[dict]:
        all_contacts = []
        for state in states:
            for term in self.search_terms:
                contacts = await self._search_state(term, state)
                all_contacts.extend(contacts)
                await asyncio.sleep(2)
        return all_contacts

    async def _search_state(self, term: str, state: str, max_pages: int = 3) -> list[dict]:
        contacts = []
        for page in range(1, max_pages + 1):
            url = self.base_url + self.url_template.format(
                term=quote_plus(term), region=quote_plus(state), page=page,
            )
            result = await self.fetcher.fetch(url)
            if not result.success:
                break

            page_contacts = self._parse_listings(result.html, state)
            if not page_contacts:
                break

            contacts.extend(page_contacts)
            logger.info(
                f"YellowPages-{self.country} [{state}] '{term}' p{page}: "
                f"{len(page_contacts)} listings"
            )

        return contacts

    def _parse_listings(self, html: str, state: str) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        contacts = []

        # Broad selectors that work across international YellowPages variants
        selectors = (
            ".result, .v-card, .search-results .srp-listing, "
            ".listing, .search-result, [class*='listing-content'], "
            "[class*='search-contact'], [class*='resultBody']"
        )
        for listing in soup.select(selectors):
            contact = {"source_url": f"yellowpages-{self.country}", "state": state}

            name_el = listing.select_one(
                ".business-name, .n a, h2 a, h3 a, "
                "[class*='name'] a, [class*='title'] a"
            )
            if name_el:
                contact["farm_name"] = name_el.get_text(strip=True)

            phone_el = listing.select_one(".phones, .phone, .primary, [class*='phone']")
            if phone_el:
                contact["phone"] = phone_el.get_text(strip=True)

            addr_el = listing.select_one(".street-address, .adr, [class*='address']")
            if addr_el:
                contact["address"] = addr_el.get_text(strip=True)

            locality_el = listing.select_one(".locality, [class*='locality']")
            if locality_el:
                parts = locality_el.get_text(strip=True).split(",")
                if parts:
                    contact["city"] = parts[0].strip()

            website_el = listing.select_one(
                "a.track-visit-website, a[href*='website'], a[class*='website']"
            )
            if website_el and website_el.get("href"):
                href = website_el["href"]
                if href.startswith("http"):
                    contact["website"] = href

            # Also try to extract emails from listing text
            text = listing.get_text()
            email_match = re.search(r'[\w.+-]+@[\w-]+\.[\w.-]+', text)
            if email_match:
                contact["email"] = email_match.group(0).lower()

            if contact.get("farm_name"):
                contacts.append(contact)

        return contacts


class YelpCrawler(DirectoryCrawler):
    """Crawl Yelp for cattle farm listings."""

    BASE_URL = "https://www.yelp.com"
    SEARCH_TERMS = ["cattle ranch", "cattle farm", "beef farm"]

    async def crawl_all_states(self, states: list[str]) -> list[dict]:
        all_contacts = []
        for state in states:
            for term in self.SEARCH_TERMS:
                contacts = await self._search_state(term, state)
                all_contacts.extend(contacts)
                await asyncio.sleep(3)
        return all_contacts

    async def _search_state(self, term: str, state: str, max_pages: int = 2) -> list[dict]:
        contacts = []
        for page_start in range(0, max_pages * 10, 10):
            url = f"{self.BASE_URL}/search?find_desc={quote_plus(term)}&find_loc={quote_plus(state)}&start={page_start}"
            result = await self.fetcher.fetch(url)
            if not result.success:
                break

            page_contacts = self._parse_listings(result.html, state)
            if not page_contacts:
                break

            contacts.extend(page_contacts)
            logger.info(f"Yelp [{state}] '{term}' offset {page_start}: {len(page_contacts)} listings")

        return contacts

    def _parse_listings(self, html: str, state: str) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        contacts = []

        for listing in soup.select("[data-testid='serp-ia-card'], .container__09f24__mpR8_, li.border-color"):
            contact = {"source_url": "yelp.com", "state": state}

            name_el = listing.select_one("a[href*='/biz/'] span, h3 a, h3 span")
            if name_el:
                contact["farm_name"] = name_el.get_text(strip=True)

            phone_el = listing.select_one("[class*='phone'], .phone")
            if phone_el:
                contact["phone"] = phone_el.get_text(strip=True)

            loc_el = listing.select_one("[class*='secondaryAttributes'], .priceRange")
            if loc_el:
                text = loc_el.get_text(strip=True)
                contact["city"] = text

            link_el = listing.select_one("a[href*='/biz/']")
            if link_el and link_el.get("href"):
                href = link_el["href"]
                if not href.startswith("http"):
                    href = self.BASE_URL + href
                contact["website"] = href

            # Extract emails from listing text
            text = listing.get_text()
            email_match = re.search(r'[\w.+-]+@[\w-]+\.[\w.-]+', text)
            if email_match:
                contact["email"] = email_match.group(0).lower()

            if contact.get("farm_name"):
                contacts.append(contact)

        return contacts


class MantaCrawler(DirectoryCrawler):
    """Crawl Manta business directory for cattle farms."""

    BASE_URL = "https://www.manta.com"

    async def crawl_all_states(self, states: list[str]) -> list[dict]:
        all_contacts = []
        for state in states:
            url = f"{self.BASE_URL}/search?search_source=nav&search={quote_plus('cattle ranch')}&search_location={quote_plus(state)}"
            result = await self.fetcher.fetch(url)
            if result.success:
                contacts = self._parse_listings(result.html, state)
                all_contacts.extend(contacts)
                logger.info(f"Manta [{state}]: {len(contacts)} listings")
            await asyncio.sleep(3)
        return all_contacts

    def _parse_listings(self, html: str, state: str) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        contacts = []

        for listing in soup.select(".listing, .search-result, [class*='result']"):
            contact = {"source_url": "manta.com", "state": state}

            name_el = listing.select_one("h3 a, .business-name a, [class*='name'] a")
            if name_el:
                contact["farm_name"] = name_el.get_text(strip=True)

            phone_el = listing.select_one("[class*='phone'], .phone")
            if phone_el:
                contact["phone"] = phone_el.get_text(strip=True)

            addr_el = listing.select_one("[class*='address'], .address")
            if addr_el:
                contact["address"] = addr_el.get_text(strip=True)

            # Extract emails from listing text
            text = listing.get_text()
            email_match = re.search(r'[\w.+-]+@[\w-]+\.[\w.-]+', text)
            if email_match:
                contact["email"] = email_match.group(0).lower()

            # Also check for mailto links
            mailto_el = listing.select_one("a[href^='mailto:']")
            if mailto_el and not contact.get("email"):
                email = mailto_el["href"].replace("mailto:", "").split("?")[0].strip().lower()
                if email:
                    contact["email"] = email

            if contact.get("farm_name"):
                contacts.append(contact)

        return contacts
