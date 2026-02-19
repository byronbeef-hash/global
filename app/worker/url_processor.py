"""Process a single URL: fetch → extract contacts → store to Supabase.

Optimised to avoid re-fetching pages when emails have already been
captured inline by directory/association crawlers.

CPU-heavy operations (BeautifulSoup parsing, regex extraction) are
offloaded to a thread executor via asyncio.to_thread() to prevent
blocking the async event loop and starving the FastAPI health endpoint.
"""

import asyncio
import logging
import re
from datetime import datetime
from urllib.parse import urlparse, urljoin

from bs4 import BeautifulSoup

from app.scraper.page_fetcher import PageFetcher, FetchResult
from app.scraper.contact_extractor import ContactExtractor, ContactInfo
from app.scraper.metadata_extractor import MetadataExtractor
from app.db import queries as db

logger = logging.getLogger(__name__)


class URLProcessor:
    """Processes a single URL through the full extraction pipeline."""

    def __init__(self, fetcher: PageFetcher):
        self.fetcher = fetcher
        self.contact_extractor = ContactExtractor()
        self.metadata_extractor = MetadataExtractor()

    async def process(self, url: str, country: str = "US") -> int:
        """Process a URL and store any contacts found.

        Returns the number of new emails saved.
        """
        # Skip if already completed or failed (avoid re-processing)
        if db.is_url_completed(url):
            return 0

        # Check if we already have a contact from this exact source_url
        # (means a directory/association crawler already captured it inline)
        if self._already_has_contact(url):
            db.mark_url_done(url, emails_found=1)
            return 0

        result = await self.fetcher.fetch(url)
        if not result.success:
            db.mark_url_done(url, emails_found=0, error=result.error or "fetch failed")
            return 0

        # Extract contact info in a thread to avoid blocking the event loop
        # (BeautifulSoup parsing + regex extraction is CPU-heavy)
        contact = await asyncio.to_thread(
            self.contact_extractor.extract, result.html, url
        )

        # If no emails found, try to follow a "Contact" link on the same domain
        if not contact.emails:
            contact_url = await asyncio.to_thread(
                self._find_contact_page_link, result.html, url
            )
            if contact_url and not db.is_url_seen(contact_url):
                contact_result = await self.fetcher.fetch(contact_url)
                if contact_result.success:
                    contact2 = await asyncio.to_thread(
                        self.contact_extractor.extract,
                        contact_result.html, contact_url,
                    )
                    # Merge emails from contact page
                    for email in contact2.emails:
                        if email not in contact.emails:
                            contact.emails.append(email)
                    # Fill in blanks
                    if not contact.phones and contact2.phones:
                        contact.phones = contact2.phones
                    if not contact.address and contact2.address:
                        contact.address = contact2.address
                    if not contact.state and contact2.state:
                        contact.state = contact2.state
                    if not contact.zip_code and contact2.zip_code:
                        contact.zip_code = contact2.zip_code
                    if not contact.city and contact2.city:
                        contact.city = contact2.city

        # No email = skip
        if not contact.emails:
            db.mark_url_done(url, emails_found=0)
            return 0

        # Extract metadata in a thread (BeautifulSoup + regex)
        metadata = await asyncio.to_thread(
            self._extract_metadata_sync, result.html
        )

        # Build records (one per email) and upsert
        records = self._contact_to_records(contact, metadata, country=country)
        saved = 0
        for record in records:
            if db.upsert_contact(record):
                saved += 1
                logger.debug(
                    f"Saved: {record.get('farm_name', '?')} | "
                    f"{record['email']} | {record.get('state', '?')}"
                )

        db.mark_url_done(url, emails_found=len(contact.emails))
        return saved

    def _extract_metadata_sync(self, html: str) -> dict:
        """Synchronous metadata extraction (run in thread)."""
        text = BeautifulSoup(html, "lxml").get_text(separator=" ", strip=True)
        return self.metadata_extractor.extract(text)

    @staticmethod
    def _validate_state(state: str, country: str) -> str:
        """Validate extracted state belongs to the country. Clear if mismatch."""
        if not state:
            return ""

        from app.config import COUNTRY_CONFIG
        config = COUNTRY_CONFIG.get(country, {})
        regions = config.get("regions", [])

        if not regions:
            return state

        # Check if state matches a known region (case-insensitive)
        state_lower = state.lower().strip()
        for region in regions:
            if state_lower == region.lower():
                return region  # Return canonical name

        # For non-US countries, reject if it looks like a US state
        if country != "US":
            # 2-letter code is almost certainly a US state abbreviation
            if len(state) == 2 and state.isalpha():
                return ""
            # Also check full US state names
            us_regions = [r.lower() for r in COUNTRY_CONFIG.get("US", {}).get("regions", [])]
            if state_lower in us_regions:
                return ""

        return state

    @staticmethod
    def _contact_to_records(contact: ContactInfo, metadata: dict, country: str = "US") -> list[dict]:
        """Convert ContactInfo + metadata to flat dicts — one row per email."""
        validated_state = URLProcessor._validate_state(contact.state, country)

        base = {
            "farm_name": contact.farm_name,
            "owner_name": contact.owner_name,
            "phone": contact.phones[0] if contact.phones else "",
            "address": contact.address,
            "city": contact.city,
            "state": validated_state,
            "zip_code": contact.zip_code,
            "country": country,
            "website": contact.website,
            "facebook": contact.facebook,
            "instagram": contact.instagram,
            "cattle_type": metadata.get("cattle_type", ""),
            "breed": metadata.get("breed", ""),
            "head_count": metadata.get("head_count", ""),
            "source_url": contact.source_url,
        }
        if not contact.emails:
            return []
        return [{"email": email, **base} for email in contact.emails]

    @staticmethod
    def _already_has_contact(url: str) -> bool:
        """Check if we already have a contact with this source_url.

        This avoids re-fetching pages that were already captured inline
        by directory or association crawlers.
        """
        try:
            from app.db.supabase_client import get_client
            client = get_client()
            result = (
                client.table("contacts")
                .select("id")
                .eq("source_url", url)
                .limit(1)
                .execute()
            )
            return bool(result.data)
        except Exception:
            return False

    @staticmethod
    def _find_contact_page_link(html: str, base_url: str) -> str | None:
        """Find a 'Contact Us' or 'Contact' link on the same domain."""
        soup = BeautifulSoup(html, "lxml")
        base_domain = urlparse(base_url).netloc.lower()

        for a in soup.find_all("a", href=True):
            text = a.get_text(strip=True).lower()
            href = a["href"].lower()

            if any(kw in text for kw in ["contact", "get in touch", "reach us", "email us"]) or \
               any(kw in href for kw in ["/contact", "/about", "/reach-us"]):
                full_url = urljoin(base_url, a["href"])
                if urlparse(full_url).netloc.lower() == base_domain:
                    return full_url
        return None
