"""Extract contact information from HTML pages."""

import json
import logging
import re
from dataclasses import dataclass, field

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# === Regex Patterns ===

EMAIL_RE = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
)

# US phone patterns: (xxx) xxx-xxxx, xxx-xxx-xxxx, xxx.xxx.xxxx, xxxxxxxxxx
PHONE_RE = re.compile(
    r"""
    (?:
        \+?1[-.\s]?                          # optional country code
    )?
    (?:
        \(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}  # (xxx) xxx-xxxx or xxx-xxx-xxxx
    )
    """,
    re.VERBOSE,
)

# US zip code
ZIP_RE = re.compile(r"\b\d{5}(?:-\d{4})?\b")

# State abbreviations
STATE_ABBREVS = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
}

# Simple address pattern: street number + name + suffix, followed by optional unit/city
ADDRESS_RE = re.compile(
    r"\d{1,6}\s+(?:[NSEW]\.?\s+)?[\w]+(?:\s+[\w]+){0,4}\s+"
    r"(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Lane|Ln|Boulevard|Blvd|Way|Court|Ct|Highway|Hwy|Route|Rt|County\s+Road|CR|Farm\s+Road|FM)\.?"
    r"(?:[,\s]+[\w\s]+){0,3}",
    re.IGNORECASE,
)

# Junk email domains to filter out
JUNK_EMAIL_DOMAINS = {
    # Technical/platform domains
    "example.com", "sentry.io", "wixpress.com", "googleapis.com",
    "w3.org", "schema.org", "facebook.com", "twitter.com",
    "instagram.com", "google.com", "googleusercontent.com",
    "gstatic.com", "cloudflare.com", "jquery.com", "wordpress.com",
    "wp.com", "gravatar.com", "bootstrapcdn.com", "squarespace.com",
    "shopify.com", "wix.com", "godaddy.com", "mailchimp.com",
    "constantcontact.com", "hubspot.com", "salesforce.com",
    "zendesk.com", "intercom.io", "typeform.com", "calendly.com",
    # News/media domains
    "nytimes.com", "washingtonpost.com", "cnn.com", "bbc.com",
    "bbc.co.uk", "reuters.com", "apnews.com", "usatoday.com",
    "denverpost.com", "westword.com", "yahoo.com", "msn.com",
    # Government
    "state.co.us", "state.tx.us", "state.mn.us",
}

# Patterns for definitely-not-contact emails
JUNK_EMAIL_PATTERNS = [
    r".*\.png$", r".*\.jpg$", r".*\.gif$", r".*\.svg$",
    r"^noreply@", r"^no-reply@", r"^donotreply@",
    r"^webmaster@", r"^postmaster@", r"^mailer-daemon@",
    # Government/institutional
    r".*@.*\.gov$", r".*@.*\.gov\.\w+$", r".*@.*\.edu$",
    r".*@.*state\.\w{2}\.us$",
    # Generic non-farm prefixes
    r"^editor@", r"^press@", r"^marketing@", r"^advertising@",
    r"^hr@", r"^careers@", r"^jobs@", r"^recruitment@",
    r"^legal@", r"^compliance@", r"^privacy@",
    r"^newsletter@", r"^subscribe@", r"^unsubscribe@",
    r"^abuse@", r"^spam@", r"^security@", r"^root@",
    r"^admin@", r"^administrator@", r"^hostmaster@",
    r"^billing@", r"^accounts@", r"^payments@",
]

# Source URL domains that are never cattle farms — skip entirely
JUNK_SOURCE_DOMAINS = {
    # News / media
    "denverpost.com", "westword.com", "nytimes.com", "washingtonpost.com",
    "cnn.com", "bbc.com", "bbc.co.uk", "reuters.com", "theguardian.com",
    "independent.co.uk", "telegraph.co.uk", "dailymail.co.uk",
    "abc.net.au", "smh.com.au", "stuff.co.nz", "nzherald.co.nz",
    "cbc.ca", "globalnews.ca", "ctvnews.ca",
    "usatoday.com", "apnews.com", "foxnews.com", "nbcnews.com",
    # Social / forums / blogs
    "reddit.com", "quora.com", "facebook.com", "twitter.com",
    "instagram.com", "youtube.com", "linkedin.com", "tiktok.com",
    "pinterest.com", "tumblr.com", "medium.com", "wordpress.com",
    "blogspot.com", "blogger.com",
    # Government
    "usda.gov", "epa.gov", "fda.gov", "irs.gov",
    "colorado.gov", "texas.gov", "nebraska.gov",
    "gov.uk", "gov.au", "govt.nz", "canada.ca",
    # Classifieds / generic directories
    "craigslist.org", "ebay.com", "amazon.com", "walmart.com",
    "indeed.com", "glassdoor.com", "zillow.com", "realtor.com",
    "realestate.com.au", "trademe.co.nz", "rightmove.co.uk",
    # Real estate / non-farm
    "flatraterealtygroup.com",
    # Misc junk
    "bigfootforums.com", "wikipedia.org", "wikimedia.org",
    "archive.org", "web.archive.org",
}


@dataclass
class ContactInfo:
    """Extracted contact information from a page."""
    farm_name: str = ""
    owner_name: str = ""
    emails: list[str] = field(default_factory=list)
    phones: list[str] = field(default_factory=list)
    address: str = ""
    city: str = ""
    state: str = ""
    zip_code: str = ""
    website: str = ""
    facebook: str = ""
    instagram: str = ""
    source_url: str = ""


class ContactExtractor:
    """Extract contact info from HTML using regex + structured data parsing."""

    def extract(self, html: str, source_url: str) -> ContactInfo:
        """Extract all contact information from an HTML page."""
        soup = BeautifulSoup(html, "lxml")
        contact = ContactInfo(source_url=source_url)

        # Try structured data first (most reliable)
        self._extract_structured_data(soup, contact)

        # Then regex-based extraction
        text = soup.get_text(separator=" ", strip=True)
        self._extract_emails(soup, text, contact)
        self._extract_phones(text, contact)
        self._extract_address(text, contact)
        self._extract_social_links(soup, contact)
        self._extract_farm_name(soup, contact)
        self._extract_owner_name(soup, text, contact)
        self._extract_website(soup, source_url, contact)

        return contact

    def _extract_structured_data(self, soup: BeautifulSoup, contact: ContactInfo) -> None:
        """Extract from JSON-LD, Schema.org markup."""
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string)
                if isinstance(data, list):
                    for item in data:
                        self._parse_schema_item(item, contact)
                elif isinstance(data, dict):
                    self._parse_schema_item(data, contact)
            except (json.JSONDecodeError, TypeError):
                continue

    def _parse_schema_item(self, data: dict, contact: ContactInfo) -> None:
        """Parse a Schema.org JSON-LD item."""
        schema_type = data.get("@type", "")
        if schema_type in ("LocalBusiness", "Farm", "Organization", "AnimalShelter", "Store"):
            if not contact.farm_name and data.get("name"):
                contact.farm_name = data["name"]
            if data.get("email"):
                email = data["email"].replace("mailto:", "")
                if email not in contact.emails:
                    contact.emails.append(email)
            if data.get("telephone"):
                phone = self._normalize_phone(data["telephone"])
                if phone and phone not in contact.phones:
                    contact.phones.append(phone)
            address = data.get("address", {})
            if isinstance(address, dict):
                if not contact.address and address.get("streetAddress"):
                    contact.address = address["streetAddress"]
                if not contact.city and address.get("addressLocality"):
                    contact.city = address["addressLocality"]
                if not contact.state and address.get("addressRegion"):
                    contact.state = address["addressRegion"]
                if not contact.zip_code and address.get("postalCode"):
                    contact.zip_code = address["postalCode"]
            if data.get("url") and not contact.website:
                contact.website = data["url"]

    def _extract_emails(self, soup: BeautifulSoup, text: str, contact: ContactInfo) -> None:
        """Extract email addresses from mailto links and page text."""
        # From mailto links first (most reliable)
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("mailto:"):
                email = href.replace("mailto:", "").split("?")[0].strip().lower()
                if self._is_valid_email(email) and email not in contact.emails:
                    contact.emails.append(email)

        # From page text via regex
        for match in EMAIL_RE.finditer(text):
            email = match.group().lower()
            if self._is_valid_email(email) and email not in contact.emails:
                contact.emails.append(email)

    def _extract_phones(self, text: str, contact: ContactInfo) -> None:
        """Extract US phone numbers from page text."""
        for match in PHONE_RE.finditer(text):
            phone = self._normalize_phone(match.group())
            if phone and phone not in contact.phones:
                contact.phones.append(phone)

    def _extract_address(self, text: str, contact: ContactInfo) -> None:
        """Extract street address from page text."""
        if contact.address:
            return

        match = ADDRESS_RE.search(text)
        if match:
            contact.address = match.group().strip()

        if not contact.zip_code:
            zip_match = ZIP_RE.search(text)
            if zip_match:
                contact.zip_code = zip_match.group()

        if not contact.state:
            for abbrev in STATE_ABBREVS:
                pattern = rf"\b{abbrev}\b\s*\d{{5}}"
                if re.search(pattern, text):
                    contact.state = abbrev
                    break

    def _extract_social_links(self, soup: BeautifulSoup, contact: ContactInfo) -> None:
        """Extract Facebook and Instagram links."""
        for a in soup.find_all("a", href=True):
            href = a["href"].lower()
            if "facebook.com/" in href and not contact.facebook:
                contact.facebook = a["href"]
            elif "instagram.com/" in href and not contact.instagram:
                contact.instagram = a["href"]

    def _extract_farm_name(self, soup: BeautifulSoup, contact: ContactInfo) -> None:
        """Extract farm/ranch name from page title or headings."""
        if contact.farm_name:
            return

        title = soup.find("title")
        if title and title.string:
            name = title.string.strip()
            for suffix in [" - Home", " | Home", " - Contact", " | Contact",
                           " - About", " | About", " – Home", " – Contact"]:
                if name.endswith(suffix):
                    name = name[: -len(suffix)]
            name = re.sub(
                r"\s*[-–|]\s*(?:" + "|".join(
                    list(STATE_ABBREVS) + [
                        "Alabama", "Alaska", "Arizona", "Arkansas", "California",
                        "Colorado", "Connecticut", "Delaware", "Florida", "Georgia",
                        "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas",
                        "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts",
                        "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana",
                        "Nebraska", "Nevada", "New Hampshire", "New Jersey",
                        "New Mexico", "New York", "North Carolina", "North Dakota",
                        "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island",
                        "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah",
                        "Vermont", "Virginia", "Washington", "West Virginia",
                        "Wisconsin", "Wyoming",
                    ]
                ) + r")\s*$",
                "",
                name,
                flags=re.IGNORECASE,
            )
            if len(name) < 100:
                contact.farm_name = name.strip()

        if not contact.farm_name:
            og = soup.find("meta", property="og:site_name")
            if og and og.get("content"):
                contact.farm_name = og["content"].strip()

        if not contact.farm_name:
            h1 = soup.find("h1")
            if h1:
                text = h1.get_text(strip=True)
                if len(text) < 100:
                    contact.farm_name = text

    def _extract_owner_name(self, soup: BeautifulSoup, text: str, contact: ContactInfo) -> None:
        """Attempt to extract owner/contact name."""
        if contact.owner_name:
            return

        patterns = [
            r"(?i:owned\s+(?:and\s+)?operated\s+by|owner[s]?:?\s*|contact[s]?:?\s*|manager:?\s*|proprietor:?\s*)\s*([A-Z][a-z]+\s+(?:[A-Z]\.?\s+)?[A-Z][a-z]+)",
            r"(?i:family[\s-]+owned|run\s+by|managed\s+by)\s+(?:by\s+)?([A-Z][a-z]+\s+(?:[A-Z]\.?\s+)?[A-Z][a-z]+)",
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                contact.owner_name = match.group(1).strip()
                return

    def _extract_website(self, soup: BeautifulSoup, source_url: str, contact: ContactInfo) -> None:
        """Set website field."""
        if not contact.website:
            og = soup.find("meta", property="og:url")
            if og and og.get("content"):
                contact.website = og["content"]
            else:
                canonical = soup.find("link", rel="canonical")
                if canonical and canonical.get("href"):
                    contact.website = canonical["href"]
                else:
                    contact.website = source_url

    def _is_valid_email(self, email: str) -> bool:
        """Check if email is likely a real contact email."""
        if not EMAIL_RE.fullmatch(email):
            return False
        domain = email.split("@")[1]
        if domain in JUNK_EMAIL_DOMAINS:
            return False
        for pattern in JUNK_EMAIL_PATTERNS:
            if re.match(pattern, email):
                return False
        return True

    @staticmethod
    def _normalize_phone(raw: str) -> str | None:
        """Normalize phone to (xxx) xxx-xxxx format."""
        digits = re.sub(r"\D", "", raw)
        if digits.startswith("1") and len(digits) == 11:
            digits = digits[1:]
        if len(digits) != 10:
            return None
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
