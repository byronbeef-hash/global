"""Contact classifier for audience segmentation.

Classifies cattle scraper contacts into 5 buckets for Facebook
Lookalike Audience exports. No DB changes — classification is
done in Python at query time using email domain, farm_name,
source_url, and website fields.
"""

CATEGORIES = ("government", "education", "media", "association", "rancher")

CATEGORY_LABELS = {
    "government": "Government",
    "education": "Education",
    "media": "Journalists / Media",
    "association": "Associations",
    "rancher": "Pure Ranchers",
}

CATEGORY_COLORS = {
    "government": "#e94560",
    "education": "#5dade2",
    "media": "#f39c12",
    "association": "#2ecc71",
    "rancher": "#1abc9c",
}

# ── Classification keywords ──────────────────────────────────────────

_GOV_DOMAINS = (".gov", ".gov.au", ".govt.nz", ".gov.uk", ".gc.ca")
_GOV_KEYWORDS = (
    "department", "ministry", "usda", "defra", "mpi.govt",
    "agriculture dept", "shire council", "local government",
    "bureau of", "federal", "state government",
)

_EDU_DOMAINS = (".edu", ".ac.uk", ".edu.au", ".ac.nz", ".edu.ca")
_EDU_KEYWORDS = (
    "university", "college", "school of", "extension service",
    "research", "professor", "academic", "institute", "faculty",
    "dept of animal science", "veterinary",
)

_MEDIA_KEYWORDS = (
    "news", "media", "journal", "press", "times", "herald",
    "gazette", "reporter", "broadcast", "tv ", " tv", "radio",
    "magazine", "editor", "correspondent", "tribune", "post",
    "daily", "weekly", "publishing", "blogger",
)
# Words that indicate rancher even if a media keyword matches
_MEDIA_EXCLUSIONS = ("ranch", "farm", "cattle", "livestock", "beef", "dairy")

_ASSOC_KEYWORDS = (
    "association", "cattlemen", "society", "federation",
    "council", "registry", "herd book", "breed society",
    "national beef", "stockgrowers", "graziers", "nfu", "nff",
    "cattlewoman", "cattlewomen", "beef council", "livestock council",
)

# ── TLD → Country mapping ────────────────────────────────────────────

_TLD_COUNTRY = {
    ".com.au": "AU", ".au": "AU",
    ".co.nz": "NZ", ".nz": "NZ",
    ".co.uk": "UK", ".uk": "UK",
    ".ca": "CA",
}


def validate_country(contact: dict) -> str:
    """Return best-guess country code based on email TLD."""
    email = contact.get("email", "")
    domain = email.split("@")[-1].lower() if "@" in email else ""
    # Check longer TLDs first (.com.au before .au)
    for tld, code in sorted(_TLD_COUNTRY.items(), key=lambda x: -len(x[0])):
        if domain.endswith(tld):
            return code
    return contact.get("country", "US")


def classify_contact(contact: dict) -> str:
    """Classify a contact into one of 5 audience buckets.

    Priority: government > education > media > association > rancher.
    Returns the category string.
    """
    email = (contact.get("email") or "").lower()
    domain = email.split("@")[-1] if "@" in email else ""
    farm = (contact.get("farm_name") or "").lower()
    source = (contact.get("source_url") or "").lower()
    website = (contact.get("website") or "").lower()
    text = f"{farm} {source} {website}"

    # 1. Government
    if any(domain.endswith(d) for d in _GOV_DOMAINS):
        return "government"
    if any(kw in text for kw in _GOV_KEYWORDS):
        return "government"

    # 2. Education
    if any(domain.endswith(d) for d in _EDU_DOMAINS):
        return "education"
    if any(kw in text for kw in _EDU_KEYWORDS):
        return "education"

    # 3. Media / Journalists
    if any(kw in domain or kw in text for kw in _MEDIA_KEYWORDS):
        # Exclude if clearly a ranch/farm
        if not any(ex in text for ex in _MEDIA_EXCLUSIONS):
            return "media"

    # 4. Associations
    if any(kw in text for kw in _ASSOC_KEYWORDS):
        return "association"

    # 5. Default: Pure Rancher
    return "rancher"


def classify_contacts(contacts: list[dict]) -> dict[str, list[dict]]:
    """Classify a list of contacts into category buckets.

    Returns dict keyed by category with lists of contacts.
    Each contact gets a '_category' key added.
    """
    buckets: dict[str, list[dict]] = {cat: [] for cat in CATEGORIES}
    for c in contacts:
        cat = classify_contact(c)
        c["_category"] = cat
        buckets[cat].append(c)
    return buckets


def contact_to_fb_row(contact: dict) -> dict:
    """Map a contact to Facebook Custom Audience CSV format."""
    name_parts = (contact.get("owner_name") or "").split(maxsplit=1)
    country = validate_country(contact)
    return {
        "email": contact.get("email", ""),
        "phone": contact.get("phone", ""),
        "fn": name_parts[0] if name_parts else "",
        "ln": name_parts[1] if len(name_parts) > 1 else "",
        "ct": contact.get("city", ""),
        "st": contact.get("state", ""),
        "zip": contact.get("zip_code", ""),
        "country": country,
    }
