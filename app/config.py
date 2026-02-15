"""Environment-driven configuration for the production cattle scraper."""

import os

# === Supabase ===
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

# === ScraperAPI Proxy ===
SCRAPER_API_KEY = os.environ.get("SCRAPER_API_KEY", "")
USE_PROXY = bool(SCRAPER_API_KEY)

# === Dashboard ===
DASHBOARD_API_KEY = os.environ.get("DASHBOARD_API_KEY", "changeme")
PORT = int(os.environ.get("PORT", "8080"))

# === Rate Limiting ===
DEFAULT_RATE_LIMIT = float(os.environ.get("DEFAULT_RATE_LIMIT", "2.0"))
SEARCH_RATE_LIMIT = float(os.environ.get("SEARCH_RATE_LIMIT", "1.0"))
DIRECTORY_RATE_LIMIT = float(os.environ.get("DIRECTORY_RATE_LIMIT", "3.0"))
MAX_CONCURRENT_REQUESTS = int(os.environ.get("MAX_CONCURRENT_REQUESTS", "10"))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "30"))

# === Retry ===
MAX_RETRIES = 3
RETRY_BACKOFF = 2.0

# === Search ===
SEARCH_RESULTS_PER_QUERY = 20
MAX_SEARCH_PAGES = 3

# === Playwright ===
PLAYWRIGHT_HEADLESS = True
PLAYWRIGHT_TIMEOUT = 20000  # ms

# === Worker ===
WORKER_BATCH_SIZE = int(os.environ.get("WORKER_BATCH_SIZE", "100"))
WORKER_SLEEP_BETWEEN_JOBS = int(os.environ.get("WORKER_SLEEP_BETWEEN_JOBS", "30"))

# === Active Countries ===
ACTIVE_COUNTRIES = os.environ.get("ACTIVE_COUNTRIES", "US,NZ,UK,CA,AU").split(",")

# ============================================================================
# COUNTRY-SPECIFIC CONFIGURATIONS
# ============================================================================

COUNTRY_CONFIG = {
    # ---------- United States ----------
    "US": {
        "name": "United States",
        "regions": [
            "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
            "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho",
            "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana",
            "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota",
            "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada",
            "New Hampshire", "New Jersey", "New Mexico", "New York",
            "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon",
            "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota",
            "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington",
            "West Virginia", "Wisconsin", "Wyoming",
        ],
        "top_regions": [
            "Texas", "Nebraska", "Kansas", "California", "Oklahoma",
            "South Dakota", "Missouri", "Iowa", "Colorado", "Montana",
            "North Dakota", "Idaho", "Wisconsin", "Minnesota", "Kentucky",
            "Wyoming", "Florida", "Oregon", "Virginia", "Georgia",
        ],
        "search_terms": [
            "cattle ranch", "cattle farm", "beef ranch", "beef cattle",
            "dairy farm", "cow calf operation", "livestock ranch",
            "angus ranch", "hereford ranch", "cattle breeder",
            "cattle rancher", "beef producer", "cattle operation",
            "ranch email", "feedlot", "cattle feedlot",
            "cow farm", "heifer farm", "stockyard",
            "cattle company", "beef company", "ranch company",
            "family ranch", "family cattle farm",
            "commercial cattle", "purebred cattle",
            "seed stock producer", "stocker cattle",
        ],
        "search_templates": [
            "{term} {region} contact email",
            "{term} {region} gmail",
            "{term} {region} email address",
            "{term} {region} contact us",
            "{term} {region} contact information",
            "registered {breed} breeder {region} contact",
            "registered {breed} {region} email",
            "{term} bulls for sale {region} email contact",
            "{term} for sale {region} contact",
            "{term} {region} owner email",
            "{term} {region} website contact",
            "{term} near {region} email",
            "{breed} breeder {region} email address",
            "{breed} cattle for sale {region} contact",
            "{term} {region} phone email",
            "{term} {region} ranch email",
            "buy cattle {region} contact email",
            "cattle for sale {region} farmer email",
        ],
        "breeds": [
            "angus", "hereford", "charolais", "simmental", "limousin",
            "brahman", "shorthorn", "red angus", "gelbvieh", "maine-anjou",
            "holstein", "jersey", "guernsey", "brown swiss", "ayrshire",
            "longhorn", "highland", "wagyu", "brangus", "beefmaster",
        ],
        "directories": {
            "yellowpages": "https://www.yellowpages.com/search",
            "yelp": "https://www.yelp.com/search",
        },
    },

    # ---------- New Zealand ----------
    "NZ": {
        "name": "New Zealand",
        "regions": [
            "Northland", "Auckland", "Waikato", "Bay of Plenty", "Gisborne",
            "Hawkes Bay", "Taranaki", "Manawatu-Whanganui", "Wellington",
            "Tasman", "Nelson", "Marlborough", "West Coast", "Canterbury",
            "Otago", "Southland",
        ],
        "top_regions": [
            "Waikato", "Canterbury", "Southland", "Otago", "Manawatu-Whanganui",
            "Taranaki", "Hawkes Bay", "Bay of Plenty", "Northland", "Gisborne",
            "Wellington", "Tasman", "Nelson", "Marlborough", "West Coast",
            "Gisborne",
        ],
        "search_terms": [
            "cattle farm", "beef farm", "dairy farm", "cattle station",
            "cattle breeder", "beef breeder", "stud cattle",
            "beef producer", "cattle farmer", "dairy farmer",
            "livestock farm", "bull breeder", "cattle stud",
            "cattle property", "grazing farm", "stock farm",
            "farm email", "farmer contact", "rural property",
            "beef cattle farm", "dairy cattle farm",
        ],
        "search_templates": [
            "{term} {region} New Zealand contact email",
            "{term} {region} NZ email",
            "{term} {region} New Zealand contact",
            "{term} New Zealand {region} email address",
            "registered {breed} breeder {region} New Zealand",
            "registered {breed} stud {region} NZ contact",
            "{breed} cattle {region} New Zealand email",
            "{term} for sale {region} New Zealand contact",
            "cattle stud {region} NZ email",
            "{term} {region} NZ farmer email",
            "{term} {region} New Zealand owner contact",
            "{breed} stud {region} NZ email address",
            "buy {breed} cattle {region} New Zealand email",
            "{term} near {region} NZ contact email",
        ],
        "breeds": [
            "angus", "hereford", "charolais", "simmental", "limousin",
            "shorthorn", "murray grey", "south devon", "red angus",
            "highland", "wagyu", "speckle park", "lowline",
            "jersey", "friesian", "ayrshire", "brown swiss",
        ],
        "directories": {
            "yellowpages": "https://www.yellow.co.nz/search",
        },
    },

    # ---------- United Kingdom ----------
    "UK": {
        "name": "United Kingdom",
        "regions": [
            # England
            "Devon", "Somerset", "Cornwall", "Dorset", "Wiltshire",
            "Hampshire", "Kent", "Sussex", "Suffolk", "Norfolk",
            "Lincolnshire", "Yorkshire", "Lancashire", "Cumbria",
            "Northumberland", "Herefordshire", "Shropshire", "Staffordshire",
            "Cheshire", "Gloucestershire", "Oxfordshire",
            # Scotland
            "Aberdeenshire", "Angus Scotland", "Perth and Kinross",
            "Highland Scotland", "Dumfries and Galloway", "Scottish Borders",
            "Fife", "Stirling",
            # Wales
            "Powys", "Carmarthenshire", "Pembrokeshire", "Ceredigion",
            "Gwynedd", "Denbighshire",
            # Northern Ireland
            "County Antrim", "County Down", "County Tyrone",
            "County Armagh", "County Fermanagh",
        ],
        "top_regions": [
            "Devon", "Somerset", "Yorkshire", "Aberdeenshire", "Cumbria",
            "Herefordshire", "Highland Scotland", "Dumfries and Galloway",
            "Lincolnshire", "Norfolk", "Powys", "Shropshire",
            "Lancashire", "Cornwall", "Dorset",
        ],
        "search_terms": [
            "cattle farm", "beef farm", "dairy farm", "cattle breeder",
            "beef breeder", "pedigree cattle", "cattle farmer",
            "beef producer", "livestock farm", "cattle herd",
            "pedigree herd", "bull breeder", "cattle stud",
            "farm email", "farmer contact", "agricultural farm",
            "beef herd", "suckler herd", "pedigree breeder",
            "livestock breeder", "cattle dealer",
        ],
        "search_templates": [
            "{term} {region} UK contact email",
            "{term} {region} England email",
            "{term} {region} Scotland email",
            "{term} {region} Wales email",
            "{term} {region} contact email",
            "pedigree {breed} breeder {region} UK contact",
            "registered {breed} {region} UK email",
            "{breed} cattle {region} UK email",
            "{term} for sale {region} UK contact",
            "{term} {region} UK farmer email",
            "{term} {region} UK owner contact",
            "{breed} herd {region} UK email address",
            "buy {breed} cattle {region} UK email",
            "{term} near {region} UK contact email",
            "{term} {region} Britain email address",
        ],
        "breeds": [
            "angus", "aberdeen angus", "hereford", "charolais", "simmental",
            "limousin", "shorthorn", "highland", "belted galloway",
            "south devon", "red poll", "dexter", "welsh black",
            "longhorn", "galloway", "british white", "lincoln red",
            "jersey", "friesian", "ayrshire", "guernsey",
        ],
        "directories": {
            "yellowpages": "https://www.yell.com/s/",
        },
    },

    # ---------- Canada ----------
    "CA": {
        "name": "Canada",
        "regions": [
            "Alberta", "Saskatchewan", "Manitoba", "British Columbia",
            "Ontario", "Quebec", "New Brunswick", "Nova Scotia",
            "Prince Edward Island", "Newfoundland",
        ],
        "top_regions": [
            "Alberta", "Saskatchewan", "Manitoba", "British Columbia",
            "Ontario", "Quebec", "New Brunswick", "Nova Scotia",
            "Prince Edward Island", "Newfoundland",
        ],
        "search_terms": [
            "cattle ranch", "cattle farm", "beef ranch", "beef cattle",
            "dairy farm", "cow calf operation", "livestock ranch",
            "cattle breeder", "cattle rancher", "beef producer",
            "cattle operation", "purebred cattle",
            "feedlot", "ranch email", "cow farm",
            "family ranch", "commercial cattle",
            "seed stock producer", "cattle company",
        ],
        "search_templates": [
            "{term} {region} Canada contact email",
            "{term} {region} Canada email",
            "{term} {region} Canadian email address",
            "{term} {region} Canada contact us",
            "registered {breed} breeder {region} Canada contact",
            "registered {breed} {region} Canada email",
            "{breed} cattle {region} Canada email",
            "{term} bulls for sale {region} Canada email",
            "{term} for sale {region} Canada contact",
            "{term} {region} Canadian farmer email",
            "{term} {region} Canada owner contact",
            "{breed} breeder {region} Canada email address",
            "buy {breed} cattle {region} Canada email",
            "{term} near {region} Canada contact email",
        ],
        "breeds": [
            "angus", "hereford", "charolais", "simmental", "limousin",
            "shorthorn", "red angus", "gelbvieh", "maine-anjou",
            "highland", "wagyu", "speckle park", "piedmontese",
            "blonde d'aquitaine", "salers",
            "holstein", "jersey", "brown swiss", "ayrshire",
        ],
        "directories": {
            "yellowpages": "https://www.yellowpages.ca/search",
        },
    },

    # ---------- Australia ----------
    "AU": {
        "name": "Australia",
        "regions": [
            "New South Wales", "Queensland", "Victoria", "South Australia",
            "Western Australia", "Tasmania", "Northern Territory",
        ],
        "top_regions": [
            "Queensland", "New South Wales", "Victoria", "South Australia",
            "Western Australia", "Tasmania", "Northern Territory",
        ],
        "search_terms": [
            "cattle station", "cattle farm", "beef farm", "cattle property",
            "cattle breeder", "beef producer", "cattle stud",
            "beef breeder", "cattle farmer", "livestock farm",
            "stud cattle", "bull breeder", "grazing property",
            "farm email", "farmer contact", "rural property",
            "beef cattle farm", "commercial cattle",
            "cattle company", "pastoral company",
        ],
        "search_templates": [
            "{term} {region} Australia contact email",
            "{term} {region} Australia email",
            "{term} {region} Australian email address",
            "{term} {region} Australia contact",
            "registered {breed} breeder {region} Australia contact",
            "registered {breed} stud {region} Australia email",
            "{breed} cattle {region} Australia email",
            "{term} for sale {region} Australia contact",
            "cattle stud {region} Australia email",
            "{term} {region} Australian farmer email",
            "{term} {region} Australia owner contact",
            "{breed} stud {region} Australia email address",
            "buy {breed} cattle {region} Australia email",
            "{term} near {region} Australia contact email",
        ],
        "breeds": [
            "angus", "hereford", "charolais", "simmental", "limousin",
            "brahman", "shorthorn", "murray grey", "droughtmaster",
            "santa gertrudis", "red angus", "belmont red", "brangus",
            "wagyu", "speckle park", "south devon",
            "holstein", "jersey", "illawarra", "ayrshire",
        ],
        "directories": {
            "yellowpages": "https://www.yellowpages.com.au/find/",
        },
    },
}


# ============================================================================
# BACKWARD-COMPATIBLE ALIASES (keep existing US code working)
# ============================================================================

US_STATES = COUNTRY_CONFIG["US"]["regions"]
TOP_CATTLE_STATES = COUNTRY_CONFIG["US"]["top_regions"]
SEARCH_TEMPLATES = COUNTRY_CONFIG["US"]["search_templates"]
SEARCH_TERMS = COUNTRY_CONFIG["US"]["search_terms"]
CATTLE_BREEDS = COUNTRY_CONFIG["US"]["breeds"]

# === Directory URLs (US) ===
YELLOWPAGES_BASE = "https://www.yellowpages.com/search"
YELP_BASE = "https://www.yelp.com/search"

# === CSV Fields ===
CSV_FIELDS = [
    "farm_name", "owner_name", "email", "phone",
    "address", "city", "state", "zip_code", "country",
    "website", "facebook", "instagram",
    "cattle_type", "breed", "head_count",
    "source_url", "scraped_date",
]

# === Logging ===
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")


def get_country_config(country_code: str) -> dict:
    """Get configuration for a specific country."""
    return COUNTRY_CONFIG.get(country_code, COUNTRY_CONFIG["US"])


def get_all_active_countries() -> list[str]:
    """Get list of active country codes."""
    return [c.strip() for c in ACTIVE_COUNTRIES if c.strip() in COUNTRY_CONFIG]
