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
DEFAULT_RATE_LIMIT = float(os.environ.get("DEFAULT_RATE_LIMIT", "3.0"))
SEARCH_RATE_LIMIT = float(os.environ.get("SEARCH_RATE_LIMIT", "5.0"))
DIRECTORY_RATE_LIMIT = float(os.environ.get("DIRECTORY_RATE_LIMIT", "4.0"))
MAX_CONCURRENT_REQUESTS = int(os.environ.get("MAX_CONCURRENT_REQUESTS", "5"))
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
WORKER_BATCH_SIZE = int(os.environ.get("WORKER_BATCH_SIZE", "50"))
WORKER_SLEEP_BETWEEN_JOBS = int(os.environ.get("WORKER_SLEEP_BETWEEN_JOBS", "60"))

# === US States ===
US_STATES = [
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
]

TOP_CATTLE_STATES = [
    "Texas", "Nebraska", "Kansas", "California", "Oklahoma",
    "South Dakota", "Missouri", "Iowa", "Colorado", "Montana",
    "North Dakota", "Idaho", "Wisconsin", "Minnesota", "Kentucky",
    "Wyoming", "Florida", "Oregon", "Virginia", "Georgia",
]

# === Search Query Templates ===
SEARCH_TEMPLATES = [
    "{term} {state} contact email",
    "{term} {state} gmail",
    "{term} {state} email address",
    "{term} {state} contact us",
    "{term} {state} contact information",
    "registered {breed} breeder {state} contact",
    "registered {breed} {state} email",
    "{term} bulls for sale {state} email contact",
    "{term} for sale {state} contact",
]

SEARCH_TERMS = [
    "cattle ranch",
    "cattle farm",
    "beef ranch",
    "beef cattle",
    "dairy farm",
    "cow calf operation",
    "livestock ranch",
    "angus ranch",
    "hereford ranch",
    "cattle breeder",
    "cattle rancher",
    "beef producer",
    "cattle operation",
]

CATTLE_BREEDS = [
    "angus", "hereford", "charolais", "simmental", "limousin",
    "brahman", "shorthorn", "red angus", "gelbvieh", "maine-anjou",
    "holstein", "jersey", "guernsey", "brown swiss", "ayrshire",
    "longhorn", "highland", "wagyu", "brangus", "beefmaster",
]

# === Directory URLs ===
YELLOWPAGES_BASE = "https://www.yellowpages.com/search"
YELP_BASE = "https://www.yelp.com/search"

# === CSV Fields ===
CSV_FIELDS = [
    "farm_name", "owner_name", "email", "phone",
    "address", "city", "state", "zip_code",
    "website", "facebook", "instagram",
    "cattle_type", "breed", "head_count",
    "source_url", "scraped_date",
]

# === Logging ===
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
