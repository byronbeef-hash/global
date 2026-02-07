"""Extract cattle-specific metadata from page text."""

import re
import logging

logger = logging.getLogger(__name__)

# Cattle types
BEEF_KEYWORDS = [
    "beef", "beef cattle", "cow-calf", "cow calf", "feeder cattle",
    "stocker", "feedlot", "slaughter", "meat", "steaks",
]
DAIRY_KEYWORDS = [
    "dairy", "dairy cattle", "milk", "milking", "creamery",
    "dairy farm", "dairy cow",
]

# Breed patterns
BREED_PATTERNS = {
    "Angus": r"\b(?:black\s+)?angus\b",
    "Red Angus": r"\bred\s+angus\b",
    "Hereford": r"\bhereford\b",
    "Charolais": r"\bcharolais\b",
    "Simmental": r"\bsimmental\b",
    "Limousin": r"\blimousin\b",
    "Brahman": r"\bbrahman\b",
    "Shorthorn": r"\bshorthorn\b",
    "Gelbvieh": r"\bgelbvieh\b",
    "Maine-Anjou": r"\bmaine[\s-]anjou\b",
    "Holstein": r"\bholstein\b",
    "Jersey": r"\bjersey\s+(?:cattle|cow|dairy|milk)\b",
    "Guernsey": r"\bguernsey\b",
    "Brown Swiss": r"\bbrown\s+swiss\b",
    "Ayrshire": r"\bayrshire\b",
    "Longhorn": r"\blonghorn\b",
    "Highland": r"\bhighland\s+(?:cattle|cow)\b",
    "Wagyu": r"\bwagyu\b",
    "Brangus": r"\bbrangus\b",
    "Beefmaster": r"\bbeefmaster\b",
    "Santa Gertrudis": r"\bsanta\s+gertrudis\b",
    "Corriente": r"\bcorriente\b",
    "Dexter": r"\bdexter\s+(?:cattle|cow)\b",
}

# Head count patterns
HEAD_COUNT_PATTERNS = [
    r"(\d[\d,]*)\s*(?:head)\b",
    r"(?:herd\s+(?:of|size)?:?\s*)(\d[\d,]*)",
    r"(\d[\d,]*)\s*(?:cattle|cows|calves|pairs|bulls)\b",
    r"(?:running|raise|raising|run)\s+(\d[\d,]*)\s*(?:head|cattle|cows)",
]

# Acreage patterns
ACREAGE_PATTERNS = [
    r"(\d[\d,]*)\s*(?:acres?|ac)\b",
    r"(\d[\d,]*)\s*(?:acre)\s*(?:ranch|farm|operation)",
]


class MetadataExtractor:
    """Extract cattle-specific metadata from page text."""

    def extract(self, text: str) -> dict:
        """Return metadata dict with cattle_type, breed, head_count, acreage."""
        text_lower = text.lower()
        return {
            "cattle_type": self._detect_cattle_type(text_lower),
            "breed": self._detect_breeds(text_lower),
            "head_count": self._detect_head_count(text_lower),
        }

    def _detect_cattle_type(self, text: str) -> str:
        """Detect beef, dairy, or both."""
        is_beef = any(kw in text for kw in BEEF_KEYWORDS)
        is_dairy = any(kw in text for kw in DAIRY_KEYWORDS)
        if is_beef and is_dairy:
            return "beef/dairy"
        if is_beef:
            return "beef"
        if is_dairy:
            return "dairy"
        if "cattle" in text or "ranch" in text:
            return "beef"
        return ""

    def _detect_breeds(self, text: str) -> str:
        """Detect cattle breeds mentioned on the page."""
        found = []
        for breed, pattern in BREED_PATTERNS.items():
            if re.search(pattern, text, re.IGNORECASE):
                found.append(breed)
        return ", ".join(found) if found else ""

    def _detect_head_count(self, text: str) -> str:
        """Try to extract head count from text."""
        for pattern in HEAD_COUNT_PATTERNS:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                count_str = match.group(1).replace(",", "")
                try:
                    count = int(count_str)
                    if 1 <= count <= 500000:
                        return str(count)
                except ValueError:
                    continue
        return ""
