"""Data models for the cattle scraper."""

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class Contact:
    """A cattle farm contact record."""
    email: str
    farm_name: str = ""
    owner_name: str = ""
    phone: str = ""
    address: str = ""
    city: str = ""
    state: str = ""
    zip_code: str = ""
    website: str = ""
    facebook: str = ""
    instagram: str = ""
    cattle_type: str = ""
    breed: str = ""
    head_count: str = ""
    source_url: str = ""
    created_at: str = ""

    def to_dict(self) -> dict:
        return {
            "email": self.email,
            "farm_name": self.farm_name,
            "owner_name": self.owner_name,
            "phone": self.phone,
            "address": self.address,
            "city": self.city,
            "state": self.state,
            "zip_code": self.zip_code,
            "website": self.website,
            "facebook": self.facebook,
            "instagram": self.instagram,
            "cattle_type": self.cattle_type,
            "breed": self.breed,
            "head_count": self.head_count,
            "source_url": self.source_url,
        }


@dataclass
class ScrapeJob:
    """A scraping job definition."""
    id: int | None = None
    job_type: str = "full"
    states: list[str] = field(default_factory=list)
    status: str = "queued"
    query_index: int = 0
    total_queries: int = 0
    urls_discovered: int = 0
    urls_processed: int = 0
    emails_found: int = 0
    error: str = ""
    started_at: str | None = None
    completed_at: str | None = None
    created_at: str | None = None
