"""All database query operations for the cattle scraper."""

import logging
from datetime import datetime

from app.db.supabase_client import get_client

logger = logging.getLogger(__name__)


# ── Contacts ──────────────────────────────────────────────────────────

def upsert_contact(record: dict) -> bool:
    """Insert a contact or update if email already exists.

    Returns True if a new contact was inserted, False if updated/skipped.
    """
    client = get_client()
    email = record.get("email", "").strip().lower()
    if not email:
        return False

    # Clean the record
    data = {
        "email": email,
        "farm_name": record.get("farm_name", ""),
        "owner_name": record.get("owner_name", ""),
        "phone": record.get("phone", ""),
        "address": record.get("address", ""),
        "city": record.get("city", ""),
        "state": record.get("state", ""),
        "zip_code": record.get("zip_code", ""),
        "country": record.get("country", "US"),
        "website": record.get("website", ""),
        "facebook": record.get("facebook", ""),
        "instagram": record.get("instagram", ""),
        "cattle_type": record.get("cattle_type", ""),
        "breed": record.get("breed", ""),
        "head_count": record.get("head_count", ""),
        "source_url": record.get("source_url", ""),
    }

    try:
        result = client.table("contacts").upsert(
            data,
            on_conflict="email",
        ).execute()
        return True
    except Exception as e:
        logger.error(f"Failed to upsert contact {email}: {e}")
        return False


def get_contact_count() -> int:
    """Get total number of contacts."""
    client = get_client()
    result = client.table("contacts").select("id", count="exact").execute()
    return result.count or 0


def get_recent_contacts(limit: int = 100) -> list[dict]:
    """Get most recently added contacts."""
    client = get_client()
    result = (
        client.table("contacts")
        .select("*")
        .order("created_at", desc=True)
        .limit(limit)
        .execute()
    )
    return result.data or []


def get_contacts_by_state(state: str, limit: int = 10000) -> list[dict]:
    """Get all contacts for a state."""
    client = get_client()
    result = (
        client.table("contacts")
        .select("*")
        .eq("state", state)
        .order("created_at", desc=True)
        .limit(limit)
        .execute()
    )
    return result.data or []


def get_all_contacts(limit: int = 100000) -> list[dict]:
    """Get all contacts (paginated internally)."""
    client = get_client()
    all_data = []
    offset = 0
    page_size = 1000

    while offset < limit:
        fetch_size = min(page_size, limit - offset)
        result = (
            client.table("contacts")
            .select("*")
            .order("created_at", desc=True)
            .range(offset, offset + fetch_size - 1)
            .execute()
        )
        batch = result.data or []
        if not batch:
            break
        all_data.extend(batch)
        offset += len(batch)
        if len(batch) < fetch_size:
            break

    return all_data


def get_emails_per_country() -> list[dict]:
    """Get email count grouped by country."""
    client = get_client()
    try:
        result = client.rpc("get_emails_per_country").execute()
        return result.data or []
    except Exception:
        # Fallback: query manually
        result = client.table("contacts").select("country").execute()
        country_counts: dict[str, int] = {}
        for row in result.data or []:
            c = row.get("country", "US")
            if c:
                country_counts[c] = country_counts.get(c, 0) + 1
        return [{"country": c, "count": n} for c, n in sorted(country_counts.items(), key=lambda x: -x[1])]


def get_emails_by_country_and_state() -> dict:
    """Get email counts grouped by country, then by state/region within each country.

    Returns dict like:
    {
        "US": {"name": "United States", "total": 17243, "states": [{"state": "ND", "count": 1535}, ...]},
        "NZ": {"name": "New Zealand", "total": 0, "states": []},
        ...
    }
    """
    from app.config import COUNTRY_CONFIG, ACTIVE_COUNTRIES

    client = get_client()

    # Get all country+state pairs (paginated — Supabase default limit is 1000)
    rows = []
    try:
        offset = 0
        page_size = 1000
        while True:
            result = (
                client.table("contacts")
                .select("country, state")
                .range(offset, offset + page_size - 1)
                .execute()
            )
            batch = result.data or []
            rows.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size
    except Exception as e:
        logger.error(f"Failed to fetch country/state data: {e}")
        rows = []

    # Build nested counts
    country_state_counts: dict[str, dict[str, int]] = {}
    country_totals: dict[str, int] = {}
    for row in rows:
        c = row.get("country") or "US"
        s = row.get("state") or "Unknown"
        if c not in country_state_counts:
            country_state_counts[c] = {}
            country_totals[c] = 0
        country_state_counts[c][s] = country_state_counts[c].get(s, 0) + 1
        country_totals[c] = country_totals[c] + 1

    # Build result for all active countries (even those with 0 contacts)
    result_data = {}
    for code in ACTIVE_COUNTRIES:
        cfg = COUNTRY_CONFIG.get(code, {})
        states_dict = country_state_counts.get(code, {})
        states_list = [
            {"state": s, "count": n}
            for s, n in sorted(states_dict.items(), key=lambda x: -x[1])
        ]
        result_data[code] = {
            "name": cfg.get("name", code),
            "total": country_totals.get(code, 0),
            "states": states_list,
        }

    # Sort countries by total count descending
    result_data = dict(
        sorted(result_data.items(), key=lambda x: -x[1]["total"])
    )
    return result_data


def get_emails_per_state() -> list[dict]:
    """Get email count grouped by state."""
    client = get_client()
    try:
        result = client.rpc("get_emails_per_state").execute()
        return result.data or []
    except Exception:
        # Fallback: query manually
        result = client.table("contacts").select("state").execute()
        state_counts: dict[str, int] = {}
        for row in result.data or []:
            s = row.get("state", "")
            if s:
                state_counts[s] = state_counts.get(s, 0) + 1
        return [{"state": s, "count": c} for s, c in sorted(state_counts.items(), key=lambda x: -x[1])]


def get_dashboard_stats() -> dict:
    """Get aggregated stats for the dashboard."""
    client = get_client()
    try:
        result = client.rpc("get_dashboard_stats").execute()
        return result.data or {}
    except Exception as e:
        logger.warning(f"Dashboard stats RPC failed, using fallback: {e}")
        # Fallback
        return {
            "total_emails": get_contact_count(),
            "total_urls": get_url_count(),
            "urls_pending": get_url_count_by_status("pending"),
            "urls_completed": get_url_count_by_status("completed"),
            "urls_failed": get_url_count_by_status("failed"),
            "active_jobs": get_job_count_by_status("running"),
            "completed_jobs": get_job_count_by_status("completed"),
        }


# ── URLs ──────────────────────────────────────────────────────────────

def add_urls(urls: list[str], source: str = "", state: str = "", discovered_by: str = "", country: str = "US") -> int:
    """Add discovered URLs to the queue. Returns number of new URLs added."""
    if not urls:
        return 0

    client = get_client()
    added = 0

    # Batch insert, ignoring duplicates
    for i in range(0, len(urls), 100):
        batch = urls[i:i + 100]
        rows = [
            {
                "url": url,
                "status": "pending",
                "source": source,
                "state_target": state,
                "discovered_by": discovered_by,
                "country": country,
            }
            for url in batch
        ]
        try:
            result = client.table("urls").upsert(
                rows,
                on_conflict="url",
                ignore_duplicates=True,
            ).execute()
            added += len(result.data) if result.data else 0
        except Exception as e:
            logger.error(f"Failed to add URL batch: {e}")

    return added


def get_pending_urls(limit: int = 50) -> list[str]:
    """Get next batch of pending URLs to process."""
    client = get_client()

    # Mark them as processing atomically
    result = (
        client.table("urls")
        .select("url")
        .eq("status", "pending")
        .order("created_at")
        .limit(limit)
        .execute()
    )
    urls = [row["url"] for row in (result.data or [])]

    if urls:
        # Mark as processing
        for url in urls:
            try:
                client.table("urls").update({"status": "processing"}).eq("url", url).execute()
            except Exception:
                pass

    return urls


def mark_url_done(url: str, emails_found: int = 0, error: str = "") -> None:
    """Mark a URL as completed or failed."""
    client = get_client()
    status = "failed" if error else "completed"
    try:
        client.table("urls").update({
            "status": status,
            "emails_found": emails_found,
            "error": error,
            "processed_at": datetime.utcnow().isoformat(),
        }).eq("url", url).execute()
    except Exception as e:
        logger.error(f"Failed to mark URL done: {e}")


def is_url_seen(url: str) -> bool:
    """Check if a URL has already been processed or is in the queue."""
    client = get_client()
    result = (
        client.table("urls")
        .select("id")
        .eq("url", url)
        .limit(1)
        .execute()
    )
    return bool(result.data)


def get_url_count() -> int:
    """Get total URL count."""
    client = get_client()
    result = client.table("urls").select("id", count="exact").execute()
    return result.count or 0


def get_url_count_by_status(status: str) -> int:
    """Get URL count for a given status."""
    client = get_client()
    result = client.table("urls").select("id", count="exact").eq("status", status).execute()
    return result.count or 0


# ── Jobs ──────────────────────────────────────────────────────────────

def create_job(job_type: str, states: list[str], total_queries: int = 0, country: str = "US") -> int:
    """Create a new scrape job. Returns the job ID.

    Country is stored in job_type as 'full:NZ' format for non-US countries.
    """
    client = get_client()
    # Encode country into job_type so we can read it back
    encoded_type = f"{job_type}:{country}" if country != "US" else job_type
    data = {
        "job_type": encoded_type,
        "states": states,
        "status": "queued",
        "total_queries": total_queries,
    }
    result = client.table("scrape_jobs").insert(data).execute()
    return result.data[0]["id"]


def start_job(job_id: int) -> None:
    """Mark a job as running."""
    client = get_client()
    client.table("scrape_jobs").update({
        "status": "running",
        "started_at": datetime.utcnow().isoformat(),
    }).eq("id", job_id).execute()


def update_job_progress(
    job_id: int,
    query_index: int | None = None,
    urls_discovered: int | None = None,
    urls_processed: int | None = None,
    emails_found: int | None = None,
) -> None:
    """Update job progress counters."""
    client = get_client()
    data = {}
    if query_index is not None:
        data["query_index"] = query_index
    if urls_discovered is not None:
        data["urls_discovered"] = urls_discovered
    if urls_processed is not None:
        data["urls_processed"] = urls_processed
    if emails_found is not None:
        data["emails_found"] = emails_found
    if data:
        try:
            client.table("scrape_jobs").update(data).eq("id", job_id).execute()
        except Exception as e:
            logger.error(f"Failed to update job {job_id}: {e}")


def complete_job(job_id: int, error: str = "") -> None:
    """Mark a job as completed or failed."""
    client = get_client()
    status = "failed" if error else "completed"
    client.table("scrape_jobs").update({
        "status": status,
        "error": error,
        "completed_at": datetime.utcnow().isoformat(),
    }).eq("id", job_id).execute()


def get_active_jobs() -> list[dict]:
    """Get all running/queued jobs."""
    client = get_client()
    result = (
        client.table("scrape_jobs")
        .select("*")
        .in_("status", ["queued", "running"])
        .order("created_at", desc=True)
        .execute()
    )
    return result.data or []


def get_all_jobs(limit: int = 50) -> list[dict]:
    """Get all jobs, most recent first."""
    client = get_client()
    result = (
        client.table("scrape_jobs")
        .select("*")
        .order("created_at", desc=True)
        .limit(limit)
        .execute()
    )
    return result.data or []


def get_job_count_by_status(status: str) -> int:
    """Get job count for a given status."""
    client = get_client()
    result = client.table("scrape_jobs").select("id", count="exact").eq("status", status).execute()
    return result.count or 0


def get_next_queued_job() -> dict | None:
    """Get the oldest queued job."""
    client = get_client()
    result = (
        client.table("scrape_jobs")
        .select("*")
        .eq("status", "queued")
        .order("created_at")
        .limit(1)
        .execute()
    )
    return result.data[0] if result.data else None


# ── Search Queries ────────────────────────────────────────────────────

def mark_query_done(query: str, results_count: int, urls_found: int, job_id: int | None = None) -> None:
    """Record a completed search query."""
    client = get_client()
    data = {
        "query": query,
        "results_count": results_count,
        "urls_found": urls_found,
    }
    if job_id:
        data["job_id"] = job_id
    try:
        client.table("search_queries").upsert(data, on_conflict="query").execute()
    except Exception as e:
        logger.error(f"Failed to mark query done: {e}")


def is_query_done(query: str) -> bool:
    """Check if a search query has already been executed."""
    client = get_client()
    result = (
        client.table("search_queries")
        .select("id")
        .eq("query", query)
        .limit(1)
        .execute()
    )
    return bool(result.data)


def get_completed_query_count() -> int:
    """Get number of completed search queries."""
    client = get_client()
    result = client.table("search_queries").select("id", count="exact").execute()
    return result.count or 0
