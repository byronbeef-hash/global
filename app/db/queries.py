"""All database query operations for the cattle scraper."""

import logging
import time
from datetime import datetime

from app.db.supabase_client import get_client

logger = logging.getLogger(__name__)

# Simple in-memory cache for expensive queries
_cache: dict[str, tuple[float, any]] = {}
CACHE_TTL = 30  # seconds


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


def get_contacts_by_country(country: str, limit: int = 10000) -> list[dict]:
    """Get all contacts for a country."""
    client = get_client()
    result = (
        client.table("contacts")
        .select("*")
        .eq("country", country)
        .order("created_at", desc=True)
        .limit(limit)
        .execute()
    )
    return result.data or []


def get_recent_contacts_filtered(
    country: str = "",
    state: str = "",
    limit: int = 100,
) -> list[dict]:
    """Get recent contacts with optional country/state filter."""
    client = get_client()
    query = client.table("contacts").select("*")
    if country:
        query = query.eq("country", country)
    if state:
        query = query.eq("state", state)
    result = query.order("created_at", desc=True).limit(limit).execute()
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

    Uses RPC function for efficiency (single SQL query instead of paginating
    all rows). Results are cached for 30s to avoid hammering the DB.

    Shows ALL regions from config for each country, with 0 for regions
    that don't have contacts yet.
    """
    # Check cache first
    cached = _cache.get("emails_by_country_state")
    if cached:
        cache_time, cache_data = cached
        if time.time() - cache_time < CACHE_TTL:
            return cache_data

    from app.config import COUNTRY_CONFIG, ACTIVE_COUNTRIES

    client = get_client()

    # Try RPC first (efficient server-side GROUP BY)
    rows = []
    try:
        result = client.rpc("get_emails_by_country_state").execute()
        rows = result.data or []
    except Exception:
        # Fallback: just get country totals (5 fast count queries).
        # Per-state breakdown requires the RPC function to be created
        # in Supabase (the paginated approach is too slow at 42K+ rows).
        try:
            for code in ACTIVE_COUNTRIES:
                count_result = (
                    client.table("contacts")
                    .select("id", count="exact")
                    .eq("country", code)
                    .limit(1)
                    .execute()
                )
                total = count_result.count or 0
                if total > 0:
                    rows.append({"country": code, "state": "", "cnt": total})
        except Exception as e:
            logger.error(f"Failed to fetch country/state data: {e}")

    # Build nested counts from RPC result
    country_state_counts: dict[str, dict[str, int]] = {}
    country_totals: dict[str, int] = {}
    for row in rows:
        c = row.get("country") or "US"
        s = row.get("state") or "Unknown"
        cnt = row.get("cnt", 0)
        if c not in country_state_counts:
            country_state_counts[c] = {}
            country_totals[c] = 0
        country_state_counts[c][s] = cnt
        country_totals[c] += cnt

    # Build result showing ALL regions from config for each country
    result_data = {}
    for code in ACTIVE_COUNTRIES:
        cfg = COUNTRY_CONFIG.get(code, {})
        all_regions = cfg.get("regions", [])
        db_states = country_state_counts.get(code, {})

        # Start with all configured regions (count=0 by default)
        region_counts = {r: 0 for r in all_regions}

        # Merge in actual counts from DB
        for s, cnt in db_states.items():
            if s in region_counts:
                region_counts[s] = cnt
            elif s == "Unknown" or s == "":
                # Skip unknown/empty states in the region list
                pass
            else:
                # State from DB not in config — include it anyway
                region_counts[s] = cnt

        # Sort: regions with contacts first (desc), then alphabetical
        states_list = [
            {"state": s, "count": n}
            for s, n in sorted(
                region_counts.items(),
                key=lambda x: (-x[1], x[0]),
            )
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

    # Cache the result
    _cache["emails_by_country_state"] = (time.time(), result_data)
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


def get_pending_urls(limit: int = 50, country: str = "") -> list[str]:
    """Get next batch of pending URLs to process, optionally filtered by country.

    When country is provided, only URLs discovered for that country are returned.
    This prevents cross-contamination (e.g. UK job processing US-discovered URLs).
    """
    client = get_client()

    try:
        query = (
            client.table("urls")
            .select("url")
            .eq("status", "pending")
        )
        if country:
            # Try country filter; fall back to unfiltered if column missing
            try:
                result = query.eq("country", country).order("created_at").limit(limit).execute()
            except Exception:
                result = query.order("created_at").limit(limit).execute()
        else:
            result = query.order("created_at").limit(limit).execute()
    except Exception as e:
        logger.error(f"get_pending_urls error: {e}")
        return []

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


def is_url_completed(url: str) -> bool:
    """Check if a URL has already been fully processed (completed or failed)."""
    client = get_client()
    result = (
        client.table("urls")
        .select("status")
        .eq("url", url)
        .limit(1)
        .execute()
    )
    if not result.data:
        return False
    return result.data[0].get("status") in ("completed", "failed")


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


def reset_stuck_urls() -> int:
    """Reset URLs stuck in 'processing' back to 'pending'.

    Called on startup to recover from prior crashes/restarts.
    Processes in batches to avoid OOM on large result sets.
    Returns total number of URLs reset.
    """
    client = get_client()
    total_reset = 0

    while True:
        # Fetch a batch of stuck URLs
        result = (
            client.table("urls")
            .select("id")
            .eq("status", "processing")
            .limit(200)
            .execute()
        )
        ids = [row["id"] for row in (result.data or [])]
        if not ids:
            break

        # Reset each batch
        try:
            client.table("urls").update(
                {"status": "pending"}
            ).in_("id", ids).execute()
            total_reset += len(ids)
        except Exception as e:
            logger.error(f"Failed to reset stuck URLs batch: {e}")
            break

    return total_reset


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


def reset_orphaned_jobs() -> int:
    """Reset jobs stuck in 'running' to 'failed' on startup.

    Returns the number of jobs reset.
    """
    client = get_client()
    try:
        result = (
            client.table("scrape_jobs")
            .update({"status": "failed", "error": "orphaned by restart"})
            .eq("status", "running")
            .execute()
        )
        return len(result.data) if result.data else 0
    except Exception as e:
        logger.error(f"Failed to reset orphaned jobs: {e}")
        return 0


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


def get_all_queued_jobs() -> list[dict]:
    """Get ALL queued jobs in a single query (for concurrent execution)."""
    client = get_client()
    result = (
        client.table("scrape_jobs")
        .select("*")
        .eq("status", "queued")
        .order("created_at")
        .execute()
    )
    return result.data or []


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


# ── Performance Metrics ──────────────────────────────────────────────

def get_performance_metrics() -> dict:
    """Get performance metrics: emails added in recent time windows.

    Returns dict with counts for last 1min, 5min, 15min, 1hr, 24hr,
    plus overall rate calculations.
    """
    from datetime import datetime, timedelta, timezone

    client = get_client()
    now = datetime.now(timezone.utc)

    windows = {
        "last_1min": now - timedelta(minutes=1),
        "last_5min": now - timedelta(minutes=5),
        "last_15min": now - timedelta(minutes=15),
        "last_1hr": now - timedelta(hours=1),
        "last_24hr": now - timedelta(hours=24),
    }

    # Email counts per time window
    email_counts = {}
    for key, since in windows.items():
        try:
            result = (
                client.table("contacts")
                .select("id", count="exact")
                .gte("created_at", since.isoformat())
                .execute()
            )
            email_counts[key] = result.count or 0
        except Exception as e:
            logger.error(f"Performance metric {key} failed: {e}")
            email_counts[key] = 0

    # URL counts per time window
    url_counts = {}
    for key, since in windows.items():
        try:
            result = (
                client.table("urls")
                .select("id", count="exact")
                .gte("created_at", since.isoformat())
                .execute()
            )
            url_counts[f"urls_{key}"] = result.count or 0
        except Exception as e:
            logger.error(f"URL metric {key} failed: {e}")
            url_counts[f"urls_{key}"] = 0

    # Total counts
    total_emails = 0
    total_urls = 0
    urls_completed = 0
    urls_pending = 0
    try:
        result = client.table("contacts").select("id", count="exact").execute()
        total_emails = result.count or 0
    except Exception:
        pass
    try:
        result = client.table("urls").select("id", count="exact").execute()
        total_urls = result.count or 0
    except Exception:
        pass
    try:
        result = client.table("urls").select("id", count="exact").eq("status", "completed").execute()
        urls_completed = result.count or 0
    except Exception:
        pass
    try:
        result = client.table("urls").select("id", count="exact").eq("status", "pending").execute()
        urls_pending = result.count or 0
    except Exception:
        pass

    # Calculate rates
    emails_per_minute = email_counts["last_5min"] / 5 if email_counts["last_5min"] else 0
    emails_per_hour = email_counts["last_1hr"]
    urls_per_minute = url_counts["urls_last_5min"] / 5 if url_counts["urls_last_5min"] else 0
    urls_per_hour = url_counts["urls_last_1hr"]

    # Get the very first contact timestamp for uptime calc
    first_contact_time = None
    try:
        result = (
            client.table("contacts")
            .select("created_at")
            .order("created_at")
            .limit(1)
            .execute()
        )
        if result.data:
            first_contact_time = result.data[0]["created_at"]
    except Exception:
        pass

    # Get scraper start time (oldest running job or first contact)
    scraper_start = None
    try:
        result = (
            client.table("scrape_jobs")
            .select("started_at")
            .eq("status", "running")
            .order("started_at")
            .limit(1)
            .execute()
        )
        if result.data and result.data[0].get("started_at"):
            scraper_start = result.data[0]["started_at"]
    except Exception:
        pass

    return {
        **email_counts,
        **url_counts,
        "total_emails": total_emails,
        "total_urls": total_urls,
        "urls_completed": urls_completed,
        "urls_pending": urls_pending,
        "per_minute": round(emails_per_minute, 1),
        "per_hour": emails_per_hour,
        "urls_per_minute": round(urls_per_minute, 1),
        "urls_per_hour": urls_per_hour,
        "scraper_start": scraper_start,
        "first_contact": first_contact_time,
    }
