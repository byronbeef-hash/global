"""FastAPI dashboard for monitoring and managing the cattle scraper."""

import csv
import io
import logging
import urllib.parse
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from app.config import TOP_CATTLE_STATES, US_STATES, CSV_FIELDS
from app.dashboard.auth import APIKeyMiddleware
from app.db import queries as db
from app.worker.job_manager import JobManager

logger = logging.getLogger(__name__)

TEMPLATES_DIR = Path(__file__).parent / "templates"

app = FastAPI(title="Cattle Scraper Dashboard")
app.add_middleware(APIKeyMiddleware)

job_manager = JobManager()


def render_template(name: str, **kwargs) -> HTMLResponse:
    """Simple template renderer — reads HTML and does string substitution."""
    template_path = TEMPLATES_DIR / name
    html = template_path.read_text(encoding="utf-8")
    for key, value in kwargs.items():
        html = html.replace(f"{{{{{key}}}}}", str(value))
    return HTMLResponse(content=html)


# ── Health Check ──────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}


# ── Dashboard Pages ───────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def home():
    stats = db.get_dashboard_stats()
    country_data = db.get_emails_by_country_and_state()

    # Country flag emojis
    flags = {"US": "🇺🇸", "NZ": "🇳🇿", "UK": "🇬🇧", "CA": "🇨🇦", "AU": "🇦🇺"}

    # Build country sections with expandable state tables
    country_sections = ""
    if country_data:
        for code, info in country_data.items():
            flag = flags.get(code, "🌍")
            name = info["name"]
            total = info["total"]
            states = info["states"]

            # Build state rows for this country
            state_rows_html = ""
            for s in states:
                encoded_state = urllib.parse.quote(s["state"])
                state_rows_html += (
                    f"<tr class='state-row state-row-{code}' style='display:none;'>"
                    f"<td style='padding-left:2.5rem;'>{s['state']}</td>"
                    f"<td>{s['count']:,}</td>"
                    f"<td><a class='view-link' href='/recent?country={code}&state={encoded_state}'>View &rarr;</a></td>"
                    f"</tr>"
                )

            expand_icon = "▶" if states else ""
            clickable = f"onclick=\"toggleStates('{code}', event)\" style=\"cursor:pointer;\"" if states else ""

            country_sections += (
                f"<tr class='country-row' {clickable}>"
                f"<td><span class='expand-icon' id='icon-{code}'>{expand_icon}</span> "
                f"{flag} <strong>{name}</strong> ({code})</td>"
                f"<td><strong>{total:,}</strong></td>"
                f"<td><a class='view-link' href='/recent?country={code}' onclick=\"viewCountry('{code}', event)\">View &rarr;</a></td>"
                f"</tr>"
                f"{state_rows_html}"
            )
    else:
        country_sections = "<tr><td colspan='3'>No data yet</td></tr>"

    return render_template(
        "home.html",
        total_emails=f"{stats.get('total_emails', 0):,}",
        total_urls=f"{stats.get('total_urls', 0):,}",
        urls_pending=f"{stats.get('urls_pending', 0):,}",
        urls_completed=f"{stats.get('urls_completed', 0):,}",
        urls_failed=f"{stats.get('urls_failed', 0):,}",
        active_jobs=stats.get("active_jobs", 0),
        completed_jobs=stats.get("completed_jobs", 0),
        emails_today=f"{stats.get('emails_today', 0):,}",
        emails_this_week=f"{stats.get('emails_this_week', 0):,}",
        country_sections=country_sections,
    )


@app.get("/recent", response_class=HTMLResponse)
async def recent(
    country: str = Query(default="", description="Filter by country code"),
    state: str = Query(default="", description="Filter by state"),
):
    contacts = db.get_recent_contacts_filtered(
        country=country, state=state, limit=200,
    )

    # Build filter description
    filter_desc = ""
    if country or state:
        parts = []
        if country:
            from app.config import COUNTRY_CONFIG
            cname = COUNTRY_CONFIG.get(country, {}).get("name", country)
            parts.append(cname)
        if state:
            parts.append(state)
        filter_desc = " — " + ", ".join(parts)

    rows = ""
    for c in contacts:
        country_code = c.get("country", "US")
        rows += (
            f"<tr>"
            f"<td>{c.get('email', '')}</td>"
            f"<td>{c.get('farm_name', '')}</td>"
            f"<td>{country_code}</td>"
            f"<td>{c.get('state', '')}</td>"
            f"<td>{c.get('cattle_type', '')}</td>"
            f"<td>{c.get('breed', '')}</td>"
            f"<td><a href='{c.get('source_url', '')}' target='_blank'>link</a></td>"
            f"<td>{c.get('created_at', '')[:19]}</td>"
            f"</tr>"
        )

    if not rows:
        rows = "<tr><td colspan='8'>No contacts found</td></tr>"

    return render_template(
        "recent.html",
        contact_rows=rows,
        filter_desc=filter_desc,
        result_count=f"{len(contacts):,}",
        filter_country=country,
        filter_state=state,
    )


@app.get("/export", response_class=HTMLResponse)
async def export_page():
    state_data = db.get_emails_per_state()

    state_options = '<option value="">All States</option>'
    if state_data:
        for row in state_data:
            state_options += f'<option value="{row["state"]}">{row["state"]} ({row["count"]:,})</option>'

    total = db.get_contact_count()
    return render_template(
        "export.html",
        total_contacts=f"{total:,}",
        state_options=state_options,
    )


@app.get("/export/csv")
async def export_csv(state: str = Query(default="", description="Filter by state")):
    """Download contacts as CSV."""
    if state:
        contacts = db.get_contacts_by_state(state)
    else:
        contacts = db.get_all_contacts()

    def generate():
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        yield output.getvalue()
        output.seek(0)
        output.truncate(0)

        for contact in contacts:
            writer.writerow(contact)
            yield output.getvalue()
            output.seek(0)
            output.truncate(0)

    filename = f"cattle_contacts_{state or 'all'}_{datetime.now().strftime('%Y%m%d')}.csv"
    return StreamingResponse(
        generate(),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.get("/export/emails")
async def export_emails(state: str = Query(default="", description="Filter by state")):
    """Download emails only as text file."""
    if state:
        contacts = db.get_contacts_by_state(state)
    else:
        contacts = db.get_all_contacts()

    emails = "\n".join(c["email"] for c in contacts if c.get("email"))
    filename = f"emails_{state or 'all'}_{datetime.now().strftime('%Y%m%d')}.txt"
    return StreamingResponse(
        io.StringIO(emails),
        media_type="text/plain",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.get("/jobs", response_class=HTMLResponse)
async def jobs_page():
    all_jobs = job_manager.get_all_jobs(limit=50)

    rows = ""
    for j in all_jobs:
        status_class = {
            "queued": "status-queued",
            "running": "status-running",
            "completed": "status-completed",
            "failed": "status-failed",
        }.get(j["status"], "")

        states_str = ", ".join(j.get("states", [])[:5])
        if len(j.get("states", [])) > 5:
            states_str += f" +{len(j['states']) - 5} more"

        rows += (
            f"<tr>"
            f"<td>{j['id']}</td>"
            f"<td>{j['job_type']}</td>"
            f"<td>{states_str}</td>"
            f"<td><span class='{status_class}'>{j['status']}</span></td>"
            f"<td>{j.get('query_index', 0)}/{j.get('total_queries', 0)}</td>"
            f"<td>{j.get('urls_discovered', 0):,}</td>"
            f"<td>{j.get('urls_processed', 0):,}</td>"
            f"<td>{j.get('emails_found', 0):,}</td>"
            f"<td>{(j.get('created_at', '')[:19]) if j.get('created_at') else ''}</td>"
            f"</tr>"
        )

    if not rows:
        rows = "<tr><td colspan='9'>No jobs yet</td></tr>"

    # Build state checkboxes for the new job form
    state_checks = ""
    for s in TOP_CATTLE_STATES:
        state_checks += f'<label><input type="checkbox" name="states" value="{s}" checked> {s}</label> '

    return render_template("jobs.html", job_rows=rows, state_checks=state_checks)


@app.post("/jobs")
async def create_job(request: Request):
    """Create a new scrape job."""
    data = await request.json()
    job_type = data.get("job_type", "full")
    states = data.get("states", TOP_CATTLE_STATES)

    if isinstance(states, str):
        states = [states]

    # Generate query count for the response
    from app.discovery.search_discovery import SearchDiscovery
    search = SearchDiscovery()
    queries = search.generate_queries(states)

    job_id = job_manager.create_job(
        job_type=job_type,
        states=states,
        total_queries=len(queries),
    )

    return JSONResponse({
        "job_id": job_id,
        "job_type": job_type,
        "states": states,
        "total_queries": len(queries),
        "status": "queued",
    })


@app.get("/stats", response_class=HTMLResponse)
async def stats_page():
    stats = db.get_dashboard_stats()
    state_data = db.get_emails_per_state()

    # Top domains
    contacts = db.get_recent_contacts(limit=1000)
    domain_counts: dict[str, int] = {}
    for c in contacts:
        email = c.get("email", "")
        if "@" in email:
            domain = email.split("@")[1]
            domain_counts[domain] = domain_counts.get(domain, 0) + 1

    top_domains = sorted(domain_counts.items(), key=lambda x: -x[1])[:20]
    domain_rows = ""
    for domain, count in top_domains:
        domain_rows += f"<tr><td>{domain}</td><td>{count}</td></tr>"

    if not domain_rows:
        domain_rows = "<tr><td colspan='2'>No data yet</td></tr>"

    # State chart data (JSON for chart.js)
    state_labels = "[]"
    state_values = "[]"
    if state_data:
        state_labels = "[" + ",".join(f'"{r["state"]}"' for r in state_data[:15]) + "]"
        state_values = "[" + ",".join(str(r["count"]) for r in state_data[:15]) + "]"

    return render_template(
        "stats.html",
        total_emails=f"{stats.get('total_emails', 0):,}",
        domain_rows=domain_rows,
        state_labels=state_labels,
        state_values=state_values,
    )


# ── API Endpoints ─────────────────────────────────────────────────────

@app.get("/api/stats")
async def api_stats():
    """JSON stats endpoint."""
    return db.get_dashboard_stats()


@app.get("/api/emails-per-state")
async def api_emails_per_state():
    """JSON emails per state."""
    return db.get_emails_per_state()


@app.get("/api/emails-by-country")
async def api_emails_by_country():
    """JSON emails grouped by country with state breakdown."""
    return db.get_emails_by_country_and_state()


@app.get("/api/recent")
async def api_recent(
    country: str = Query(default="", description="Filter by country"),
    state: str = Query(default="", description="Filter by state"),
    limit: int = Query(default=100, description="Limit results"),
):
    """JSON recent contacts with optional filters."""
    contacts = db.get_recent_contacts_filtered(
        country=country, state=state, limit=limit,
    )
    return {"count": len(contacts), "contacts": contacts}


@app.get("/api/performance")
async def api_performance():
    """JSON performance metrics: collection rates and timing."""
    return db.get_performance_metrics()


@app.get("/api/debug/url-status")
async def api_debug_url_status():
    """Debug: show URL count by status."""
    statuses = {}
    for s in ["pending", "processing", "completed", "failed"]:
        statuses[s] = db.get_url_count_by_status(s)
    statuses["total"] = db.get_url_count()
    return statuses


@app.get("/api/debug/process-one")
async def api_debug_process_one():
    """Debug: pick one pending URL, process it, and return full trace."""
    import traceback
    from app.scraper.page_fetcher import PageFetcher
    from app.scraper.contact_extractor import ContactExtractor
    from app.utils.rate_limiter import RateLimiter
    from app.utils.robots_checker import RobotsChecker
    from app.scraper.proxy_manager import ProxyManager

    trace = {"steps": []}

    # Get one pending URL
    try:
        pending = db.get_pending_urls(limit=1)
        if not pending:
            return {"error": "No pending URLs", "trace": trace}
        url = pending[0]
        trace["url"] = url
        trace["steps"].append("Got pending URL")
    except Exception as e:
        return {"error": f"get_pending_urls failed: {e}", "trace": trace}

    # Try to fetch it
    try:
        fetcher = PageFetcher(RateLimiter(), RobotsChecker(), ProxyManager())
        trace["steps"].append("Created fetcher, starting fetch...")
        result = await fetcher.fetch(url)
        trace["steps"].append(f"Fetch done: success={result.success}, status={result.status_code}, html_len={len(result.html)}, error={result.error}")
        await fetcher.close()
    except Exception as e:
        trace["steps"].append(f"Fetch exception: {e}")
        trace["traceback"] = traceback.format_exc()
        return {"error": str(e), "trace": trace}

    if not result.success:
        db.mark_url_done(url, emails_found=0, error=result.error or "fetch failed")
        trace["steps"].append("Marked URL as failed")
        return {"result": "fetch_failed", "trace": trace}

    # Try to extract contacts
    try:
        extractor = ContactExtractor()
        contact = extractor.extract(result.html, url)
        trace["steps"].append(f"Extracted: emails={contact.emails}, farm={contact.farm_name}, phones={contact.phones}")
    except Exception as e:
        trace["steps"].append(f"Extract exception: {e}")
        trace["traceback"] = traceback.format_exc()
        db.mark_url_done(url, emails_found=0, error=str(e))
        return {"error": str(e), "trace": trace}

    # Save if emails found
    if contact.emails:
        for email in contact.emails:
            record = {
                "email": email,
                "farm_name": contact.farm_name,
                "owner_name": contact.owner_name,
                "phone": contact.phones[0] if contact.phones else "",
                "source_url": url,
                "country": "US",
            }
            saved = db.upsert_contact(record)
            trace["steps"].append(f"Upserted {email}: {saved}")
        db.mark_url_done(url, emails_found=len(contact.emails))
        trace["steps"].append(f"Marked URL done with {len(contact.emails)} emails")
    else:
        db.mark_url_done(url, emails_found=0)
        trace["steps"].append("No emails found, marked URL done")

    return {"result": "success", "emails": contact.emails, "trace": trace}
