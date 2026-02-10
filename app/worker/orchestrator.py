"""Main worker orchestrator — runs the 24/7 scraping loop (multi-country).

Key design: interleaves URL discovery with email extraction so that
emails start flowing immediately rather than waiting for all discovery
to complete first.
"""

import asyncio
import logging
import signal

from app.config import (
    MAX_CONCURRENT_REQUESTS, WORKER_BATCH_SIZE,
    WORKER_SLEEP_BETWEEN_JOBS, TOP_CATTLE_STATES,
    COUNTRY_CONFIG, get_all_active_countries, get_country_config,
)
from app.db import queries as db
from app.utils.rate_limiter import RateLimiter
from app.utils.robots_checker import RobotsChecker
from app.scraper.proxy_manager import ProxyManager
from app.scraper.page_fetcher import PageFetcher
from app.worker.url_processor import URLProcessor
from app.worker.job_manager import JobManager
from app.discovery.search_discovery import SearchDiscovery
from app.discovery.directory_crawler import YellowPagesCrawler, YelpCrawler, MantaCrawler
from app.discovery.association_crawler import AssociationCrawler

logger = logging.getLogger(__name__)

# How many search queries to run before pausing to process URLs
DISCOVERY_BATCH_SIZE = 10
# How many URL batches to process between discovery batches
PROCESS_BATCHES_PER_CYCLE = 3


class Orchestrator:
    """Main worker that continuously processes scrape jobs.

    Supports multi-country operation. Each job can target a specific country.

    Key improvement: interleaves discovery and processing so emails flow
    from the very first minutes of operation, not after hours of discovery.
    """

    def __init__(self):
        self.rate_limiter = RateLimiter()
        self.robots = RobotsChecker()
        self.proxy = ProxyManager()
        self.fetcher = PageFetcher(self.rate_limiter, self.robots, self.proxy)
        self.processor = URLProcessor(self.fetcher)
        self.job_manager = JobManager()
        self.search = SearchDiscovery()
        self._shutdown = False

    async def run_forever(self) -> None:
        """Main loop: pick jobs, execute them, repeat."""
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._handle_shutdown)

        # Wait 10s for dashboard to start and pass healthcheck first
        logger.info("Worker waiting 10s for dashboard startup...")
        await asyncio.sleep(10)

        logger.info("Orchestrator started — multi-country mode")
        logger.info(f"Active countries: {get_all_active_countries()}")

        # Crash recovery: reset any URLs stuck in 'processing'
        self._recover_stuck_urls()

        while not self._shutdown:
            try:
                job = self.job_manager.get_next_job()

                if job:
                    await self._execute_job(job)
                else:
                    # Before creating new jobs, process any pending URLs first
                    pending_count = db.get_url_count_by_status("pending")
                    if pending_count > 0:
                        logger.info(
                            f"Found {pending_count} pending URLs — processing before new jobs"
                        )
                        await self._process_pending_urls(job_id=0, country="US")

                    created = self._auto_create_jobs()
                    if not created:
                        logger.debug(
                            f"No new jobs to create, sleeping {WORKER_SLEEP_BETWEEN_JOBS}s"
                        )
                        await asyncio.sleep(WORKER_SLEEP_BETWEEN_JOBS)

            except Exception as e:
                logger.error(f"Orchestrator error: {e}", exc_info=True)
                await asyncio.sleep(10)

        logger.info("Orchestrator shutting down gracefully")
        await self.fetcher.close()

    def _recover_stuck_urls(self) -> None:
        """Reset URLs stuck in 'processing' from a previous crash/restart."""
        try:
            # First check how many are stuck
            count = db.get_url_count_by_status("processing")
            if not count:
                return

            logger.info(f"Crash recovery: found {count} stuck URLs, resetting...")

            # Reset in batches of 200 to avoid oversized responses
            from app.db.supabase_client import get_client
            client = get_client()
            total_reset = 0

            while total_reset < count + 100:  # safety limit
                batch = (
                    client.table("urls")
                    .select("url")
                    .eq("status", "processing")
                    .limit(200)
                    .execute()
                )
                if not batch.data:
                    break

                urls = [r["url"] for r in batch.data]
                for url in urls:
                    try:
                        (client.table("urls")
                         .update({"status": "pending"})
                         .eq("url", url)
                         .execute())
                    except Exception:
                        pass
                total_reset += len(urls)
                logger.info(f"  Reset {total_reset}/{count} URLs...")

            logger.info(f"Crash recovery complete: reset {total_reset} URLs to pending")
        except Exception as e:
            logger.error(f"Failed to recover stuck URLs: {e}")

    def _auto_create_jobs(self) -> bool:
        """Auto-create full scrape jobs for each active country."""
        countries = get_all_active_countries()
        created = False

        for country_code in countries:
            config = get_country_config(country_code)
            regions = config["top_regions"]

            queries = self.search.generate_queries(
                states=regions, country=country_code
            )

            job_id = self.job_manager.create_job(
                job_type="full",
                states=regions,
                total_queries=len(queries),
                country=country_code,
            )
            logger.info(
                f"Auto-created job {job_id} for {config['name']} "
                f"({len(regions)} regions, {len(queries)} queries)"
            )
            created = True

        return created

    async def _execute_job(self, job: dict) -> None:
        """Execute a single scrape job — interleaving discovery and processing."""
        job_id = job["id"]
        job_type = job["job_type"]
        states = job.get("states", TOP_CATTLE_STATES)
        country = job.get("country", "US")

        config = get_country_config(country)
        logger.info(
            f"Executing job {job_id}: type={job_type}, "
            f"country={config['name']}, regions={len(states)}"
        )
        self.job_manager.start_job(job_id)

        try:
            # Phase 1+2 interleaved: discover some URLs, then process some, repeat
            if job_type in ("full", "search"):
                await self._run_search_with_processing(job_id, states, country)

            if self._shutdown:
                return

            # Run associations (with inline email extraction)
            if job_type in ("full", "associations"):
                await self._run_association_discovery(job_id, states, country)

            if self._shutdown:
                return

            # Process any remaining pending URLs
            await self._process_pending_urls(job_id, country)

            if self._shutdown:
                return

            # Phase 3: Directory crawling
            if job_type in ("full", "directories"):
                await self._run_directory_crawlers(job_id, states, country)

            self.job_manager.complete_job(job_id)

        except Exception as e:
            logger.error(f"Job {job_id} failed: {e}", exc_info=True)
            self.job_manager.complete_job(job_id, error=str(e))

    async def _run_search_with_processing(
        self, job_id: int, states: list[str], country: str = "US"
    ) -> None:
        """Interleaved search discovery + URL processing.

        After every DISCOVERY_BATCH_SIZE search queries, pause and process
        a few batches of pending URLs. This ensures emails flow from the
        very beginning rather than waiting for all discovery to finish.
        """
        config = get_country_config(country)
        logger.info(
            f"[Job {job_id}] Interleaved discovery+processing ({config['name']})"
        )

        total_discovered = 0
        total_emails = 0
        queries_since_processing = 0

        async for urls, query, query_idx, total_queries in self.search.discover_urls_db(
            states=states,
            job_id=job_id,
            country=country,
        ):
            added = db.add_urls(
                urls, source="search",
                state=states[0] if len(states) == 1 else "",
                country=country,
            )
            total_discovered += added
            queries_since_processing += 1

            self.job_manager.update_progress(
                job_id,
                query_index=query_idx,
                urls_discovered=total_discovered,
            )

            # Every N queries, pause discovery and process pending URLs
            if queries_since_processing >= DISCOVERY_BATCH_SIZE:
                emails = await self._process_url_batches(
                    job_id, country, max_batches=PROCESS_BATCHES_PER_CYCLE,
                )
                total_emails += emails
                queries_since_processing = 0

            if self._shutdown:
                return

        logger.info(
            f"[Job {job_id}] Search complete: {total_discovered} URLs, "
            f"{total_emails} emails so far ({config['name']})"
        )

    async def _process_url_batches(
        self, job_id: int, country: str, max_batches: int = 3,
    ) -> int:
        """Process a limited number of URL batches. Returns emails saved."""
        total_emails = 0
        sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

        for _ in range(max_batches):
            pending = db.get_pending_urls(limit=WORKER_BATCH_SIZE)
            if not pending:
                break

            async def process_one(url: str) -> int:
                async with sem:
                    return await self.processor.process(url, country=country)

            tasks = [process_one(url) for url in pending]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            batch_emails = 0
            for r in results:
                if isinstance(r, int):
                    batch_emails += r
                elif isinstance(r, Exception):
                    logger.error(f"URL processing error: {r}")

            total_emails += batch_emails

            if batch_emails:
                logger.info(
                    f"[Job {job_id}] Processed {len(pending)} URLs, "
                    f"found {batch_emails} emails"
                )

            if self._shutdown:
                break

        return total_emails

    async def _run_association_discovery(
        self, job_id: int, states: list[str], country: str = "US"
    ) -> None:
        """Discover URLs from association directories (with inline email extraction)."""
        logger.info(f"[Job {job_id}] Association discovery")

        crawler = AssociationCrawler(self.fetcher, country=country)
        urls = await crawler.crawl_all(states)
        added = db.add_urls(urls, source="association", country=country)

        inline_saved = crawler._inline_emails_saved
        logger.info(
            f"[Job {job_id}] Association discovery: {added} new URLs, "
            f"{inline_saved} emails saved inline"
        )

        if inline_saved:
            self.job_manager.update_progress(job_id, emails_found=inline_saved)

    async def _process_pending_urls(
        self, job_id: int, country: str = "US"
    ) -> None:
        """Process ALL pending URLs in the queue."""
        pending_count = db.get_url_count_by_status("pending")
        if not pending_count:
            return

        logger.info(f"[Job {job_id}] Processing {pending_count} pending URLs")

        total_processed = 0
        total_emails = 0
        sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

        while not self._shutdown:
            pending = db.get_pending_urls(limit=WORKER_BATCH_SIZE)
            if not pending:
                break

            async def process_one(url: str) -> int:
                async with sem:
                    return await self.processor.process(url, country=country)

            tasks = [process_one(url) for url in pending]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            batch_emails = 0
            for r in results:
                if isinstance(r, int):
                    batch_emails += r
                elif isinstance(r, Exception):
                    logger.error(f"URL processing error: {r}")

            total_processed += len(pending)
            total_emails += batch_emails

            if job_id:
                self.job_manager.update_progress(
                    job_id,
                    urls_processed=total_processed,
                    emails_found=total_emails,
                )

            logger.info(
                f"[Job {job_id}] Processed {total_processed} URLs, "
                f"{total_emails} emails saved"
            )

        logger.info(
            f"[Job {job_id}] URL processing complete: "
            f"{total_processed} URLs, {total_emails} emails"
        )

    async def _run_directory_crawlers(
        self, job_id: int, states: list[str], country: str = "US"
    ) -> None:
        """Run directory crawlers for direct contact extraction."""
        config = get_country_config(country)
        logger.info(f"[Job {job_id}] Directory crawlers ({config['name']})")

        crawlers = [
            (f"YellowPages-{country}", YellowPagesCrawler(self.fetcher, country=country)),
        ]

        if country == "US":
            crawlers.append(("Yelp", YelpCrawler(self.fetcher)))
            crawlers.append(("Manta", MantaCrawler(self.fetcher)))

        for name, crawler in crawlers:
            if self._shutdown:
                return

            logger.info(f"[Job {job_id}] Running {name} crawler")
            try:
                contacts = await crawler.crawl_all_states(states)
                saved = 0
                for raw in contacts:
                    if not raw.get("email"):
                        continue
                    raw["country"] = country
                    if db.upsert_contact(raw):
                        saved += 1

                logger.info(f"[Job {job_id}] {name}: saved {saved} contacts")
            except Exception as e:
                logger.error(f"[Job {job_id}] {name} crawler failed: {e}")

    def _handle_shutdown(self) -> None:
        """Handle SIGTERM/SIGINT for graceful shutdown."""
        logger.info("Shutdown signal received")
        self._shutdown = True
