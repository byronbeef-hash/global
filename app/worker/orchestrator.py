"""Main worker orchestrator — runs the 24/7 scraping loop (multi-country)."""

import asyncio
import logging
import signal
import sys

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


class Orchestrator:
    """Main worker that continuously processes scrape jobs.

    Supports multi-country operation. Each job can target a specific country.

    Lifecycle:
        1. Check for queued jobs
        2. If a job exists: run discovery → process URLs → mark complete
        3. If no jobs: auto-create jobs for active countries, then sleep
        4. Handle graceful shutdown on SIGTERM
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
        # Set up graceful shutdown
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._handle_shutdown)

        logger.info("Orchestrator started — multi-country mode")
        logger.info(f"Active countries: {get_all_active_countries()}")

        # Recovery from prior crash/restart (run in background to not block startup)
        asyncio.create_task(self._startup_recovery())

        while not self._shutdown:
            try:
                # Check for queued jobs
                job = self.job_manager.get_next_job()

                if job:
                    await self._execute_job(job)
                else:
                    # Auto-create jobs for each active country
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

    def _auto_create_jobs(self) -> bool:
        """Auto-create full scrape jobs for each active country.

        Returns True if any jobs were created.
        """
        countries = get_all_active_countries()
        created = False

        for country_code in countries:
            config = get_country_config(country_code)
            regions = config["top_regions"]

            # Create a full scrape job for this country
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
        """Execute a single scrape job through all phases."""
        job_id = job["id"]
        job_type = job["job_type"]
        states = job.get("states", TOP_CATTLE_STATES)
        # Extract country from job metadata; default to US
        country = job.get("country", "US")

        config = get_country_config(country)
        logger.info(
            f"Executing job {job_id}: type={job_type}, "
            f"country={config['name']}, regions={len(states)}"
        )
        self.job_manager.start_job(job_id)

        try:
            # Phase 1: Discovery
            if job_type in ("full", "search"):
                await self._run_search_discovery(job_id, states, country)

            if job_type in ("full", "associations"):
                await self._run_association_discovery(job_id, states, country)

            if self._shutdown:
                return

            # Phase 2: Process discovered URLs
            await self._process_pending_urls(job_id, country)

            if self._shutdown:
                return

            # Phase 3: Directory crawling (all countries with YellowPages)
            if job_type in ("full", "directories"):
                await self._run_directory_crawlers(job_id, states, country)

            self.job_manager.complete_job(job_id)

        except Exception as e:
            logger.error(f"Job {job_id} failed: {e}", exc_info=True)
            self.job_manager.complete_job(job_id, error=str(e))

    async def _run_search_discovery(
        self, job_id: int, states: list[str], country: str = "US"
    ) -> None:
        """Phase 1a: Discover URLs via search engine queries."""
        config = get_country_config(country)
        logger.info(f"[Job {job_id}] Phase 1a: Search discovery ({config['name']})")

        total_discovered = 0
        queries_since_processing = 0

        async for urls, query, query_idx, total_queries in self.search.discover_urls_db(
            states=states,
            job_id=job_id,
            country=country,
        ):
            # Add URLs to queue with country tag
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

            # Every 10 queries, process some pending URLs for email extraction
            if queries_since_processing >= 10:
                await self._process_pending_urls(job_id, country)
                queries_since_processing = 0

            if self._shutdown:
                return

        logger.info(
            f"[Job {job_id}] Search discovery complete: "
            f"{total_discovered} URLs ({config['name']})"
        )

    async def _run_association_discovery(
        self, job_id: int, states: list[str], country: str = "US"
    ) -> None:
        """Phase 1b: Discover URLs from association directories.

        Now also extracts emails inline from every page fetched, so
        contacts are saved immediately rather than waiting for Phase 2.
        """
        logger.info(f"[Job {job_id}] Phase 1b: Association discovery")

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
        """Phase 2: Process all pending URLs in the queue."""
        logger.info(f"[Job {job_id}] Phase 2: Processing pending URLs")

        total_processed = 0
        total_emails = 0
        sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

        while not self._shutdown:
            # Get next batch
            pending = db.get_pending_urls(limit=WORKER_BATCH_SIZE)
            if not pending:
                break

            async def process_one(url: str) -> int:
                async with sem:
                    try:
                        result = await self.processor.process(url, country=country)
                        return result
                    except Exception as e:
                        logger.error(f"process_one error for {url}: {e}")
                        try:
                            db.mark_url_done(url, emails_found=0, error=str(e)[:200])
                        except Exception:
                            pass
                        return 0

            # Process batch concurrently
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

            self.job_manager.update_progress(
                job_id,
                urls_processed=total_processed,
                emails_found=total_emails,
            )

            logger.info(
                f"[Job {job_id}] Progress: {total_processed} URLs processed, "
                f"{total_emails} emails saved"
            )

        logger.info(
            f"[Job {job_id}] URL processing complete: "
            f"{total_processed} URLs, {total_emails} emails"
        )

    async def _run_directory_crawlers(
        self, job_id: int, states: list[str], country: str = "US"
    ) -> None:
        """Phase 3: Run directory crawlers for direct extraction.

        YellowPages runs for all countries. Yelp and Manta only run for US.
        """
        config = get_country_config(country)
        logger.info(
            f"[Job {job_id}] Phase 3: Directory crawlers ({config['name']})"
        )

        # YellowPages runs for ALL countries (country-specific URLs)
        crawlers: list[tuple[str, DirectoryCrawler]] = [
            (f"YellowPages-{country}", YellowPagesCrawler(self.fetcher, country=country)),
        ]

        # Yelp and Manta only available for US
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

    async def _startup_recovery(self) -> None:
        """Reset stuck URLs and orphaned jobs from prior crash/restart."""
        try:
            stuck = db.reset_stuck_urls()
            if stuck:
                logger.info(f"Recovery: reset {stuck} stuck 'processing' URLs")
            orphaned = db.reset_orphaned_jobs()
            if orphaned:
                logger.info(f"Recovery: reset {orphaned} orphaned 'running' jobs")
        except Exception as e:
            logger.warning(f"Startup recovery error: {e}")

    def _handle_shutdown(self) -> None:
        """Handle SIGTERM/SIGINT for graceful shutdown."""
        logger.info("Shutdown signal received")
        self._shutdown = True
