"""Search engine discovery using DuckDuckGo via ddgs — DB-backed, multi-country version."""

import asyncio
import logging
from typing import AsyncGenerator

from ddgs import DDGS

from app.config import (
    SEARCH_TEMPLATES, SEARCH_TERMS, CATTLE_BREEDS,
    SEARCH_RESULTS_PER_QUERY, TOP_CATTLE_STATES,
    SEARCH_RATE_LIMIT, COUNTRY_CONFIG, get_country_config,
    QUERY_RERUN_DAYS,
)
from app.db import queries as db

logger = logging.getLogger(__name__)


class SearchDiscovery:
    """Discover cattle farm URLs via search engine queries.

    DB-backed: skips already-executed queries, stores results in urls table.
    Supports multi-country operation via country_code parameter.
    """

    def __init__(self):
        self.ddgs = DDGS()

    def generate_queries(
        self,
        states: list[str] | None = None,
        max_queries: int | None = None,
        country: str = "US",
    ) -> list[str]:
        """Generate search queries from templates x terms x regions.

        Args:
            states: Override regions. If None, uses top_regions for the country.
            max_queries: Limit total queries.
            country: Country code (US, NZ, UK, CA, AU).
        """
        config = get_country_config(country)
        target_regions = states or config["top_regions"]
        templates = config["search_templates"]
        terms = config["search_terms"]
        breeds = config["breeds"]

        queries = []

        for region in target_regions:
            for template in templates:
                for term in terms:
                    query = template.format(
                        term=term, region=region, breed="angus",
                        # Legacy compat: {state} maps to {region}
                        state=region,
                    )
                    queries.append(query)

                # Breed-specific queries
                if "{breed}" in template:
                    for breed in breeds[:10]:
                        query = template.format(
                            term=term, region=region, breed=breed,
                            state=region,
                        )
                        if query not in queries:
                            queries.append(query)

        if max_queries:
            queries = queries[:max_queries]

        logger.info(
            f"Generated {len(queries)} search queries for {len(target_regions)} "
            f"regions in {config['name']} ({country})"
        )
        return queries

    async def search(self, query: str, max_results: int = SEARCH_RESULTS_PER_QUERY) -> list[str]:
        """Run a single search query and return result URLs."""
        try:
            loop = asyncio.get_event_loop()
            results = await loop.run_in_executor(
                None,
                lambda: list(self.ddgs.text(query, max_results=max_results)),
            )
            urls = [r["href"] for r in results if "href" in r]
            logger.debug(f"Query '{query[:60]}' -> {len(urls)} URLs")
            return urls
        except Exception as e:
            logger.warning(f"Search failed for '{query[:60]}': {e}")
            return []

    async def discover_urls_db(
        self,
        states: list[str] | None = None,
        max_queries: int | None = None,
        job_id: int | None = None,
        country: str = "US",
    ) -> AsyncGenerator[tuple[list[str], str, int, int], None]:
        """Run queries, skip recently-done ones, yield (urls, query, index, total).

        Each batch of results is yielded so the caller can store them.
        Queries older than QUERY_RERUN_DAYS are re-executed for fresh results.

        Uses bulk get_recent_queries() to check all queries in ONE DB call
        instead of per-query is_query_done() which made 29K+ HTTP round-trips
        and starved the dashboard event loop.
        """
        queries = self.generate_queries(states, max_queries, country=country)
        total = len(queries)
        skipped = 0

        # Bulk-fetch all recently-done queries in ONE call (replaces 29K+ individual checks)
        recent_queries = await asyncio.to_thread(
            db.get_recent_queries, QUERY_RERUN_DAYS
        )
        logger.info(
            f"[{country}] {len(recent_queries)} queries done recently, "
            f"{total} total to check"
        )

        for i, query in enumerate(queries):
            # In-memory set lookup — instant, no DB call
            if query in recent_queries:
                skipped += 1
                # Yield to event loop every 500 skips so dashboard stays responsive
                if skipped % 500 == 0:
                    await asyncio.sleep(0)
                continue

            urls = await self.search(query)

            # Record query as done (updates executed_at on re-runs)
            db.mark_query_done(
                query=query,
                results_count=len(urls),
                urls_found=len(urls),
                job_id=job_id,
            )

            # Add to in-memory set so we don't re-check next iteration
            recent_queries.add(query)

            if urls:
                logger.info(
                    f"[{i+1}/{total}] [{country}] Found {len(urls)} URLs "
                    f"for '{query[:50]}...'"
                )
                yield urls, query, i, total

            await asyncio.sleep(SEARCH_RATE_LIMIT)

        if skipped:
            logger.info(
                f"[{country}] Skipped {skipped}/{total} recently-done queries "
                f"(re-run after {QUERY_RERUN_DAYS}d)"
            )

    async def discover_urls(
        self,
        states: list[str] | None = None,
        max_queries: int | None = None,
        start_index: int = 0,
        country: str = "US",
    ) -> list[str]:
        """Run all queries and collect unique URLs (non-DB fallback)."""
        queries = self.generate_queries(states, max_queries, country=country)
        all_urls: set[str] = set()

        for i, query in enumerate(queries[start_index:], start=start_index):
            urls = await self.search(query)
            new_urls = [u for u in urls if u not in all_urls]
            all_urls.update(new_urls)

            if new_urls:
                logger.info(
                    f"[{i+1}/{len(queries)}] [{country}] Found {len(new_urls)} new URLs "
                    f"(total: {len(all_urls)})"
                )

            await asyncio.sleep(SEARCH_RATE_LIMIT)

        logger.info(
            f"[{country}] Discovery complete: {len(all_urls)} unique URLs "
            f"from {len(queries)} queries"
        )
        return list(all_urls)
