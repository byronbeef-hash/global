"""Search engine discovery using DuckDuckGo via ddgs — DB-backed version."""

import asyncio
import logging
from typing import AsyncGenerator

from ddgs import DDGS

from app.config import (
    SEARCH_TEMPLATES, SEARCH_TERMS, CATTLE_BREEDS,
    SEARCH_RESULTS_PER_QUERY, TOP_CATTLE_STATES,
    SEARCH_RATE_LIMIT,
)
from app.db import queries as db

logger = logging.getLogger(__name__)


class SearchDiscovery:
    """Discover cattle farm URLs via search engine queries.

    DB-backed: skips already-executed queries, stores results in urls table.
    """

    def __init__(self):
        self.ddgs = DDGS()

    def generate_queries(self, states: list[str] | None = None, max_queries: int | None = None) -> list[str]:
        """Generate search queries from templates x terms x states."""
        target_states = states or TOP_CATTLE_STATES
        queries = []

        for state in target_states:
            for template in SEARCH_TEMPLATES:
                for term in SEARCH_TERMS:
                    query = template.format(term=term, state=state, breed="angus")
                    queries.append(query)

                # Breed-specific queries
                if "{breed}" in template:
                    for breed in CATTLE_BREEDS[:10]:
                        query = template.format(term=term, state=state, breed=breed)
                        if query not in queries:
                            queries.append(query)

        if max_queries:
            queries = queries[:max_queries]

        logger.info(f"Generated {len(queries)} search queries for {len(target_states)} states")
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
    ) -> AsyncGenerator[tuple[list[str], str, int, int], None]:
        """Run queries, skip already-done ones, yield (urls, query, index, total).

        Each batch of results is yielded so the caller can store them.
        """
        queries = self.generate_queries(states, max_queries)
        total = len(queries)

        for i, query in enumerate(queries):
            # Skip if already executed
            if db.is_query_done(query):
                logger.debug(f"[{i+1}/{total}] Skipping already-done query")
                continue

            urls = await self.search(query)

            # Record query as done
            db.mark_query_done(
                query=query,
                results_count=len(urls),
                urls_found=len(urls),
                job_id=job_id,
            )

            if urls:
                logger.info(
                    f"[{i+1}/{total}] Found {len(urls)} URLs for '{query[:50]}...'"
                )
                yield urls, query, i, total

            await asyncio.sleep(SEARCH_RATE_LIMIT)

    async def discover_urls(
        self,
        states: list[str] | None = None,
        max_queries: int | None = None,
        start_index: int = 0,
    ) -> list[str]:
        """Run all queries and collect unique URLs (non-DB fallback)."""
        queries = self.generate_queries(states, max_queries)
        all_urls: set[str] = set()

        for i, query in enumerate(queries[start_index:], start=start_index):
            urls = await self.search(query)
            new_urls = [u for u in urls if u not in all_urls]
            all_urls.update(new_urls)

            if new_urls:
                logger.info(
                    f"[{i+1}/{len(queries)}] Found {len(new_urls)} new URLs "
                    f"(total: {len(all_urls)})"
                )

            await asyncio.sleep(SEARCH_RATE_LIMIT)

        logger.info(f"Discovery complete: {len(all_urls)} unique URLs from {len(queries)} queries")
        return list(all_urls)
