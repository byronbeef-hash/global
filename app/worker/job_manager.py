"""Job creation and lifecycle management (multi-country aware)."""

import logging

from app.db import queries as db
from app.config import TOP_CATTLE_STATES, US_STATES, get_country_config

logger = logging.getLogger(__name__)


class JobManager:
    """Creates and manages scrape jobs."""

    def create_job(
        self,
        job_type: str = "full",
        states: list[str] | None = None,
        total_queries: int = 0,
        country: str = "US",
    ) -> int:
        """Create a new scrape job and return its ID."""
        target_states = states or TOP_CATTLE_STATES
        job_id = db.create_job(
            job_type=job_type,
            states=target_states,
            total_queries=total_queries,
            country=country,
        )
        config = get_country_config(country)
        logger.info(
            f"Created job {job_id}: type={job_type}, country={config['name']}, "
            f"regions={len(target_states)}, queries={total_queries}"
        )
        return job_id

    def start_job(self, job_id: int) -> None:
        """Mark a job as running."""
        db.start_job(job_id)
        logger.info(f"Job {job_id} started")

    def update_progress(
        self,
        job_id: int,
        query_index: int | None = None,
        urls_discovered: int | None = None,
        urls_processed: int | None = None,
        emails_found: int | None = None,
    ) -> None:
        """Update job progress counters."""
        db.update_job_progress(
            job_id,
            query_index=query_index,
            urls_discovered=urls_discovered,
            urls_processed=urls_processed,
            emails_found=emails_found,
        )

    def complete_job(self, job_id: int, error: str = "") -> None:
        """Mark a job as completed or failed."""
        db.complete_job(job_id, error=error)
        status = "failed" if error else "completed"
        logger.info(f"Job {job_id} {status}" + (f": {error}" if error else ""))

    def get_next_job(self) -> dict | None:
        """Get the next queued job to process.

        Decodes country from the job_type field (e.g. 'full:NZ' -> country='NZ').
        """
        job = db.get_next_queued_job()
        if job:
            self._decode_country(job)
        return job

    def get_all_queued_jobs(self) -> list[dict]:
        """Get all queued jobs at once for concurrent execution."""
        jobs = db.get_all_queued_jobs()
        for job in jobs:
            self._decode_country(job)
        return jobs

    @staticmethod
    def _decode_country(job: dict) -> None:
        """Decode country from job_type field (e.g. 'full:NZ' -> country='NZ')."""
        jt = job.get("job_type", "full")
        if ":" in jt:
            parts = jt.split(":", 1)
            job["job_type"] = parts[0]
            job["country"] = parts[1]
        else:
            job["country"] = "US"

    def get_active_jobs(self) -> list[dict]:
        """Get all running/queued jobs."""
        return db.get_active_jobs()

    def get_all_jobs(self, limit: int = 50) -> list[dict]:
        """Get all jobs."""
        return db.get_all_jobs(limit)
