"""Entry point: starts the worker + FastAPI dashboard concurrently."""

import asyncio
import logging
import sys
import uvicorn

from app.config import PORT, LOG_LEVEL
from app.worker.orchestrator import Orchestrator

logger = logging.getLogger("cattle_scraper")


def setup_logging() -> None:
    """Configure logging for production."""
    level = getattr(logging, LOG_LEVEL, logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(level)
    root.addHandler(handler)


async def run_dashboard() -> None:
    """Run the FastAPI dashboard server."""
    from app.dashboard.app import app

    config = uvicorn.Config(
        app,
        host="0.0.0.0",
        port=PORT,
        log_level="warning",
        access_log=False,
    )
    server = uvicorn.Server(config)
    await server.serve()


async def run_worker() -> None:
    """Run the background scraping worker."""
    # Wait for dashboard to be fully up before starting worker
    await asyncio.sleep(15)
    try:
        orchestrator = Orchestrator()
        await orchestrator.run_forever()
    except Exception as e:
        logger.error(f"Worker crashed: {e}", exc_info=True)
        # Worker crash should NOT kill the dashboard
        logger.info("Worker stopped — dashboard continues running")


async def main() -> None:
    """Start both the dashboard and the worker concurrently."""
    setup_logging()
    logger.info(f"Starting Cattle Scraper Production on port {PORT}")

    # Run dashboard and worker in parallel
    # Worker is wrapped to not crash the dashboard
    await asyncio.gather(
        run_dashboard(),
        run_worker(),
    )


if __name__ == "__main__":
    asyncio.run(main())
