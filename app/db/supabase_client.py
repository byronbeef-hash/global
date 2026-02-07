"""Supabase client initialization and connection management."""

import logging
from supabase import create_client, Client

from app.config import SUPABASE_URL, SUPABASE_KEY

logger = logging.getLogger(__name__)

_client: Client | None = None


def get_client() -> Client:
    """Get or create the Supabase client singleton."""
    global _client
    if _client is None:
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise RuntimeError(
                "SUPABASE_URL and SUPABASE_KEY must be set. "
                "Copy .env.example to .env and fill in your credentials."
            )
        _client = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info("Supabase client initialized")
    return _client
