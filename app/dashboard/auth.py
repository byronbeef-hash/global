"""Simple API key authentication for the dashboard."""

import logging
from fastapi import Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware

from app.config import DASHBOARD_API_KEY

logger = logging.getLogger(__name__)

# Paths that don't require auth
PUBLIC_PATHS = {"/health", "/favicon.ico"}


class APIKeyMiddleware(BaseHTTPMiddleware):
    """Check for API key in header or query parameter."""

    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        # Public endpoints
        if path in PUBLIC_PATHS:
            return await call_next(request)

        # Check API key
        api_key = (
            request.headers.get("X-API-Key")
            or request.query_params.get("key")
            or request.cookies.get("api_key")
        )

        if api_key != DASHBOARD_API_KEY:
            # If it's a page request, show a login form
            if "text/html" in request.headers.get("accept", ""):
                from fastapi.responses import HTMLResponse
                return HTMLResponse(
                    content=LOGIN_HTML,
                    status_code=401,
                )
            raise HTTPException(status_code=401, detail="Invalid or missing API key")

        return await call_next(request)


LOGIN_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Cattle Scraper — Login</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, sans-serif; background: #1a1a2e; color: #eee;
               display: flex; justify-content: center; align-items: center; min-height: 100vh; }
        .card { background: #16213e; padding: 2rem; border-radius: 12px; width: 360px;
                box-shadow: 0 8px 32px rgba(0,0,0,.3); }
        h1 { font-size: 1.4rem; margin-bottom: 1.5rem; text-align: center; }
        input { width: 100%; padding: .8rem; border: 1px solid #333; border-radius: 8px;
                background: #0f3460; color: #eee; font-size: 1rem; margin-bottom: 1rem; }
        button { width: 100%; padding: .8rem; border: none; border-radius: 8px;
                 background: #e94560; color: white; font-size: 1rem; cursor: pointer; }
        button:hover { background: #c73a52; }
    </style>
</head>
<body>
    <div class="card">
        <h1>Cattle Scraper Dashboard</h1>
        <form id="login">
            <input type="password" id="key" placeholder="API Key" autofocus>
            <button type="submit">Login</button>
        </form>
    </div>
    <script>
        document.getElementById('login').onsubmit = (e) => {
            e.preventDefault();
            const key = document.getElementById('key').value;
            document.cookie = `api_key=${key}; path=/; max-age=86400`;
            window.location.reload();
        };
    </script>
</body>
</html>
"""
