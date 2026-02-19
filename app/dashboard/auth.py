"""Simple API key authentication for the dashboard.

Uses a pure ASGI middleware instead of Starlette's BaseHTTPMiddleware to avoid
the internal thread-hop and MemoryObjectStream overhead that causes /health
to time out when the async event loop is under heavy worker load.

The /health endpoint is short-circuited at the ASGI level before any
framework processing, guaranteeing sub-millisecond response times.
"""

import json
import logging
from http.cookies import SimpleCookie
from urllib.parse import parse_qs

from app.config import DASHBOARD_API_KEY

logger = logging.getLogger(__name__)

# Paths that don't require auth — handled as fast ASGI short-circuits
PUBLIC_PATHS = {"/health", "/favicon.ico"}

# Pre-built /health response body (avoids any work at request time)
_HEALTH_BODY = b'{"status":"ok"}'


class APIKeyMiddleware:
    """Pure ASGI middleware for API key authentication.

    /health is answered directly at the ASGI layer — no framework overhead,
    no thread hops, guaranteed instant response even under heavy load.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")

        # ── Fast path: /health short-circuit ──────────────────────────
        if path == "/health":
            await send({
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    [b"content-type", b"application/json"],
                    [b"content-length", str(len(_HEALTH_BODY)).encode()],
                ],
            })
            await send({
                "type": "http.response.body",
                "body": _HEALTH_BODY,
            })
            return

        # ── Other public paths: pass through without auth ─────────────
        if path in PUBLIC_PATHS:
            await self.app(scope, receive, send)
            return

        # ── Auth check ────────────────────────────────────────────────
        headers = dict(scope.get("headers", []))
        query_string = scope.get("query_string", b"").decode()
        params = parse_qs(query_string)

        # Check API key from multiple sources
        key_from_header = headers.get(b"x-api-key", b"").decode()
        key_from_query = params.get("key", [None])[0]

        # Parse cookies
        key_from_cookie = ""
        raw_cookie = headers.get(b"cookie", b"").decode()
        if raw_cookie:
            cookie = SimpleCookie()
            cookie.load(raw_cookie)
            if "api_key" in cookie:
                key_from_cookie = cookie["api_key"].value

        api_key = key_from_header or key_from_query or key_from_cookie

        if api_key != DASHBOARD_API_KEY:
            # Check Accept header for HTML
            accept = headers.get(b"accept", b"").decode()
            if "text/html" in accept:
                body = LOGIN_HTML.encode("utf-8")
                await send({
                    "type": "http.response.start",
                    "status": 401,
                    "headers": [
                        [b"content-type", b"text/html; charset=utf-8"],
                        [b"content-length", str(len(body)).encode()],
                    ],
                })
                await send({
                    "type": "http.response.body",
                    "body": body,
                })
            else:
                body = json.dumps({"detail": "Invalid or missing API key"}).encode()
                await send({
                    "type": "http.response.start",
                    "status": 401,
                    "headers": [
                        [b"content-type", b"application/json"],
                        [b"content-length", str(len(body)).encode()],
                    ],
                })
                await send({
                    "type": "http.response.body",
                    "body": body,
                })
            return

        # ── Authenticated: pass through ───────────────────────────────
        # If key came from query param, inject a Set-Cookie header
        if key_from_query == DASHBOARD_API_KEY:
            original_send = send

            async def send_with_cookie(message):
                if message["type"] == "http.response.start":
                    headers_list = list(message.get("headers", []))
                    cookie_val = (
                        f"api_key={api_key}; Path=/; Max-Age={86400 * 30}; "
                        f"HttpOnly; SameSite=Lax"
                    )
                    headers_list.append([b"set-cookie", cookie_val.encode()])
                    message = {**message, "headers": headers_list}
                await original_send(message)

            await self.app(scope, receive, send_with_cookie)
        else:
            await self.app(scope, receive, send)


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
