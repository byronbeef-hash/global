"""Simple email/password authentication for the dashboard.

Uses a pure ASGI middleware instead of Starlette's BaseHTTPMiddleware to avoid
the internal thread-hop and MemoryObjectStream overhead that causes /health
to time out when the async event loop is under heavy worker load.

The /health endpoint is short-circuited at the ASGI level before any
framework processing, guaranteeing sub-millisecond response times.
"""

import hashlib
import json
import logging
import secrets
from http.cookies import SimpleCookie
from urllib.parse import parse_qs

from app.config import DASHBOARD_API_KEY

logger = logging.getLogger(__name__)

# Paths that don't require auth — handled as fast ASGI short-circuits
PUBLIC_PATHS = {"/health", "/favicon.ico"}

# Pre-built /health response body (avoids any work at request time)
_HEALTH_BODY = b'{"status":"ok"}'

# ── Credentials ──────────────────────────────────────────────────────
# Hardcoded credentials (hashed password for basic security)
_AUTH_EMAIL = "byronbeef@protonmail.com"
_AUTH_PASSWORD_HASH = hashlib.sha256(b"stake@123").hexdigest()

# Active sessions: token -> True
_sessions: dict[str, bool] = {}


def _check_credentials(email: str, password: str) -> bool:
    """Verify email/password."""
    pw_hash = hashlib.sha256(password.encode()).hexdigest()
    return email.strip().lower() == _AUTH_EMAIL and pw_hash == _AUTH_PASSWORD_HASH


def _check_session(raw_cookie: str) -> bool:
    """Check if request has a valid session cookie."""
    if not raw_cookie:
        return False
    cookie = SimpleCookie()
    cookie.load(raw_cookie)
    if "session" in cookie:
        token = cookie["session"].value
        return token in _sessions
    return False


def _check_api_key(headers: dict, query_string: str) -> bool:
    """Legacy API key check for programmatic access (curl, etc.)."""
    params = parse_qs(query_string)
    key_from_header = headers.get(b"x-api-key", b"").decode()
    key_from_query = params.get("key", [None])[0]

    raw_cookie = headers.get(b"cookie", b"").decode()
    key_from_cookie = ""
    if raw_cookie:
        cookie = SimpleCookie()
        cookie.load(raw_cookie)
        if "api_key" in cookie:
            key_from_cookie = cookie["api_key"].value

    api_key = key_from_header or key_from_query or key_from_cookie
    return api_key == DASHBOARD_API_KEY


class APIKeyMiddleware:
    """Pure ASGI middleware for authentication.

    /health is answered directly at the ASGI layer — no framework overhead,
    no thread hops, guaranteed instant response even under heavy load.

    Supports both session-based login (email/password) and legacy API key.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        method = scope.get("method", "GET")

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

        # ── Login POST handler ────────────────────────────────────────
        if path == "/login" and method == "POST":
            body = b""
            while True:
                message = await receive()
                body += message.get("body", b"")
                if not message.get("more_body", False):
                    break

            # Parse form data
            form_data = parse_qs(body.decode())
            email = form_data.get("email", [""])[0]
            password = form_data.get("password", [""])[0]

            if _check_credentials(email, password):
                token = secrets.token_hex(32)
                _sessions[token] = True
                cookie_val = (
                    f"session={token}; Path=/; Max-Age={86400 * 30}; "
                    f"HttpOnly; SameSite=Lax"
                )
                # Also set api_key cookie for export downloads
                api_cookie = (
                    f"api_key={DASHBOARD_API_KEY}; Path=/; Max-Age={86400 * 30}; "
                    f"HttpOnly; SameSite=Lax"
                )
                # Redirect to dashboard
                resp_body = b""
                await send({
                    "type": "http.response.start",
                    "status": 302,
                    "headers": [
                        [b"location", b"/"],
                        [b"set-cookie", cookie_val.encode()],
                        [b"set-cookie", api_cookie.encode()],
                        [b"content-length", b"0"],
                    ],
                })
                await send({"type": "http.response.body", "body": resp_body})
            else:
                body = LOGIN_HTML.replace("{{error}}", "Invalid email or password").encode()
                await send({
                    "type": "http.response.start",
                    "status": 401,
                    "headers": [
                        [b"content-type", b"text/html; charset=utf-8"],
                        [b"content-length", str(len(body)).encode()],
                    ],
                })
                await send({"type": "http.response.body", "body": body})
            return

        # ── Login page GET ────────────────────────────────────────────
        if path == "/login" and method == "GET":
            body = LOGIN_HTML.replace("{{error}}", "").encode()
            await send({
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    [b"content-type", b"text/html; charset=utf-8"],
                    [b"content-length", str(len(body)).encode()],
                ],
            })
            await send({"type": "http.response.body", "body": body})
            return

        # ── Auth check ────────────────────────────────────────────────
        headers = dict(scope.get("headers", []))
        query_string = scope.get("query_string", b"").decode()
        raw_cookie = headers.get(b"cookie", b"").decode()

        # Check session cookie first, then fall back to API key
        authenticated = _check_session(raw_cookie) or _check_api_key(headers, query_string)

        if not authenticated:
            accept = headers.get(b"accept", b"").decode()
            if "text/html" in accept:
                # Redirect to login page
                await send({
                    "type": "http.response.start",
                    "status": 302,
                    "headers": [
                        [b"location", b"/login"],
                        [b"content-length", b"0"],
                    ],
                })
                await send({"type": "http.response.body", "body": b""})
            else:
                body = json.dumps({"detail": "Authentication required"}).encode()
                await send({
                    "type": "http.response.start",
                    "status": 401,
                    "headers": [
                        [b"content-type", b"application/json"],
                        [b"content-length", str(len(body)).encode()],
                    ],
                })
                await send({"type": "http.response.body", "body": body})
            return

        # ── Authenticated: pass through ───────────────────────────────
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
        input::placeholder { color: #888; }
        button { width: 100%; padding: .8rem; border: none; border-radius: 8px;
                 background: #e94560; color: white; font-size: 1rem; cursor: pointer; }
        button:hover { background: #c73a52; }
        .error { color: #e94560; font-size: .85rem; text-align: center; margin-bottom: 1rem; }
    </style>
</head>
<body>
    <div class="card">
        <h1>Cattle Scraper Dashboard</h1>
        <div class="error">{{error}}</div>
        <form method="POST" action="/login">
            <input type="email" name="email" placeholder="Email" autofocus required>
            <input type="password" name="password" placeholder="Password" required>
            <button type="submit">Login</button>
        </form>
    </div>
</body>
</html>
"""
