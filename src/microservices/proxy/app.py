import logging
import os
import random
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("proxy-service")

PORT = int(os.getenv("PORT", "8000"))
MONOLITH_URL = os.getenv("MONOLITH_URL", "http://localhost:8080").rstrip("/")
MOVIES_SERVICE_URL = os.getenv("MOVIES_SERVICE_URL", "http://localhost:8081").rstrip("/")
EVENTS_SERVICE_URL = os.getenv("EVENTS_SERVICE_URL", "http://localhost:8082").rstrip("/")
GRADUAL_MIGRATION = os.getenv("GRADUAL_MIGRATION", "true").lower() == "true"

try:
    MOVIES_MIGRATION_PERCENT = max(0, min(100, int(os.getenv("MOVIES_MIGRATION_PERCENT", "50"))))
except ValueError:
    MOVIES_MIGRATION_PERCENT = 50

HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
}


def choose_movies_target() -> str:
    if MOVIES_MIGRATION_PERCENT <= 0:
        return MONOLITH_URL
    if MOVIES_MIGRATION_PERCENT >= 100:
        return MOVIES_SERVICE_URL
    if not GRADUAL_MIGRATION:
        return MOVIES_SERVICE_URL
    return MOVIES_SERVICE_URL if random.randrange(100) < MOVIES_MIGRATION_PERCENT else MONOLITH_URL


def choose_target(path: str) -> str:
    if path.startswith("/api/events"):
        return EVENTS_SERVICE_URL
    if path.startswith("/api/movies"):
        return choose_movies_target()
    return MONOLITH_URL


class ProxyHandler(BaseHTTPRequestHandler):
    server_version = "CinemaAbyssProxy/1.0"

    def do_OPTIONS(self):
        self.send_response(204)
        self._send_cors_headers()
        self.end_headers()

    def do_GET(self):
        if self.path.split("?", 1)[0] == "/health":
            body = b"Strangler Fig Proxy is healthy"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self._send_cors_headers()
            self.end_headers()
            self.wfile.write(body)
            return
        self._proxy_request()

    def do_POST(self):
        self._proxy_request()

    def _send_cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type,Authorization")

    def _proxy_request(self):
        target = choose_target(self.path.split("?", 1)[0])
        upstream_url = urljoin(target + "/", self.path.lstrip("/"))
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length) if length > 0 else None

        headers = {
            key: value
            for key, value in self.headers.items()
            if key.lower() not in HOP_BY_HOP_HEADERS
        }
        request = Request(upstream_url, data=body, headers=headers, method=self.command)
        log.info("%s %s -> %s", self.command, self.path, upstream_url)

        try:
            with urlopen(request, timeout=15) as response:
                response_body = response.read()
                self.send_response(response.status)
                for key, value in response.headers.items():
                    if key.lower() not in HOP_BY_HOP_HEADERS:
                        self.send_header(key, value)
                self.send_header("Content-Length", str(len(response_body)))
                self._send_cors_headers()
                self.end_headers()
                self.wfile.write(response_body)
        except HTTPError as exc:
            response_body = exc.read()
            self.send_response(exc.code)
            for key, value in exc.headers.items():
                if key.lower() not in HOP_BY_HOP_HEADERS:
                    self.send_header(key, value)
            self.send_header("Content-Length", str(len(response_body)))
            self._send_cors_headers()
            self.end_headers()
            self.wfile.write(response_body)
        except URLError as exc:
            log.warning("upstream unavailable: %s", exc)
            body = b'{"error":"upstream unavailable"}'
            self.send_response(502)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self._send_cors_headers()
            self.end_headers()
            self.wfile.write(body)

    def log_message(self, fmt, *args):
        log.info("%s - %s", self.address_string(), fmt % args)


if __name__ == "__main__":
    log.info(
        "proxy-service listening on :%s, movies migration=%s%%, gradual=%s",
        PORT,
        MOVIES_MIGRATION_PERCENT,
        GRADUAL_MIGRATION,
    )
    ThreadingHTTPServer(("0.0.0.0", PORT), ProxyHandler).serve_forever()
