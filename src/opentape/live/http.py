"""The only module in opentape that opens a socket.

Every venue source takes a :class:`Fetcher` and does nothing but turn
the JSON it returns into canonical events. That keeps the parsing
logic pure and lets the whole test suite run offline against recorded
payloads, which is why the network lives behind one small interface
instead of being sprinkled through the sources.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from typing import Any, Protocol

from opentape.errors import LiveError

#: Sent on every request so venue operators can identify the traffic.
USER_AGENT = "opentape/live-capture (+https://github.com/andreruizloera/opentape)"

# Polymarket answers 403 to a request with no User-Agent, so the header
# is not decoration: without it the CLOB endpoints are unusable.
DEFAULT_HEADERS = {"User-Agent": USER_AGENT, "Accept": "application/json"}


class Fetcher(Protocol):
    """A JSON GET. Implementations must raise :class:`LiveError` on failure."""

    def __call__(self, url: str, params: dict[str, Any] | None = None) -> Any: ...


def build_url(url: str, params: dict[str, Any] | None) -> str:
    """Append ``params`` to ``url``, dropping keys whose value is None."""
    if not params:
        return url
    query = {k: str(v) for k, v in params.items() if v is not None}
    if not query:
        return url
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}{urllib.parse.urlencode(query)}"


class HttpFetcher:
    """A JSON GET with timeouts, bounded retries, and clean errors.

    Retries cover the failures that are worth retrying: connection
    errors, timeouts, 5xx, and 429. A 4xx other than 429 is reported
    immediately, because a 401, 403, or 404 does not become true on the
    second attempt and retrying only hides the real problem.
    """

    def __init__(
        self,
        *,
        timeout: float = 10.0,
        retries: int = 2,
        backoff: float = 0.5,
        sleep: Callable[[float], None] = time.sleep,
        opener: Callable[[urllib.request.Request, float], Any] | None = None,
    ) -> None:
        self.timeout = timeout
        self.retries = retries
        self.backoff = backoff
        self._sleep = sleep
        self._opener = opener or (lambda req, timeout: urllib.request.urlopen(req, timeout=timeout))

    def __call__(self, url: str, params: dict[str, Any] | None = None) -> Any:
        full = build_url(url, params)
        request = urllib.request.Request(full, headers=dict(DEFAULT_HEADERS))
        last: str = "no attempt was made"
        for attempt in range(self.retries + 1):
            try:
                with self._opener(request, self.timeout) as response:
                    body = response.read()
                break
            except urllib.error.HTTPError as exc:
                detail = _http_detail(exc)
                if exc.code not in (429, 500, 502, 503, 504):
                    raise LiveError(f"GET {full} returned HTTP {exc.code}{detail}") from exc
                last = f"HTTP {exc.code}{detail}"
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                last = f"{type(exc).__name__}: {exc}"
            if attempt == self.retries:
                raise LiveError(
                    f"GET {full} failed after {self.retries + 1} attempts; last error: {last}"
                )
            self._sleep(self.backoff * (2**attempt))
        try:
            return json.loads(body)
        except (ValueError, UnicodeDecodeError) as exc:
            raise LiveError(f"GET {full} did not return JSON: {exc}") from exc


def _http_detail(exc: urllib.error.HTTPError) -> str:
    """A short excerpt of an error body, so a 4xx says why it happened."""
    try:
        body = exc.read().decode("utf-8", "replace").strip()
    except Exception:
        return ""
    if not body:
        return ""
    return f": {body[:200]}"
