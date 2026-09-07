"""Tests for the HTTP transport.

No socket is opened: the opener is injected, so these exercise the
retry policy and the error messages rather than the network.
"""

from __future__ import annotations

import urllib.error
from typing import Any

import pytest

from opentape.errors import LiveError
from opentape.live.http import DEFAULT_HEADERS, HttpFetcher, build_url


class _Response:
    def __init__(self, body: bytes) -> None:
        self._body = body

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *exc: object) -> None:
        return None


def _http_error(code: int, body: bytes = b"") -> urllib.error.HTTPError:
    import io

    return urllib.error.HTTPError("https://x", code, "boom", {}, io.BytesIO(body))  # type: ignore[arg-type]


def _fetcher(outcomes: list[Any], **kwargs: Any) -> tuple[HttpFetcher, list[float]]:
    """Build a fetcher whose opener replays ``outcomes`` in order."""
    slept: list[float] = []
    calls = iter(outcomes)

    def opener(request: Any, timeout: float) -> Any:
        result = next(calls)
        if isinstance(result, Exception):
            raise result
        return result

    kwargs.setdefault("backoff", 0.01)
    return HttpFetcher(opener=opener, sleep=slept.append, **kwargs), slept


def test_build_url_appends_params_and_drops_none() -> None:
    assert build_url("https://x/y", None) == "https://x/y"
    assert build_url("https://x/y", {"a": 1, "b": None}) == "https://x/y?a=1"
    assert build_url("https://x/y?z=1", {"a": 2}) == "https://x/y?z=1&a=2"


def test_successful_fetch_parses_json() -> None:
    fetch, _ = _fetcher([_Response(b'{"ok": true}')])
    assert fetch("https://x") == {"ok": True}


def test_a_user_agent_is_always_sent() -> None:
    # Polymarket answers 403 without one, so this is load bearing.
    seen: list[dict[str, str]] = []

    def opener(request: Any, timeout: float) -> Any:
        seen.append(dict(request.headers))
        return _Response(b"[]")

    HttpFetcher(opener=opener)("https://x")
    assert any("opentape" in v for v in seen[0].values())
    assert "User-Agent" in DEFAULT_HEADERS


def test_a_server_error_is_retried_and_can_succeed() -> None:
    fetch, slept = _fetcher([_http_error(503), _Response(b"[1]")])
    assert fetch("https://x") == [1]
    assert len(slept) == 1


def test_rate_limiting_is_retried() -> None:
    fetch, slept = _fetcher([_http_error(429), _Response(b"[]")])
    assert fetch("https://x") == []
    assert len(slept) == 1


def test_a_client_error_is_not_retried_and_names_the_status() -> None:
    fetch, slept = _fetcher([_http_error(404, b'{"error": "market not found"}')])
    with pytest.raises(LiveError) as exc:
        fetch("https://x/markets/nope")
    assert "HTTP 404" in str(exc.value)
    assert "market not found" in str(exc.value)
    # Retrying a 404 only hides the real problem.
    assert slept == []


def test_retries_are_bounded_and_the_last_error_is_reported() -> None:
    fetch, slept = _fetcher([_http_error(500)] * 3, retries=2)
    with pytest.raises(LiveError) as exc:
        fetch("https://x")
    assert "after 3 attempts" in str(exc.value)
    assert "HTTP 500" in str(exc.value)
    assert len(slept) == 2


def test_a_connection_error_is_retried_then_reported() -> None:
    fetch, _ = _fetcher([urllib.error.URLError("no route"), TimeoutError("slow")], retries=1)
    with pytest.raises(LiveError) as exc:
        fetch("https://x")
    assert "TimeoutError" in str(exc.value)


def test_a_non_json_body_is_a_clean_error() -> None:
    fetch, _ = _fetcher([_Response(b"<html>maintenance</html>")])
    with pytest.raises(LiveError) as exc:
        fetch("https://x")
    assert "did not return JSON" in str(exc.value)


def test_backoff_grows_between_attempts() -> None:
    fetch, slept = _fetcher([_http_error(500)] * 4, retries=3, backoff=1.0)
    with pytest.raises(LiveError):
        fetch("https://x")
    assert slept == [1.0, 2.0, 4.0]
