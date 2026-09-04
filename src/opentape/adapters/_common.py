"""Shared helpers for adapters."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from opentape.errors import AdapterError
from opentape.events import Event


def load_json(path: Path) -> Any:
    """Load a JSON document with a clean error on failure."""
    try:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except OSError as exc:
        raise AdapterError(f"could not read {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise AdapterError(f"{path} is not valid JSON: {exc}") from exc


def parse_ts(value: Any, *, context: str) -> datetime:
    """Parse a timestamp from adapter input.

    Accepted forms:
    - ISO 8601 string (a trailing "Z" is understood as UTC)
    - int or float epoch seconds (< 10^12)
    - int epoch milliseconds (10^12 to < 10^15)
    - int epoch microseconds (>= 10^15)
    Naive ISO strings are rejected: source data must be explicit about
    its timezone for a tape to be trustworthy.
    """
    if isinstance(value, str):
        try:
            ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise AdapterError(f"{context}: bad ISO 8601 timestamp {value!r}") from exc
        if ts.tzinfo is None:
            raise AdapterError(
                f"{context}: timestamp {value!r} has no timezone; use an offset or Z suffix"
            )
        return ts.astimezone(UTC)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise AdapterError(f"{context}: bad timestamp {value!r}")
    v = float(value)
    if v >= 1e15:  # epoch microseconds
        return datetime.fromtimestamp(v / 1e6, tz=UTC)
    if v >= 1e12:  # epoch milliseconds
        return datetime.fromtimestamp(v / 1e3, tz=UTC)
    return datetime.fromtimestamp(v, tz=UTC)


def parse_fraction(value: Any, *, context: str) -> float:
    """Parse a price given as a decimal fraction of 1 (string or number)."""
    try:
        price = float(value)
    except (TypeError, ValueError) as exc:
        raise AdapterError(f"{context}: bad price {value!r}") from exc
    if not 0.0 <= price <= 1.0:
        raise AdapterError(f"{context}: price {price} is outside [0, 1]")
    return price


def parse_size(value: Any, *, context: str) -> float:
    try:
        size = float(value)
    except (TypeError, ValueError) as exc:
        raise AdapterError(f"{context}: bad size {value!r}") from exc
    if size < 0:
        raise AdapterError(f"{context}: size {size} is negative")
    return size


def sequence_events(events: list[Event]) -> list[Event]:
    """Sort converted events by timestamp and assign a dense global seq.

    The sort is stable, so events that share a timestamp keep the order
    they appeared in the source file.
    """
    ordered = sorted(events, key=lambda e: e.ts)
    return [replace(e, seq=i) for i, e in enumerate(ordered)]
