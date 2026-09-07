"""Adapters that convert exchange-shaped data into the canonical schema.

Adapter contract: each adapter module exposes

    convert(path: str | Path) -> Tape

taking a local file and returning a validated, sequenced Tape.
Adapters never perform network I/O. Reading a venue directly is a
different interface, :class:`opentape.live.LiveSource`, because a
converter and a poller have different shapes: one is handed all the
data at once, the other has to ask repeatedly and work out what
changed.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from opentape.adapters import generic, kalshi_style, polymarket_style
from opentape.errors import AdapterError
from opentape.tape import Tape

ADAPTERS: dict[str, Callable[[str | Path], Tape]] = {
    "generic": generic.convert,
    "kalshi-style": kalshi_style.convert,
    "polymarket-style": polymarket_style.convert,
}


def convert_file(path: str | Path, fmt: str) -> Tape:
    """Convert ``path`` using the adapter registered under ``fmt``."""
    try:
        adapter = ADAPTERS[fmt]
    except KeyError:
        known = ", ".join(sorted(ADAPTERS))
        raise AdapterError(f"unknown format {fmt!r}; known formats: {known}") from None
    path = Path(path)
    if not path.exists():
        raise AdapterError(f"no such file: {path}")
    return adapter(path)
