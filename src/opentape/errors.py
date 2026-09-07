"""Exception types raised by opentape."""

from __future__ import annotations


class OpenTapeError(Exception):
    """Base class for all opentape errors."""


class SchemaError(OpenTapeError):
    """A frame or file does not conform to the canonical tape schema."""


class AdapterError(OpenTapeError):
    """An input file could not be converted into the canonical schema."""


class LiveError(OpenTapeError):
    """A live feed could not be reached, or answered with something unusable."""
