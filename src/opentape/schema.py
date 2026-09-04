"""The canonical OpenTape schema.

A tape is a single Parquet file holding one flat table of events. Every
event type shares the same columns; columns that do not apply to a given
event type are null. This keeps replay ordering trivial (one sort over
one table) while staying fully queryable from Polars, DuckDB, or any
Parquet reader.

See SCHEMA.md in the repository root for the full specification.
"""

from __future__ import annotations

import polars as pl

from opentape.errors import SchemaError

#: Current schema version. Bumped when columns or semantics change.
SCHEMA_VERSION = 1

# Event type tags stored in the ``event_type`` column.
EVENT_MARKET = "market"
EVENT_SNAPSHOT = "book_snapshot"
EVENT_DELTA = "book_delta"
EVENT_TRADE = "trade"
EVENT_STATUS = "market_status"
EVENT_RESOLUTION = "resolution"

EVENT_TYPES: tuple[str, ...] = (
    EVENT_MARKET,
    EVENT_SNAPSHOT,
    EVENT_DELTA,
    EVENT_TRADE,
    EVENT_STATUS,
    EVENT_RESOLUTION,
)

#: One price level of an order book: price as a fraction of 1, size in contracts.
BOOK_LEVEL_DTYPE = pl.Struct({"price": pl.Float64, "size": pl.Float64})
BOOK_DTYPE = pl.List(BOOK_LEVEL_DTYPE)

#: Canonical column set, in canonical order.
TAPE_SCHEMA: dict[str, pl.DataType] = {
    "seq": pl.UInt64,
    "ts": pl.Datetime(time_unit="us", time_zone="UTC"),
    "event_type": pl.Utf8,
    "market_id": pl.Utf8,
    "source": pl.Utf8,
    "outcome": pl.Utf8,
    "side": pl.Utf8,
    "price": pl.Float64,
    "size": pl.Float64,
    "status": pl.Utf8,
    "title": pl.Utf8,
    "outcomes": pl.List(pl.Utf8),
    "bids": BOOK_DTYPE,
    "asks": BOOK_DTYPE,
    "event_id": pl.Utf8,
    "schema_version": pl.UInt16,
}

COLUMNS: tuple[str, ...] = tuple(TAPE_SCHEMA)


def empty_frame() -> pl.DataFrame:
    """Return an empty DataFrame with the canonical schema."""
    return pl.DataFrame(schema=TAPE_SCHEMA)


def validate_frame(df: pl.DataFrame) -> pl.DataFrame:
    """Validate and normalize a frame against the canonical schema.

    Returns a frame with canonical column order and dtypes, sorted by
    ``(ts, seq)``. Raises :class:`SchemaError` on missing columns,
    uncastable dtypes, unknown event types, or a schema version newer
    than this library understands.
    """
    missing = [c for c in COLUMNS if c not in df.columns]
    if missing:
        raise SchemaError(f"tape is missing required columns: {', '.join(missing)}")
    try:
        df = df.select(list(COLUMNS)).cast(TAPE_SCHEMA)  # type: ignore[arg-type]
    except pl.exceptions.PolarsError as exc:
        raise SchemaError(f"tape columns could not be cast to the canonical dtypes: {exc}") from exc

    if df.height > 0:
        bad_types = (
            df.select(pl.col("event_type"))
            .filter(~pl.col("event_type").is_in(EVENT_TYPES))
            .get_column("event_type")
            .unique()
            .to_list()
        )
        if bad_types:
            raise SchemaError(f"unknown event types in tape: {sorted(map(str, bad_types))}")

        max_version = df.get_column("schema_version").max()
        if max_version is not None and int(max_version) > SCHEMA_VERSION:  # type: ignore[arg-type]
            raise SchemaError(
                f"tape declares schema version {max_version}, but this build of opentape "
                f"only understands versions up to {SCHEMA_VERSION}; upgrade opentape"
            )
        if df.get_column("seq").null_count() or df.get_column("ts").null_count():
            raise SchemaError("seq and ts must be non-null for every event")

    return df.sort(["ts", "seq"])
