"""The Tape: read, write, replay, and query canonical event data."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import duckdb
import polars as pl

from opentape import schema
from opentape.book import OrderBook, reconstruct
from opentape.errors import OpenTapeError, SchemaError
from opentape.events import Event, from_row, to_row

# Views exposed to sql(), defined over the flat "events" table.
_VIEWS: tuple[tuple[str, str], ...] = (
    (
        "markets",
        "CREATE VIEW markets AS SELECT seq, ts, market_id, source, title, outcomes "
        f"FROM events WHERE event_type = '{schema.EVENT_MARKET}'",
    ),
    (
        "snapshots",
        "CREATE VIEW snapshots AS SELECT seq, ts, market_id, source, outcome, bids, asks "
        f"FROM events WHERE event_type = '{schema.EVENT_SNAPSHOT}'",
    ),
    (
        "deltas",
        "CREATE VIEW deltas AS SELECT seq, ts, market_id, source, outcome, side, price, size "
        f"FROM events WHERE event_type = '{schema.EVENT_DELTA}'",
    ),
    (
        "trades",
        "CREATE VIEW trades AS SELECT seq, ts, market_id, source, outcome, side, price, size, "
        f"event_id AS trade_id FROM events WHERE event_type = '{schema.EVENT_TRADE}'",
    ),
    (
        "status",
        "CREATE VIEW status AS SELECT seq, ts, market_id, source, status "
        f"FROM events WHERE event_type = '{schema.EVENT_STATUS}'",
    ),
    (
        "resolutions",
        "CREATE VIEW resolutions AS SELECT seq, ts, market_id, source, outcome, "
        f"price AS settlement FROM events WHERE event_type = '{schema.EVENT_RESOLUTION}'",
    ),
)


@dataclass(frozen=True, slots=True)
class MarketPriceStats:
    """Trade price statistics for one market."""

    market_id: str
    trades: int
    min_price: float
    mean_price: float
    max_price: float
    last_price: float
    vwap: float


@dataclass(frozen=True, slots=True)
class TapeSummary:
    """What `opentape inspect` reports."""

    schema_version: int
    n_events: int
    markets: tuple[str, ...]
    sources: tuple[str, ...]
    event_counts: dict[str, int]
    start: datetime | None
    end: datetime | None
    price_stats: tuple[MarketPriceStats, ...]


class Tape:
    """An ordered sequence of canonical prediction-market events.

    Construct with :meth:`read` (from Parquet) or :meth:`from_events`
    (from typed event objects). The underlying frame is validated,
    cast to canonical dtypes, and sorted by ``(ts, seq)`` on construction.
    """

    def __init__(self, frame: pl.DataFrame) -> None:
        self._df = schema.validate_frame(frame)
        self._con: duckdb.DuckDBPyConnection | None = None

    # -- construction ------------------------------------------------------

    @classmethod
    def read(cls, path: str | Path) -> Tape:
        """Read a tape from a Parquet file."""
        path = Path(path)
        if not path.exists():
            raise OpenTapeError(f"no such file: {path}")
        try:
            df = pl.read_parquet(path)
        except Exception as exc:
            raise SchemaError(f"could not read {path} as Parquet: {exc}") from exc
        return cls(df)

    @classmethod
    def from_events(cls, events: Iterable[Event]) -> Tape:
        """Build a tape from typed event objects."""
        rows = [to_row(e) for e in events]
        if not rows:
            return cls(schema.empty_frame())
        df = pl.from_dicts(rows, schema=schema.TAPE_SCHEMA)  # type: ignore[arg-type]
        return cls(df)

    # -- persistence -------------------------------------------------------

    def write(self, path: str | Path) -> None:
        """Write the tape to a Parquet file."""
        self._df.write_parquet(path)

    # -- basic accessors ---------------------------------------------------

    @property
    def frame(self) -> pl.DataFrame:
        """The underlying Polars frame (canonical order and dtypes)."""
        return self._df

    def __len__(self) -> int:
        return self._df.height

    @property
    def schema_version(self) -> int:
        if self._df.height == 0:
            return schema.SCHEMA_VERSION
        return int(self._df.get_column("schema_version").max())  # type: ignore[arg-type]

    def market_ids(self) -> list[str]:
        """Sorted unique market ids on this tape."""
        if self._df.height == 0:
            return []
        return sorted(self._df.get_column("market_id").unique().to_list())

    def time_range(self) -> tuple[datetime, datetime] | None:
        """(first, last) event timestamps, or None for an empty tape."""
        if self._df.height == 0:
            return None
        ts = self._df.get_column("ts")
        return ts.min(), ts.max()  # type: ignore[return-value]

    # -- replay ------------------------------------------------------------

    def replay(
        self,
        speed: float | str = 1.0,
        *,
        sleep: Callable[[float], None] | None = None,
    ) -> Iterator[Event]:
        """Yield typed events in tape order, paced against the event clock.

        ``speed=N`` plays back at N times real time: a gap of ``g``
        seconds between consecutive events becomes a sleep of ``g / N``
        wall seconds. ``speed="max"`` never sleeps. Ordering is by
        ``(ts, seq)``: simultaneous events of any type replay in the
        order the venue sequenced them.

        ``sleep`` is injectable so tests (or custom pacers) can observe
        the exact sleep schedule without waiting.
        """
        if isinstance(speed, str):
            if speed != "max":
                raise OpenTapeError(f'speed must be a positive number or "max", got {speed!r}')
            factor = None
        else:
            factor = float(speed)
            if factor <= 0:
                raise OpenTapeError(f'speed must be a positive number or "max", got {speed!r}')
        if sleep is None:
            sleep = time.sleep

        prev_ts: datetime | None = None
        for row in self._df.iter_rows(named=True):
            ts: datetime = row["ts"]
            if factor is not None and prev_ts is not None:
                gap = (ts - prev_ts).total_seconds() / factor
                if gap > 0:
                    sleep(gap)
            prev_ts = ts
            yield from_row(row)

    # -- order book --------------------------------------------------------

    def book_at(self, market_id: str, ts: datetime | None = None) -> OrderBook:
        """Rebuild ``market_id``'s order book as of ``ts``.

        ``ts`` defaults to the end of the tape. The result is built from
        the last snapshot at or before ``ts`` plus every delta between
        the two, and it carries both of those facts so a caller can see
        how it was derived. Raises :class:`OpenTapeError` when the
        market is not on the tape, or when no snapshot precedes ``ts``.
        """
        if market_id not in self.market_ids():
            known = ", ".join(self.market_ids()) or "none"
            raise OpenTapeError(f"no market {market_id!r} on this tape; markets: {known}")
        if ts is None:
            rng = self.time_range()
            if rng is None:
                raise OpenTapeError("cannot rebuild a book from an empty tape")
            ts = rng[1]
        elif ts.tzinfo is None:
            raise OpenTapeError(f"timestamp {ts!r} is naive; pass a timezone-aware datetime")

        window = self._df.filter(
            (pl.col("market_id") == market_id)
            & pl.col("event_type").is_in([schema.EVENT_SNAPSHOT, schema.EVENT_DELTA])
            & (pl.col("ts") <= ts)
        )
        events = (from_row(row) for row in window.iter_rows(named=True))
        return reconstruct(events, market_id=market_id)

    # -- SQL ---------------------------------------------------------------

    def sql(self, query: str) -> pl.DataFrame:
        """Run DuckDB SQL over the tape and return a Polars frame.

        Available relations: ``events`` (the raw flat table) and the
        per-type views ``markets``, ``snapshots``, ``deltas``,
        ``trades``, ``status``, ``resolutions``.
        """
        con = self._connection()
        try:
            result = con.execute(query).arrow()
        except duckdb.Error as exc:
            raise OpenTapeError(f"SQL error: {exc}") from exc
        out = pl.from_arrow(result)
        assert isinstance(out, pl.DataFrame)
        return out

    def _connection(self) -> duckdb.DuckDBPyConnection:
        if self._con is None:
            con = duckdb.connect(":memory:")
            con.execute("SET TimeZone = 'UTC'")
            con.register("events", self._df.to_arrow())
            for _name, ddl in _VIEWS:
                con.execute(ddl)
            self._con = con
        return self._con

    # -- summary -----------------------------------------------------------

    def summary(self) -> TapeSummary:
        """Aggregate statistics used by ``opentape inspect``."""
        df = self._df
        counts = dict.fromkeys(schema.EVENT_TYPES, 0)
        if df.height:
            observed = df.group_by("event_type").len().iter_rows()
            counts.update({t: int(n) for t, n in observed})

        stats: list[MarketPriceStats] = []
        trades = df.filter(pl.col("event_type") == schema.EVENT_TRADE)
        if trades.height:
            agg = (
                trades.group_by("market_id")
                .agg(
                    pl.len().alias("trades"),
                    pl.col("price").min().alias("min_price"),
                    pl.col("price").mean().alias("mean_price"),
                    pl.col("price").max().alias("max_price"),
                    pl.col("price").sort_by(["ts", "seq"]).last().alias("last_price"),
                    ((pl.col("price") * pl.col("size")).sum() / pl.col("size").sum()).alias("vwap"),
                )
                .sort("market_id")
            )
            stats = [MarketPriceStats(**row) for row in agg.iter_rows(named=True)]

        rng = self.time_range()
        return TapeSummary(
            schema_version=self.schema_version,
            n_events=df.height,
            markets=tuple(self.market_ids()),
            sources=tuple(sorted(df.get_column("source").unique().to_list()) if df.height else ()),
            event_counts=counts,
            start=rng[0] if rng else None,
            end=rng[1] if rng else None,
            price_stats=tuple(stats),
        )
