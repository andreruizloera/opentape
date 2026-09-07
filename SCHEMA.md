# The OpenTape schema, version 1

OpenTape stores prediction-market history as a single Parquet file
called a tape. A tape is one flat table of events: every row is one
event, every event type shares the same columns, and columns that do
not apply to a row are null. This document is the normative
specification for schema version 1.

Design goals, in priority order:

1. Exchange-neutral: any binary or categorical prediction market maps
   in without loss of ordering or price meaning.
2. One sort defines truth: replaying a tape is exactly "read rows in
   `(ts, seq)` order". No joins, no cross-table merging.
3. Plain Parquet: every column uses standard Parquet types, so DuckDB,
   Polars, pandas, Spark, and Arrow readers work with no OpenTape code.

## Columns

All columns exist in every tape, in this order.

| column           | type                          | null | meaning |
|------------------|-------------------------------|------|---------|
| `seq`            | uint64                        | no   | Global sequence number. Strictly ordered within a tape; breaks timestamp ties. Assigned by the venue where available, otherwise by the writer in arrival order. |
| `ts`             | timestamp[us, tz=UTC]         | no   | Event time as UTC epoch microseconds (Parquet physical) exposed as a tz-aware timestamp. Never naive, never local. |
| `event_type`     | utf8                          | no   | One of `market`, `book_snapshot`, `book_delta`, `trade`, `market_status`, `resolution`. |
| `market_id`      | utf8                          | no   | Venue-scoped market identifier (ticker, condition id, ...). |
| `source`         | utf8                          | no   | Tag for the originating exchange or feed (`kalshi-style`, `polymarket-style`, `synthetic`, ...). |
| `outcome`        | utf8                          | yes  | Outcome the event refers to (`YES`, `NO`, a candidate name, ...). Null when the event concerns the whole market. |
| `side`           | utf8                          | yes  | For book events: `bid` or `ask`. For trades: aggressor side, `buy` or `sell`. |
| `price`          | float64                       | yes  | Price as a decimal fraction of 1 (a probability-style price in [0, 1]). Never cents, never basis points. |
| `size`           | float64                       | yes  | Contracts. For `book_delta`, the new absolute resting size at the level (0 removes the level). |
| `status`         | utf8                          | yes  | For `market_status`: `open`, `halted`, `closed`, or a venue-specific value. |
| `title`          | utf8                          | yes  | For `market`: human-readable question. |
| `outcomes`       | list[utf8]                    | yes  | For `market`: the full outcome set. |
| `bids`           | list[struct{price: f64, size: f64}] | yes | For `book_snapshot`: bid ladder, best (highest) price first. |
| `asks`           | list[struct{price: f64, size: f64}] | yes | For `book_snapshot`: ask ladder, best (lowest) price first. |
| `event_id`       | utf8                          | yes  | Venue-assigned id (trade id, ...), when one exists. |
| `schema_version` | uint16                        | no   | The schema version this row was written under. Constant within a tape in practice. |

## Event types

Six event types cover the lifecycle of a market. The "uses" column
lists which nullable columns are populated.

| `event_type`    | Python class        | uses |
|-----------------|---------------------|------|
| `market`        | `Market`            | `title`, `outcomes` |
| `book_snapshot` | `OrderBookSnapshot` | `outcome`, `bids`, `asks` |
| `book_delta`    | `BookDelta`         | `outcome`, `side`, `price`, `size` |
| `trade`         | `Trade`             | `outcome`, `side`, `price`, `size`, `event_id` |
| `market_status` | `MarketStatus`      | `status` |
| `resolution`    | `Resolution`        | `outcome` (winner), `price` (settlement value) |

Notes:

- `market` declares a market and may repeat if metadata changes; the
  latest one wins.
- `book_snapshot` is a full statement of the book. A reader can join a
  tape mid-stream at any snapshot and apply subsequent deltas.
- `book_delta` sets the absolute size at one price level. This is
  deliberate: absolute sizes make deltas idempotent and let readers
  detect gaps, where signed changes silently corrupt state. Adapters
  for feeds that publish signed changes (Kalshi-style) resolve them to
  absolute sizes at conversion time.
- `trade` records the traded price and size; `side` is the aggressor.
- `resolution` reuses the `price` column for the settlement value per
  contract (1.0 for a winning binary outcome, 0.0 for a losing one;
  scalar markets may settle anywhere in [0, 1]). Its `outcome` column
  names the WINNING outcome as the venue spells it, which is the one
  place in the schema where that column is not the string `YES`, and
  `price` is what THAT outcome pays. What the YES side settled at is
  recoverable from the tape alone: the `market` row for the same
  `market_id` lists `outcomes` with the YES side first, so YES settled
  at `price` when `outcome` equals `outcomes[0]` and at `1 - price`
  otherwise.
- A `market_status` of `closed` and a `resolution` are different facts
  arriving at different times: the first says the market stopped
  trading, the second says how it settled. A tape can carry the first
  without the second, and a live capture routinely does.

## Ordering

The canonical order of a tape is `(ts, seq)` ascending. `seq` is
strictly increasing within a tape, so ordering is total and stable:
two events with the same timestamp (a trade and the book delta it
caused, for example) always replay in the same relative order, the one
the venue sequenced. Writers must never reuse or reorder sequence
numbers. `Tape` sorts and validates on construction, so files written
by other tools are normalized on read.

## Price and size conventions

- Prices are decimal fractions of 1. A Kalshi-style 62 cents is 0.62;
  a Polymarket-style "0.62" string is 0.62. This makes prices directly
  comparable across venues and readable as probabilities.
- For binary YES/NO markets, everything is expressed in YES terms.
  A resting NO bid at price q is economically a YES ask at 1 - q, and
  adapters normalize accordingly.
- Sizes are contracts as float64. Venues with integer contracts simply
  use whole numbers.

## Timestamps

`ts` is stored as Parquet timestamp with microsecond resolution and
UTC timezone (physically: int64 UTC epoch micros). The Python API
accepts any tz-aware datetime and normalizes it to UTC; naive
datetimes are rejected rather than guessed at.

## Versioning

`schema_version` is a column stamped on every row (currently 1). A
reader must refuse tapes whose version is greater than the newest it
understands, and this library does. Planned evolution rules:

- Adding a nullable column is a minor, compatible change: old readers
  ignore it, and the version stays the same.
- Changing a column's meaning or type, removing a column, or adding a
  new required column bumps `schema_version`.
- Readers should support at least one previous major version.

The version lives in the data rather than only in file metadata so it
survives tools that strip Parquet key-value metadata, and so tapes can
be concatenated and re-partitioned without losing it.

## Why one flat table instead of one table per event type

A tape's core job is faithful replay, and replay is a total order over
heterogeneous events. With per-type tables, that order has to be
reconstructed with a k-way merge on every read, and any consumer that
forgets the merge silently gets wrong microstructure. With one table,
the order is just the row order, and the per-type views (`trades`,
`deltas`, `snapshots`, `markets`, `status`, `resolutions`) are cheap
projections that `Tape.sql()` provides on top. Parquet's columnar
layout keeps the null-heavy columns nearly free: null pages compress
to almost nothing.
