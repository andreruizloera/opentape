# Roadmap

Honest future work. Everything below is unimplemented unless a section
says otherwise.

## Live capture

`opentape capture` SHIPPED, over the public unauthenticated REST
endpoints of Kalshi and Polymarket, with rotation and with
re-snapshotting after a dropped poll. See the README.

`capture --transport websocket` SHIPPED for Polymarket, over the public
market channel, with a book mirror that checks its reconstruction
against the venue's own periodic snapshots and reports divergence. The
Kalshi half of this item is NOT shipped and is blocked on the item
below it: Kalshi's websocket answers HTTP 401 to an unauthenticated
upgrade, so it needs a key. What is still open:

- Authenticated feeds: Kalshi's signed API, its websocket included, and
  Polymarket's authenticated CLOB endpoints, all of which need keys.
  The fetcher is already injected and the daemon already owns the
  socket, so this is a credentials and signing question rather than a
  structural one.
- Order-level Polymarket channels, if the venue exposes them. The
  market channel is level-based, which is what schema v1 describes, so
  this waits on the schema item below rather than on the transport.
- Use the `hash` field the venue publishes on each book and price
  change. Today the mirror is checked against full snapshots when they
  happen to arrive; a per-message hash would catch a divergence at the
  message that caused it rather than at the next snapshot.
- Reconnect with backoff that survives a long outage. Today the attempt
  count is bounded and a capture gives up rather than retrying forever,
  which is the right default for a fixed `--duration` and the wrong one
  for a daemon meant to run for a week.
- Optional debouncing for lifecycle flapping: require a status to hold
  for N consecutive checks before writing a row. Polymarket has been
  observed answering closed, open, then closed on three reads twenty
  seconds apart, and today every one of those becomes a row. It stays
  off by default, because "what the venue said when asked" is the
  honest raw record and a smoothed one cannot be recovered from it.
- Stop or narrow a capture once every market it tracks has closed.
  Today `--status-every` records the close and the loop keeps polling a
  book that will not move again, which is correct but wasteful on a
  long `--duration`.
- Resolution capture: a `resolution` row when a venue publishes the
  winning outcome. `market_status` says the market stopped trading,
  which is not the same as saying how it settled, and schema v1 already
  has the event type.
- Adaptive poll pacing: back off on a market whose book has not moved
  in a while and spend the request budget on the ones that are, which
  matters once a single capture tracks many markets.
- Rate-limit awareness beyond retrying a 429, including a shared
  budget across markets in one capture.
- A `--markets-from` file so a long capture can track a list without a
  command line full of tickers.

## Format

- Multi-file datasets: a directory-of-tapes convention (partitioned by
  day or market) with a manifest, plus `Tape.scan()` for lazy reads
  over many files.
- Schema v2 candidates: order-level events (add/cancel/replace with
  order ids), cross-market metadata (event groups, mutually exclusive
  outcome sets), and a quotes/candles derived layer.
- Zstd-level and row-group tuning benchmarks for large tapes.

## Tools

`opentape book` SHIPPED: it folds a tape's snapshots and deltas back
into a ladder at any timestamp, and `Tape.book_at()` is the same thing
from Python. What is still open:

- Top-of-book and mid-price time series derivation, so a backtest can
  get every quote change as a series instead of asking for one book at
  a time. `book.py` already computes the top of a single book; the
  missing piece is doing it incrementally across a whole tape without
  refolding from the snapshot each time.
- Cumulative depth and a notional column in the printed ladder, plus a
  `--json` output for piping.
- `opentape slice`: cut a tape by time range or market into a new
  valid tape; `opentape merge` for the reverse.
- `opentape diff`: compare two tapes of the same market from
  different sources.

## Distribution

- Publish to PyPI.
- Prebuilt example datasets (clearly synthetic) of larger sizes for
  benchmarking.
