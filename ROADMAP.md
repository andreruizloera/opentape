# Roadmap

Honest future work. Everything below is unimplemented unless a section
says otherwise.

## Live capture

`opentape capture` SHIPPED, over the public unauthenticated REST
endpoints of Kalshi and Polymarket, with rotation and with
re-snapshotting after a dropped poll. See the README. What is still
open:

- Websocket transports for both venues, so the tape stops being a
  sample of the book and becomes the venue's own change stream.
  Kalshi's orderbook and trade channels and Polymarket's CLOB socket
  are the two targets, and both would keep the same `LiveSource`
  boundary the REST sources use.
- Authenticated feeds: Kalshi's signed API and Polymarket's
  authenticated CLOB endpoints, both of which need keys. The fetcher
  is already injected, so this is a credentials and signing question
  rather than a structural one.
- Re-read a market's status while a capture runs, so a market that
  closes or halts mid-capture records the change instead of keeping
  the status it had when the capture started.
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

- `opentape book`: reconstruct and print the order book at any
  timestamp by folding snapshots and deltas.
- `opentape slice`: cut a tape by time range or market into a new
  valid tape; `opentape merge` for the reverse.
- `opentape diff`: compare two tapes of the same market from
  different sources.
- Top-of-book and mid-price time series derivation helpers for
  backtesting loops.

## Distribution

- Publish to PyPI.
- Prebuilt example datasets (clearly synthetic) of larger sizes for
  benchmarking.
