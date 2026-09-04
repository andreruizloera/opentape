# Roadmap

Honest future work: none of this is implemented today.

## Live capture

- Kalshi capture daemon: authenticate against the real Kalshi API,
  subscribe to the websocket orderbook and trade channels, and write
  rotating tapes. The `kalshi-style` adapter documents the mapping;
  the daemon is the missing transport.
- Polymarket capture daemon: same, over the CLOB websocket and REST
  backfill endpoints.
- Gap detection and re-snapshot logic when a feed drops.

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
