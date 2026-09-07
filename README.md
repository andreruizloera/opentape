# opentape

A standard format and replay engine for prediction-market data.

Prediction markets are the cheapest source of real order-flow data
anywhere, but every venue ships it in a different shape: cents here,
decimal strings there, YES books and NO books, signed deltas and
absolute deltas. OpenTape defines one exchange-neutral Parquet schema
for market history and gives you the tools around it: a typed Python
API, a timestamp-faithful replay engine, DuckDB SQL over any tape,
adapters that convert common data shapes into the standard, and a
capture daemon that records live venues into it.

[![CI](https://github.com/andreruizloera/opentape/actions/workflows/ci.yml/badge.svg)](https://github.com/andreruizloera/opentape/actions/workflows/ci.yml)
![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue)
![License: MIT](https://img.shields.io/badge/license-MIT-green)

## Quickstart

```sh
git clone https://github.com/andreruizloera/opentape
cd opentape
uv sync
uv run opentape inspect examples/sample.parquet
```

`examples/sample.parquet` is a bundled, clearly-synthetic tape (three
invented markets, seeded generator, no real exchange data), so the
commands below work immediately after cloning.

## Example output

`opentape inspect` summarizes a tape:

```
$ opentape inspect examples/sample.parquet
tape           : examples/sample.parquet
schema version : 1
events         : 6,973
markets        : 3
sources        : synthetic
time range     : 2026-03-02T14:30:07.485Z to 2026-03-02T15:55:22.635Z (1h 25m 15s)

event counts
  market                3
  book_snapshot        35
  book_delta        5,692
  trade             1,236
  market_status         5
  resolution            2

trade prices
  market             trades    min   mean    max   last   vwap
  OT-BTC-150K-Q3        262   0.49   0.52   0.57   0.51   0.52
  OT-FEDCUT-SEP26       556   0.54   0.70   0.86   0.86   0.71
  OT-RAINSEA-0902       418   0.12   0.27   0.38   0.12   0.26
```

`opentape replay` streams the events in tape order (add `--speed 60`
to pace them at 60x real time):

```
$ opentape replay examples/sample.parquet --limit 10
[2026-03-02T14:30:07.485Z] seq=     0 MARKET      OT-RAINSEA-0902  "Will it rain in Seattle on 2026-09-02?"
[2026-03-02T14:30:07.485Z] seq=     1 STATUS      OT-RAINSEA-0902  open
[2026-03-02T14:30:07.673Z] seq=     2 SNAPSHOT    OT-RAINSEA-0902  5x5 levels, best 0.37/0.38
[2026-03-02T14:30:15.545Z] seq=     3 BOOK_DELTA  OT-RAINSEA-0902  bid  0.35 set 630
[2026-03-02T14:30:15.545Z] seq=     4 BOOK_DELTA  OT-RAINSEA-0902  bid  0.36 set 180
[2026-03-02T14:30:15.545Z] seq=     5 BOOK_DELTA  OT-RAINSEA-0902  bid  0.37 set 220
[2026-03-02T14:30:15.545Z] seq=     6 BOOK_DELTA  OT-RAINSEA-0902  ask  0.39 set 540
[2026-03-02T14:30:15.859Z] seq=     7 TRADE       OT-RAINSEA-0902  YES  sell 0.37 x 47
[2026-03-02T14:30:16.092Z] seq=     8 TRADE       OT-RAINSEA-0902  YES  buy  0.38 x 27
[2026-03-02T14:30:19.182Z] seq=     9 MARKET      OT-FEDCUT-SEP26  "Will the Fed cut rates at the September 2026 meeting?"
```

## Recording a live venue

`opentape capture` polls a venue's public endpoints and writes the
same canonical tapes. Only unauthenticated endpoints are used, so
there is nothing to sign up for and no key to set. Find a market, then
record it:

```
$ opentape markets --venue polymarket --limit 3
xi-jinping-out-before-2027                                            Xi Jinping out before 2027?  (2027-01-01T00:00:00Z)
will-gavin-newsom-win-the-2028-democratic-presidential-nomination-568  Will Gavin Newsom win the 2028 Democratic presidential nomination?  (2028-11-07T00:00:00Z)
will-alexandria-ocasio-cortez-win-the-2028-democratic-presidential-nomination-653  Will Alexandria Ocasio-Cortez win the 2028 Democratic presidential nomination?  (2028-11-07T00:00:00Z)
```

A real one-minute run against Polymarket's five-minute Bitcoin market,
on 2026-09-07:

```
$ opentape capture --venue polymarket --market btc-updown-5m-1788755100 \
      -o btc.parquet --poll 1s --duration 60s
capturing 1 market(s) from polymarket for 60s
polling every 1s; press Ctrl-C to stop and write what has been captured
tracking btc-updown-5m-1788755100 (open): Bitcoin Up or Down - September 7, 12:25AM-12:30AM ET
wrote btc.parquet: 1,259 events
captured 1,259 events over 60 polls (1 snapshots, 1,256 deltas, 0 trades)

$ opentape replay btc.parquet --limit 6
[2026-09-07T04:24:08.743Z] seq=     0 MARKET      btc-updown-5m-1788755100 "Bitcoin Up or Down - September 7, 12:25AM-12:30AM ET"
[2026-09-07T04:24:08.743Z] seq=     1 STATUS      btc-updown-5m-1788755100 open
[2026-09-07T04:24:08.743Z] seq=     2 SNAPSHOT    btc-updown-5m-1788755100 50x49 levels, best 0.50/0.51
[2026-09-07T04:24:10.083Z] seq=     3 BOOK_DELTA  btc-updown-5m-1788755100 bid  0.49 set 240
[2026-09-07T04:24:10.083Z] seq=     4 BOOK_DELTA  btc-updown-5m-1788755100 ask  0.51 set 30
[2026-09-07T04:24:10.083Z] seq=     5 BOOK_DELTA  btc-updown-5m-1788755100 ask  0.52 set 214.38
```

That tape is an ordinary tape, so the rest of the tooling works on it:

```
>>> Tape.read("btc.parquet").sql("""
...     SELECT side, count(*) AS changes, count(DISTINCT price) AS levels,
...            round(min(price), 2) AS lo, round(max(price), 2) AS hi
...     FROM deltas GROUP BY side ORDER BY side
... """)
shape: (2, 5)
┌──────┬─────────┬────────┬──────┬──────┐
│ side ┆ changes ┆ levels ┆ lo   ┆ hi   │
│ ---  ┆ ---     ┆ ---    ┆ ---  ┆ ---  │
│ str  ┆ i64     ┆ i64    ┆ f64  ┆ f64  │
╞══════╪═════════╪════════╪══════╪══════╡
│ ask  ┆ 606     ┆ 64     ┆ 0.36 ┆ 0.99 │
│ bid  ┆ 650     ┆ 63     ┆ 0.01 ┆ 0.63 │
└──────┴─────────┴────────┴──────┴──────┘
```

`./demo_live.sh` runs that whole sequence against a market it picks
itself. It is the one script here that needs the network.

### What a polled tape does and does not claim

Capture goes over REST polling, not a websocket, and the difference is
recorded rather than glossed over.

- **A poll interval is a sampling rate.** A level that appears and
  disappears between two polls is not in the tape, and a `book_delta`
  means "this level differs from the last poll", not "the venue
  published this change". The `source` column says `kalshi-rest-poll`
  or `polymarket-rest-poll`, so a consumer can tell a polled tape from
  a streamed one without being told.
- **A failed poll is a gap.** After one, the book held in memory is of
  unknown age, so the next successful poll writes a full snapshot
  instead of deltas measured against a book nobody confirmed.
  `--snapshot-every N` adds routine snapshots as recovery points.
- **Only trades observed to arrive are recorded.** Both venues' trade
  endpoints answer with recent history, so the first poll would
  otherwise dump a page of trades that executed before the capture
  began. Pass `--backfill N` to keep them deliberately; they carry
  their venue timestamps and so sort before the market row that opens
  the tape.
- **Trade timestamps come from the venue, and can lag.** Polymarket's
  public trades endpoint has been observed publishing a trade around a
  minute after it executed, so a tape's earliest trade can predate the
  capture's own start even with the default `--backfill 0`.
- **Kalshi book events carry local capture time.** Its orderbook
  endpoint publishes no timestamp of its own. Polymarket's does, and
  that one is used.

## Why?

Everyone who backtests against Kalshi, Polymarket, or a private book
writes the same three things from scratch: a normalization layer, a
replay loop, and ad hoc analytics. They all make slightly different
choices (cents or fractions? are deltas signed? which clock wins a
tie?) and none of the resulting files interoperate. OpenTape fixes the
choices once, in a documented, versioned schema (SCHEMA.md), and keeps
the container boring: plain Parquet that any Arrow-family tool reads
without this library.

The important choices, spelled out:

- Prices are decimal fractions of 1, always. They read as
  probabilities and compare across venues.
- Timestamps are UTC epoch microseconds, exposed tz-aware. Naive
  timestamps are rejected, not guessed at.
- A global sequence number gives replay a total order: events sharing
  a timestamp (a trade and the book change it caused) always replay in
  venue order.
- Book deltas carry the new absolute size at a level, so they are
  idempotent and gap-detectable; adapters convert signed feeds.
- Binary markets are expressed in YES terms; a NO bid at q becomes a
  YES ask at 1 - q.
- Every row carries `schema_version`, so files stay honest about what
  they are as the schema evolves.

## Installation

Requires Python 3.12+.

```sh
uv sync          # in a clone, or:
uv pip install . # into an existing environment, or:
pip install .
```

Not on PyPI yet (see ROADMAP.md). Dependencies: Polars, PyArrow,
DuckDB.

## Usage

### Python API

```python
from opentape import Tape

tape = Tape.read("examples/sample.parquet")

# Replay at 10x real time (sleeps are scaled down by 10).
for event in tape.replay(speed=10):
    print(event)

# Replay as fast as possible, no sleeping.
for event in tape.replay(speed="max"):
    ...

# DuckDB SQL over the tape's views:
# events, markets, snapshots, deltas, trades, status, resolutions.
tape.sql("SELECT * FROM trades WHERE price > 0.7")
```

The SQL example, for real:

```
>>> tape.sql("""
...     SELECT market_id, count(*) AS trades, round(avg(price), 3) AS avg_price
...     FROM trades WHERE price > 0.7
...     GROUP BY market_id ORDER BY trades DESC
... """)
shape: (1, 3)
┌─────────────────┬────────┬───────────┐
│ market_id       ┆ trades ┆ avg_price │
│ ---             ┆ ---    ┆ ---       │
│ str             ┆ i64    ┆ f64       │
╞═════════════════╪════════╪═══════════╡
│ OT-FEDCUT-SEP26 ┆ 242    ┆ 0.798     │
└─────────────────┴────────┴───────────┘
```

Replay yields typed events (`Market`, `OrderBookSnapshot`,
`BookDelta`, `Trade`, `MarketStatus`, `Resolution`) in `(ts, seq)`
order. `speed=N` means N times real time; the sleep function is
injectable (`tape.replay(speed=10, sleep=my_sleep)`) so simulations
and tests can control the clock. Writing works the other way around:
build events, then `Tape.from_events(events).write("out.parquet")`.

### CLI

```sh
opentape inspect examples/sample.parquet
opentape replay  examples/sample.parquet --speed 60
opentape replay  examples/sample.parquet --limit 50      # speed defaults to "max"
opentape convert data.json --format kalshi-style -o tape.parquet
opentape convert data.json --format polymarket-style
opentape convert events.csv --format generic

opentape markets --venue kalshi --search bitcoin
opentape capture --venue kalshi --market TICKER -o tape.parquet --duration 10m
opentape capture --venue polymarket --market SLUG -o tape.parquet \
    --poll 1s --rotate 5m          # numbered segments, each a valid tape
```

`capture` runs until `--duration` elapses, or until Ctrl-C, which
stops after the current poll and writes what it has rather than
discarding it. `--rotate` writes `tape-0001.parquet`,
`tape-0002.parquet`, and so on; every segment repeats the market
definition rows for the markets in it, so a segment is readable on its
own.

### Adapters

Adapters convert local files in documented shapes into canonical
tapes. Working example inputs for all three live in
`examples/fixtures/`, and each adapter module's docstring specifies
the exact shape it accepts:

- `kalshi-style`: Kalshi API conventions. Integer-cent prices,
  YES/NO books, "yes"/"no" taker sides, signed orderbook deltas
  (resolved to absolute sizes against the running book).
- `polymarket-style`: Polymarket CLOB conventions. Decimal-string
  prices and sizes, BUY/SELL sides, epoch second or millisecond
  timestamps, hex condition ids.
- `generic`: canonical field names in JSON or CSV, for pipelines that
  already speak OpenTape terms.

Adapters are file converters by design: they never call an exchange
API. For reading a venue directly, see `opentape capture` above, which
uses the separate `LiveSource` interface.

### Live sources

| Venue | Command | Credentials | Endpoints |
| --- | --- | --- | --- |
| Kalshi | `--venue kalshi` | none | `api.elections.kalshi.com/trade-api/v2` markets, orderbook, and public trades |
| Polymarket | `--venue polymarket` | none | `clob.polymarket.com` markets and book, `data-api.polymarket.com` trades, `gamma-api.polymarket.com` slug lookup |

Both venues are read through their public, unauthenticated endpoints,
which is a deliberate limit: everything here works from a clean clone
with no account. Kalshi's authenticated API (private order state, the
websocket feed) and Polymarket's authenticated CLOB endpoints need
keys and are not implemented; see ROADMAP.md. Writing another venue
means implementing `LiveSource` (`list_markets`, `describe`, `book`,
`trades`) and registering it in `SOURCES`, which is also where an
authenticated one would go, since the HTTP fetcher is injected and can
carry whatever headers a venue wants.

Both sources translate into the canonical YES terms the schema
requires. Kalshi's NO ladder becomes canonical asks at the
complementary price; Polymarket trades on the NO token become
YES-terms trades with both the price complemented and the side
flipped. Polymarket's binary markets are not all spelled Yes/No, so a
market like `btc-updown-5m-...` maps Up to the YES side and records
`("Up", "Down")` on the tape's market row.

## Architecture

```
src/opentape/
  schema.py        the canonical column set, dtypes, validation, SCHEMA_VERSION
  events.py        typed event classes and the row <-> event mapping
  tape.py          Tape: read/write Parquet, replay pacing, DuckDB views, summary
  cli.py           opentape inspect | replay | convert | markets | capture
  synthetic.py     seeded synthetic tape generator (bursts, drift, resolution)
  adapters/
    generic.py            canonical-shaped CSV/JSON
    kalshi_style.py       Kalshi-style JSON
    polymarket_style.py   Polymarket-style JSON
  live/
    http.py               the only module that opens a socket; injectable
    base.py               LiveSource, plus BookTracker and TradeDeduper
    kalshi.py             Kalshi public REST
    polymarket.py         Polymarket public REST
    daemon.py             the poll loop, rotation, gap handling
```

Live capture keeps the same split the rest of the library uses. All
network I/O is behind one `Fetcher` callable, so the venue sources are
pure functions of the JSON they are handed and the whole test suite
runs offline against recorded payloads (`tests/fixtures/live/`).
Turning repeated full books into snapshots and deltas is shared in
`BookTracker` rather than repeated per venue, so a new venue only has
to answer what a market is, what its book is, and what has traded.

A tape is one flat Parquet table: every event is a row, unused columns
are null, and `(ts, seq)` defines the total order. SCHEMA.md explains
the format and the reasoning behind each choice. `Tape.sql()` registers
the table with an in-memory DuckDB connection plus per-type views, so
analytics run at DuckDB speed without any copies beyond Arrow handoff.

The bundled `examples/sample.parquet` is generated by
`examples/generate_sample.py` (seeded, deterministic; a test asserts
the committed file matches the generator's output exactly). Its three
markets follow a random walk with drift, one-cent ladders, burst
windows with heavier trading, and two resolutions.

## Limitations

- Live capture polls REST; there is no websocket transport, so a tape
  samples the book rather than subscribing to it. See "What a polled
  tape does and does not claim" above for what that costs.
- Only the two venues' unauthenticated endpoints are supported. Nothing
  here reads private order state, and no venue that requires a key is
  implemented.
- A market's status is read once when the capture starts. A market that
  closes mid-capture keeps its opening status on that tape.
- Polymarket's public trades endpoint publishes no per-fill id, so
  deduplication uses a composite key (transaction, taker, token, size,
  price). Two identical fills by one taker in one transaction would
  collapse into one.
- Adapters target the documented Kalshi-style and Polymarket-style
  file shapes, not every endpoint variant those venues expose.
- One tape is one file; there is no partitioning or catalog story for
  multi-day archives yet. `--rotate` produces numbered segments, but
  nothing indexes them.
- Order-level (add/cancel/modify per order id) microstructure is out
  of scope for schema v1, which is level-based.
- CSV input cannot carry order book snapshots; use JSON for books.

## Roadmap

See ROADMAP.md. Highlights: websocket transports for both venues,
authenticated feeds, book reconstruction and top-of-book derivation,
tape slicing and merging, multi-file datasets, and a PyPI release.

## Contributing

See CONTRIBUTING.md. Short version: `uv sync`, make your change, then
`uv run ruff format . && uv run ruff check . && uv run pytest`.

## License

MIT, see LICENSE.

GitHub topics: `prediction-markets`, `market-data`, `quant`,
`parquet`, `duckdb`.
