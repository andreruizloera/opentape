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

`opentape capture` reads a venue's public market data and writes the
same canonical tapes. Only unauthenticated endpoints are used, so
there is nothing to sign up for and no key to set. There are two
transports: `--transport rest-poll`, the default, samples the book on
an interval, and `--transport websocket` subscribes to the venue's own
change stream. Find a market, then record it:

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

### Reading the book back

A tape stores a book as one snapshot plus per-level changes, which is
compact but is not a book you can read. `opentape book` folds it back
into a ladder, at the end of the tape or at any moment inside it. On
the capture above, forty seconds apart:

```
$ opentape book btc.parquet --at 2026-09-07T04:24:30Z --depth 3
market   : btc-updown-5m-1788755100
as of    : 2026-09-07T04:24:29.662Z
built    : snapshot at 2026-09-07T04:24:08.743Z plus 114 deltas
top      : 0.4900 / 0.5000  (mid 0.4950, spread 0.0100)
levels   : 49 bid, 50 ask

      bid size     bid | ask     ask size
         32.27  0.4900 | 0.5000  158.00
         81.00  0.4800 | 0.5100  158.00
         87.50  0.4700 | 0.5200  214.40

$ opentape book btc.parquet --depth 3
market   : btc-updown-5m-1788755100
as of    : 2026-09-07T04:25:09.002Z
built    : snapshot at 2026-09-07T04:24:08.743Z plus 1,256 deltas
top      : 0.6300 / 0.6400  (mid 0.6350, spread 0.0100)
levels   : 63 bid, 36 ask

      bid size     bid | ask     ask size
         29.35  0.6300 | 0.6400  267.33
        183.00  0.6200 | 0.6500  153.76
         60.00  0.6100 | 0.6600  130.00
```

A book cannot be rebuilt from deltas alone: a delta says what one
level became, never what the rest of the book was. A time with no
snapshot before it is therefore an error rather than a book made only
of the levels that happened to change. `Tape.book_at(market_id, ts)`
is the same thing from Python, and the returned `OrderBook` carries
`best_bid`, `best_ask`, `spread`, `mid`, and the snapshot and delta
count it was built from.

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

### A market that closes while you are recording it

A capture re-reads each market's lifecycle status every `--status-every`
(default 30s) and writes a `market_status` row at the point a change is
observed. Both transports do it, because a change stream carries book
and trade messages, not lifecycle. This part of `./demo.sh` runs a
scripted venue rather than a real one so it works offline, but the
daemon, the tape, and the status rows are the shipped ones:

```
$ python: a capture across a market's close
tracking DEMO-CLOSE (open): Will the demo market close?
DEMO-CLOSE changed status: open -> closed
wrote /tmp/.../closing.parquet: 9 events
status changes observed: 1
  seq=1  15:20:00  status=open
  seq=7  15:20:04  status=closed
```

The row is stamped when the change was **observed**, not when the venue
made it: a poller cannot know the second one and does not guess. With
`--rotate`, a segment that saw the close opens with `open` and carries
the transition in its body, and every later segment opens with
`closed`, so each segment still reads correctly on its own.

`--no-status-check` restores the old behaviour of asking exactly once.

### A market that settles while you are recording it

`market_status: closed` says a market stopped trading. It does not say
how it settled, and those are two different facts that arrive at two
different times. When the venue publishes a winner, the capture writes
a `resolution` row naming the winning outcome and what it pays. This
comes out of the same document the status check already reads, so
watching for it costs no extra request and `--status-every` paces both.

Continuing the same offline part of `./demo.sh`, with the scripted
venue closing on one check and settling on the next:

```
$ python: a capture across a market's close and its settlement
tracking DEMO-CLOSE (open): Will the demo market close?
DEMO-CLOSE changed status: open -> closed
DEMO-CLOSE resolved: YES settles at 1
wrote /tmp/.../closing.parquet: 12 events
status changes observed: 1
settlements observed:    1
  seq=1  15:20:00  status=open
  seq=7  15:20:04  status=closed
  seq=10  15:20:06  resolution=YES settles at 1
```

**The gap between those two rows is the reason this exists, and it is
not a demo artifact.** A Kalshi market advertises
`settlement_timer_seconds: 5`, and one recorded for the test suite
(`tests/fixtures/live/kalshi_market_closed.json`) had closed at
17:30 UTC and was still answering `result: ""` with no settlement value
when it was captured 28 minutes later. A tape that only recorded the
close would say nothing about how that market ended.

Three details worth knowing before reading a `resolution` row:

- **`outcome` names the winner, not the YES side.** It is the one place
  in the schema where that column is not the string `YES`. What YES
  settled at is recoverable from the tape alone: the market row lists
  `outcomes` with the YES side first, so YES settled at `settlement`
  when `outcome` is `outcomes[0]` and at `1 - settlement` otherwise.
- **Kalshi's row carries the venue's own settlement time.** It
  publishes `settlement_ts`, so unlike a status row the timestamp is
  not merely an upper bound set by `--status-every`. Polymarket
  publishes nothing comparable, so its resolution rows are stamped when
  the capture observed them.
- **Rotation follows the same rule the status header does.** The
  segment that watched the settlement carries the observed row in its
  body; every later segment opens with a `resolution` header, so a
  reader who picks up a late segment alone still learns how the market
  ended.

A settlement is written once per market per capture. A venue that
changes its mind after publishing a winner is not written a second
time; see ROADMAP.md.

### Stopping once there is nothing left to record

A market that has closed and settled will not trade again, so every
poll after that point asks the venue for a book that cannot move.
`--stop-when-settled` stops asking about such a market, and ends the
capture once every market it tracks has reached that state.

The last part of `./demo.sh` runs the same scripted venue twice, with
the same sixty-second duration, changing nothing but the flag:

```
$ python: the same capture with and without --stop-when-settled
             default: 60 polls, 150 venue requests,   60s of the 60s asked for, stopped_early=False
 --stop-when-settled:  7 polls,  18 venue requests,    6s of the 60s asked for, stopped_early=True
```

**Both halves are required, and that is not caution in the abstract.**
A status can flap: polling Polymarket's `btc-updown-5m-1788794400`
every 20 seconds on 2026-09-07, the three consecutive reads at
15:37:02, 15:37:22, and 15:37:42 answered closed, then open, then
closed. A capture that ended on the first `closed` would have thrown
away a market that the venue then reported open again. A resolution
cannot do that, because it is written once and never revised, so
requiring one is what makes ending a capture safe to hang on this. A
market that closes and never settles holds the capture open, which is
the Kalshi case above: closed at 17:30 and still unsettled 28 minutes
later, in exactly the window a settlement is most likely to arrive.

Four things this does and does not do:

- **It is off by default.** A capture that ends before the `--duration`
  you asked for is a surprise, and it should be one you requested.
- **It narrows before it stops.** With several markets, the ones that
  have settled stop being polled while the rest keep going, so a
  capture of ten markets does not pay for ten once nine are over.
- **A market that was already over when the capture opened still gets
  one poll**, on the polling transport. That poll is what makes the
  tape worth having: it records the final book under a header that
  already carries the closed status and the winning outcome, so asking
  for a capture of a market that is finished gives you a picture of how
  it ended rather than an empty directory.
- **On `--transport websocket` it narrows the lifecycle re-read only.**
  A subscription is sent once when the connection opens, so dropping
  one market from a live stream would mean tearing the connection down
  and re-subscribing, which discards every mirrored book and puts a gap
  in the tape for the markets still trading. The whole capture still
  ends when every market has settled, and a streamed capture of a
  market that is already over does not open the connection at all,
  because a stream has no equivalent of that one poll: a snapshot
  arrives when the venue chooses to send one, and on a market that
  settled hours ago it may never arrive.

Nothing about the tape changes. The reason a capture stopped is
already on it, as the `resolution` rows that ended it.

### Streaming instead of polling

`--transport websocket` subscribes to the venue's change stream, so a
`book_delta` in the tape is a change the venue published rather than a
difference between two samples. A real three-minute run on 2026-09-07:

```
$ opentape capture --venue polymarket --transport websocket \
      --market lol-ig1-lgd-2026-09-08 -o ig.parquet --duration 3m
streaming 1 market(s) from polymarket for 3m
connecting to wss://ws-subscriptions-clob.polymarket.com/ws/market; press Ctrl-C to stop and write the tape
tracking lol-ig1-lgd-2026-09-08 (open): LoL: Invictus Gaming vs LGD Gaming (BO5) - LPL Playoffs
subscribed to 1 market(s) on wss://ws-subscriptions-clob.polymarket.com/ws/market
wrote ig.parquet: 24 events
captured 24 events from 24 messages (3 snapshots, 17 deltas, 2 trades), 2 snapshot check(s) with 0 divergence(s)

$ opentape replay ig.parquet --limit 8
[2026-09-07T13:23:46.633Z] seq=     0 MARKET      lol-ig1-lgd-2026-09-08 "LoL: Invictus Gaming vs LGD Gaming (BO5) - LPL Playoffs"
[2026-09-07T13:23:46.633Z] seq=     1 STATUS      lol-ig1-lgd-2026-09-08 open
[2026-09-07T13:23:46.633Z] seq=     2 SNAPSHOT    lol-ig1-lgd-2026-09-08 22x21 levels, best 0.63/0.64
[2026-09-07T13:24:47.175Z] seq=     3 BOOK_DELTA  lol-ig1-lgd-2026-09-08 ask  0.64 set 82495.8
[2026-09-07T13:24:47.175Z] seq=     4 SNAPSHOT    lol-ig1-lgd-2026-09-08 22x21 levels, best 0.63/0.64
[2026-09-07T13:24:47.247Z] seq=     5 TRADE       lol-ig1-lgd-2026-09-08 YES  buy  0.64 x 122.703
[2026-09-07T13:24:52.376Z] seq=     6 BOOK_DELTA  lol-ig1-lgd-2026-09-08 ask  0.64 set 82490.8
[2026-09-07T13:24:52.376Z] seq=     7 BOOK_DELTA  lol-ig1-lgd-2026-09-08 bid  0.62 set 35125.8
```

The `source` column reads `polymarket-ws` rather than
`polymarket-rest-poll`, so the transport travels with the data.

**The "snapshot check" line is the part worth explaining.** A streamed
tape is only useful if its deltas are enough to rebuild the book, and
that is a claim which can quietly stop being true. Polymarket publishes
full books periodically as well as every level change, so opentape
keeps its own book from the changes and compares it against each
published snapshot. Two comparisons happened in the run above and both
agreed. A disagreement is counted, named on stderr with the levels that
differ, and the venue's snapshot wins.

That check is also how the transport's one load-bearing assumption was
settled. A `price_change` carries a `size`, and reading it as the
level's new total rather than as an amount to add is the difference
between a correct book and a garbage one. Rather than trust the
documentation, a recorded session was replayed both ways and each
reconstruction compared against the venue's next published snapshot:
the absolute reading reproduced it exactly on both outcome tokens
across thirteen changes each, and the additive reading matched neither.
That recording is committed as `tests/fixtures/live/polymarket_ws.jsonl`
and the comparison runs offline on every push.

### What a streamed tape does and does not claim

- **Polymarket only.** Kalshi's websocket answers HTTP 401 to an
  unauthenticated upgrade, so it needs an API key and is not
  implemented; `--transport websocket --venue kalshi` says exactly that
  and points at `rest-poll`.
- **A dropped connection is a gap.** On reconnect every mirrored book is
  discarded and nothing is written for a market until the venue sends a
  fresh snapshot, because an unknown number of changes were missed and
  deltas across that hole would describe a book that never existed. The
  same rule as a failed poll, for the same reason.
- **A level change that arrives before any snapshot is dropped**, and
  the count is reported. Applying it to an empty book would produce a
  file that looks like a book and is missing every level nobody happened
  to touch.
- **Book timestamps are the venue's**, and a book's timestamp is when it
  last changed, not when it was sent. A quiet market's opening snapshot
  can therefore be minutes older than the capture that received it.
- **Fills carry no per-fill id.** The dedupe key is the transaction hash
  plus the fill's YES-terms price, size, and side, which also folds
  together the two publications of one fill if the venue ever sends it
  against both outcome tokens. In the sessions recorded so far each fill
  was published once, against the token it executed on.

### What a polled tape does and does not claim

The default transport is REST polling, and the difference is recorded
rather than glossed over.

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
- **A settlement is recorded only once the venue publishes a winner.**
  A market that has closed but not yet settled writes a `market_status`
  row and nothing else. Neither venue is asked to guess, and a
  contradictory document (two winning outcomes on a binary market) is
  refused by name rather than resolved to one of them.

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

# Rebuild the order book at any moment on the tape.
book = tape.book_at("OT-FEDCUT-SEP26")
book.best_bid, book.best_ask, book.spread, book.mid
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
opentape book    examples/sample.parquet --market OT-FEDCUT-SEP26 --depth 5
opentape book    tape.parquet --at 2026-09-07T04:24:30Z
opentape convert data.json --format kalshi-style -o tape.parquet
opentape convert data.json --format polymarket-style
opentape convert events.csv --format generic

opentape markets --venue kalshi --search bitcoin
opentape capture --venue kalshi --market TICKER -o tape.parquet --duration 10m
opentape capture --venue polymarket --market SLUG -o tape.parquet \
    --poll 1s --rotate 5m          # numbered segments, each a valid tape
opentape capture --venue polymarket --market SLUG -o tape.parquet \
    --transport websocket --duration 10m    # the venue's change stream
opentape capture --venue polymarket --market SLUG -o tape.parquet \
    --status-every 10s             # re-read the lifecycle this often
opentape capture --venue kalshi --market TICKER -o tape.parquet \
    --no-status-check              # ask once at the start and never again
opentape capture --venue kalshi --market TICKER -o tape.parquet \
    --duration 6h --stop-when-settled   # end early once it has settled
```

`capture` runs until `--duration` elapses, or until Ctrl-C, which
stops after the current poll or message and writes what it has rather
than discarding it. `--stop-when-settled` adds one more way to end: it
stops asking about a market once the venue reports it both closed and
settled, and ends the capture when every market has. `--poll`,
`--snapshot-every`, and `--backfill`
belong to `rest-poll` and are refused with a reason under
`--transport websocket`, where the venue sets the pace and publishes
its own snapshots. `--status-every`, `--no-status-check`, and
`--stop-when-settled` apply to
both, since neither venue publishes lifecycle changes on its stream.
`--rotate` writes `tape-0001.parquet`, `tape-0002.parquet`, and so on;
every segment repeats the market definition rows for the markets in
it, so a segment is readable on its own.

A lifecycle check that fails is counted and reported, never fatal. It
reads a different endpoint than the book, so losing it should not throw
away book and trade data that is arriving fine, and an unknown status
is never written down as a change. A run whose checks failed prints
`N status check(s) failed; the tape's status rows are as of the last
check that succeeded` next to its summary, so the one case where the
tape's status might be out of date says so.

`--stop-when-settled` with `--no-status-check` is allowed and prints a
note. It is not a contradiction: the lifecycle is still read once when
the capture opens, so a market that had already settled by then is
still recognised and the capture still ends immediately, which is a
real use. What cannot happen is noticing a settlement that arrives
during the capture, and a user who asked for one should be told rather
than left watching the full duration elapse.

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

The two venues publish a settlement differently, and `describe()`
normalizes both into the same `resolution` row:

| | Kalshi | Polymarket |
| --- | --- | --- |
| Settled when | `result` is `yes` or `no` | exactly one outcome token has `winner: true` |
| Settlement value | `settlement_value_dollars`, which is the **YES** side's value, complemented when NO won | the winning token's own `price` |
| Venue settlement time | `settlement_ts`, kept | not published, so the row is stamped when observed |

Kalshi's field being the YES value matters: on a market that resolved
NO it reads `0.0000`, and copying it across would write "NO won and
pays 0.00" onto the tape.

## Architecture

```
src/opentape/
  schema.py        the canonical column set, dtypes, validation, SCHEMA_VERSION
  events.py        typed event classes and the row <-> event mapping
  tape.py          Tape: read/write Parquet, replay pacing, DuckDB views, summary
  book.py          fold snapshots and deltas back into an order book (pure)
  cli.py           opentape inspect | replay | convert | book | markets | capture
  synthetic.py     seeded synthetic tape generator (bursts, drift, resolution)
  adapters/
    generic.py            canonical-shaped CSV/JSON
    kalshi_style.py       Kalshi-style JSON
    polymarket_style.py   Polymarket-style JSON
  live/
    http.py               the only module that opens an HTTP socket; injectable
    ws.py                 the only module that opens a websocket; a small RFC 6455 client
    base.py               LiveSource, plus BookTracker and TradeDeduper
    stream.py             StreamSource and BookMirror: the push-shaped seam
    kalshi.py             Kalshi public REST
    polymarket.py         Polymarket public REST
    polymarket_stream.py  Polymarket public websocket
    daemon.py             both loops, rotation, gap handling, tape writing
```

Live capture keeps the same split the rest of the library uses. All
network I/O is behind one `Fetcher` callable or one websocket the
daemon owns, so the venue sources are pure functions of the JSON or
text they are handed and the whole test suite runs offline against
recorded payloads (`tests/fixtures/live/`). Turning repeated full books
into snapshots and deltas is shared in `BookTracker` rather than
repeated per venue, so a new REST venue only has to answer what a
market is, what its book is, and what has traded. A streaming venue
implements four methods instead, of which only `parse()` carries any
venue detail: text in, canonical updates out, no clock and no socket.

The two transports differ only in how events are produced. Buffering,
segment headers, rotation, sequence numbering, and writing are shared,
which is what makes a streamed tape and a polled tape the same kind of
file. The offline tests run the real websocket client against a real
loopback server replaying recorded frames, so the handshake, the
masking, the frame parsing, and the fragment reassembly all execute on
every push rather than only during a live capture.

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

- The websocket transport covers Polymarket only. Kalshi's stream
  requires an API key, so capturing it still means REST polling, where a
  tape samples the book rather than subscribing to it. See "What a
  polled tape does and does not claim" above for what that costs.
- Only the two venues' unauthenticated endpoints are supported. Nothing
  here reads private order state, and no venue that requires a key is
  implemented.
- A streamed capture reconnects after a dropped connection, but what was
  missed while it was disconnected is gone. The tape re-snapshots rather
  than guessing, so the gap is visible, not filled.
- **A status row records the venue's flags, not the market, and those
  flags lag.** Polymarket's five-minute `btc-updown-5m-1788794400`
  covers 15:20 to 15:25 UTC. Polled every 20 seconds on 2026-09-07 it
  still reported `closed=false, accepting_orders=true` at 15:27, and
  first reported closed at **15:37:02, about twelve minutes after its
  window ended**. A row is stamped when the change was observed,
  because a poller cannot know when the venue decided.
- **The venue can disagree with itself between requests, and the tape
  will show it.** In that same run the three consecutive reads at
  15:37:02, 15:37:22, and 15:37:42 answered closed, then open, then
  closed. opentape writes what it observed and does not debounce, so a
  flapping venue produces flapping rows. Two status rows twenty seconds
  apart are a fact about the endpoint, not about the market.
- A status change is caught no sooner than the next `--status-every`,
  so the row's time is an upper bound on when the change happened, not
  the moment it did. Shortening the interval costs one request per
  market per check on a different endpoint than the book. A Kalshi
  `resolution` row is the exception, because that venue publishes its
  own `settlement_ts`.
- **A settlement is recorded once and never revised.** A venue that
  publishes a winner and later changes it leaves the first row on the
  tape and nothing else, because a second row would be
  indistinguishable from an ordinary settlement in a tape sorted by
  time. See ROADMAP.md.
- **A settlement is only seen if the capture is still running when the
  venue publishes it, and that wait is not bounded by the close.** A
  Kalshi market recorded for the test suite closed at 17:30 UTC and was
  still unsettled 28 minutes later, despite advertising
  `settlement_timer_seconds: 5`. Polymarket settles through UMA and is
  slower still, so a resolution row for it usually belongs to a
  different capture than the one that recorded the trading.
- **`--stop-when-settled` will wait forever for a market that closes
  and never settles.** That is deliberate, since the alternative is
  ending the capture in exactly the window a settlement arrives in,
  but it means the flag bounds a capture only for markets that
  actually resolve. The `--duration` is still the real bound. See
  ROADMAP.md.
- **On `--transport websocket`, `--stop-when-settled` narrows only the
  lifecycle re-read, not the subscription.** A settled market's stream
  messages keep arriving and keep being written, because dropping one
  market from a live subscription means a reconnect that discards every
  mirrored book. Only the whole-capture stop applies there.
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

See ROADMAP.md. Highlights: authenticated feeds (which is what a
Kalshi stream needs), tape slicing and merging, multi-file datasets,
and a PyPI release.

## Contributing

See CONTRIBUTING.md. Short version: `uv sync`, make your change, then
`uv run ruff format . && uv run ruff check . && uv run pytest`.

## License

MIT, see LICENSE.

GitHub topics: `prediction-markets`, `market-data`, `quant`,
`parquet`, `duckdb`.
