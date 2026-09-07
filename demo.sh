#!/bin/sh
# Demo: the full opentape loop on the bundled synthetic dataset.
set -eu
cd "$(dirname "$0")"

run() { echo; echo "\$ $*"; "$@"; }

run uv run opentape inspect examples/sample.parquet
run uv run opentape replay examples/sample.parquet --limit 15
run uv run opentape convert examples/fixtures/kalshi_style.json \
    --format kalshi-style -o /tmp/opentape-demo-kalshi.parquet
run uv run opentape inspect /tmp/opentape-demo-kalshi.parquet
run uv run opentape convert examples/fixtures/polymarket_style.json \
    --format polymarket-style -o /tmp/opentape-demo-poly.parquet
run uv run opentape replay /tmp/opentape-demo-poly.parquet
run uv run opentape book examples/sample.parquet --market OT-FEDCUT-SEP26 --depth 5

echo
echo "\$ python: tape.sql(...)"
uv run python - <<'EOF'
from opentape import Tape

tape = Tape.read("examples/sample.parquet")
print(
    tape.sql(
        """
        SELECT market_id, count(*) AS trades, round(avg(price), 3) AS avg_price
        FROM trades WHERE price > 0.7
        GROUP BY market_id ORDER BY trades DESC
        """
    )
)
EOF

# A market that closes while the capture is running. The venue is a
# scripted source rather than a real one so this part runs offline in
# CI, but the daemon, the tape, and the status rows are the shipped
# ones: only the answers to describe() are stand-ins.
echo
echo "\$ python: a capture across a market's close"
uv run python - <<'EOF'
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import mkdtemp
from typing import ClassVar

from opentape import Tape
from opentape.events import MarketStatus
from opentape.live.base import (
    BookLevel,
    BookQuote,
    LiveSource,
    MarketDescription,
    MarketRef,
)
from opentape.live.daemon import CaptureConfig, CaptureDaemon

START = datetime(2026, 9, 7, 15, 20, tzinfo=UTC)


class ClosingVenue(LiveSource):
    """Answers "open" twice, then "closed"."""

    key: ClassVar[str] = "demo"
    source_tag: ClassVar[str] = "demo-rest-poll"

    def __init__(self) -> None:
        self.asked = 0

    def list_markets(self, *, limit, search=None):
        return [MarketRef(market_id="DEMO-CLOSE", title="Demo")]

    def describe(self, market_id):
        self.asked += 1
        status = "open" if self.asked <= 2 else "closed"
        return MarketDescription(
            market_id="DEMO-CLOSE", title="Will the demo market close?", status=status
        )

    def book(self, market_id):
        self.asked_book = getattr(self, "asked_book", 0) + 1
        return BookQuote(
            bids=(BookLevel(price=0.61, size=100.0 + self.asked_book),),
            asks=(BookLevel(price=0.63, size=80.0),),
        )

    def trades(self, market_id, *, limit=100):
        return []


class Clock:
    def __init__(self):
        self.t = 0.0

    def monotonic(self):
        return self.t

    def sleep(self, seconds):
        self.t += seconds

    def now(self):
        return START + timedelta(seconds=self.t)


clock = Clock()
out = Path(mkdtemp()) / "closing.parquet"
daemon = CaptureDaemon(
    ClosingVenue(),
    CaptureConfig(
        markets=("DEMO-CLOSE",),
        output=out,
        poll_interval=1.0,
        duration=6.0,
        status_every=2.0,
    ),
    now=clock.now,
    monotonic=clock.monotonic,
    sleep=clock.sleep,
    log=print,
)
stats = daemon.run()
print(f"status changes observed: {stats.status_changes}")
for event in Tape.read(stats.files[0]).replay(speed="max"):
    if isinstance(event, MarketStatus):
        print(f"  seq={event.seq}  {event.ts:%H:%M:%S}  status={event.status}")
EOF
