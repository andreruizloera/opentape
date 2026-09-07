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
