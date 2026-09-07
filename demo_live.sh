#!/bin/sh
# Demo: capture from a real venue.
#
# Unlike demo.sh, this one needs the network. It uses only public,
# unauthenticated endpoints, so it needs no credentials and no account.
# It is not run in CI, because a demo whose result depends on someone
# else's uptime is not a test.
#
# Usage: ./demo_live.sh [venue] [duration]
set -eu
cd "$(dirname "$0")"

VENUE="${1:-polymarket}"
DURATION="${2:-30s}"
OUT="${TMPDIR:-/tmp}/opentape-live-${VENUE}.parquet"

run() { echo; echo "\$ $*"; "$@"; }

echo "Finding an open market on $VENUE ..."
run uv run opentape markets --venue "$VENUE" --limit 5

MARKET="$(uv run opentape markets --venue "$VENUE" --limit 1 | awk '{print $1}')"
if [ -z "$MARKET" ]; then
    echo "no open markets were returned by $VENUE; try again later" >&2
    exit 1
fi

rm -f "$OUT"
run uv run opentape capture --venue "$VENUE" --market "$MARKET" \
    -o "$OUT" --poll 2s --duration "$DURATION"
run uv run opentape inspect "$OUT"
run uv run opentape replay "$OUT" --limit 10
run uv run opentape book "$OUT" --depth 5

# The websocket transport, where the venue publishes its own changes
# instead of being asked. Only Polymarket has a public stream that needs
# no credentials: Kalshi's answers HTTP 401 to an unauthenticated
# upgrade, so there is nothing here to run for it.
if [ "$VENUE" = "polymarket" ]; then
    WS_OUT="${TMPDIR:-/tmp}/opentape-live-${VENUE}-ws.parquet"
    rm -f "$WS_OUT"
    echo
    echo "== the same market over the venue's websocket =="
    run uv run opentape capture --venue "$VENUE" --transport websocket \
        --market "$MARKET" -o "$WS_OUT" --duration "$DURATION"
    run uv run opentape inspect "$WS_OUT"
    run uv run opentape replay "$WS_OUT" --limit 10
fi
