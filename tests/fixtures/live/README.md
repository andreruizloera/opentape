# Recorded live payloads

These are real responses from the public Kalshi and Polymarket
endpoints, recorded on 2026-09-06 and trimmed to a few book levels and
a few trades so the suite stays small. The field names and value shapes
are exactly what the venues returned, which is the point: the parsers
are tested against the real API shape rather than against a guess at
it.

Two edits were made. Book and trade arrays were truncated, and in
`polymarket_trades.json` the trader identity fields (`proxyWallet`,
`name`, `pseudonym`, `bio`, and the profile image URLs) were dropped or
replaced with placeholders, because those identify real accounts and
none of them are needed to test the mapping.

Nothing here is generated or invented. Tests that use these files make
no network calls; the fetcher is injected.

## The settlement recordings

Three files describe markets at the end of their life, recorded on
2026-09-07. They exist because "a market has closed" and "a market has
settled" are different facts, and the tests should be pinned to what
the venues actually publish rather than to an assumption about it.

- `kalshi_market_settled.json` is a settled market: status `finalized`,
  `result: "yes"`, `settlement_value_dollars: "1.0000"`, and a real
  `settlement_ts`.
- `kalshi_market_closed.json` is the case that motivates the whole
  feature. It is a market that had **closed at 17:30 UTC and was still
  answering `result: ""` with no settlement value and no settlement
  timestamp when it was recorded 28 minutes later**, despite carrying
  `settlement_timer_seconds: 5`. Every field in it is as returned; the
  file was written from the same response the closed-and-unsettled
  condition was checked on, because an earlier attempt read a market as
  closed and then saved a second response in which it had already
  settled.
- `polymarket_market_resolved.json` is a resolved Polymarket market:
  `closed: true`, `accepting_orders: false`, and exactly one outcome
  token with `winner: true` and `price: 1`. Its `description` field was
  dropped to keep the file small, as it is prose the parser never
  reads; nothing else was changed.

The Polymarket rule that exactly one `winner` marks a settlement was
checked against 60 closed markets before being relied on: all 60 had
two tokens, one winner, the winner's price at 1 and the loser's at 0,
while open markets carry `winner: false` on both tokens and a live
price on each.

## The websocket recording

`polymarket_ws.jsonl` is a real session on Polymarket's public market
channel (`wss://ws-subscriptions-clob.polymarket.com/ws/market`),
recorded on 2026-09-07, one message per line in the order they arrived.
`polymarket_ws_market.json` is the CLOB `/markets/{condition}` response
for the same market, which is how a slug is resolved to the two outcome
token ids before subscribing.

One edit was made: events belonging to the other markets that the same
subscription covered were removed, so the file describes one market.
Nothing inside a kept event was changed, and in particular the book
levels were NOT truncated the way the REST fixtures were. That is
deliberate and the tests depend on it: the recording holds three full
book snapshots with the real level changes between them, so applying
only those changes to one snapshot has to reproduce the next one
exactly. Trimming the levels would break that invariant and with it the
only offline proof that a streamed tape's deltas are enough to rebuild
the market.

The blank line in the file is not an accident either. The venue really
does send empty text frames as a keepalive, and keeping one means the
parser meets one in the tests.
