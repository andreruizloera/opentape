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
