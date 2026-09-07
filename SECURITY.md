# Security policy

## Threat model

opentape parses untrusted input: Parquet tapes, JSON and CSV adapter
inputs, and SQL passed to `Tape.sql()`. Parsing is delegated to
Polars, PyArrow, and DuckDB; opentape validates shapes and values on
top of them and turns malformed input into clean `OpenTapeError`
failures instead of undefined behavior.

Notes for integrators:

- `Tape.sql()` executes arbitrary SQL on an in-memory DuckDB
  connection in your process. Do not pass attacker-controlled SQL if
  your process holds secrets; DuckDB can read local files via its own
  functions.
- Adapter inputs are fully parsed before conversion; very large files
  will use memory accordingly.
- `opentape capture` and `opentape markets` make outbound HTTPS
  requests to the venue whose name you pass, and nothing else does any
  network I/O. All of it goes through `opentape.live.http`, which
  sends no credentials, reads no environment variables, and only ever
  issues GETs. The venue's JSON is treated as untrusted input and is
  validated into `LiveError` failures rather than trusted.
- A capture writes the venue's own market titles into the tape
  verbatim. Those strings come from a third party; treat them as you
  would any untrusted text if you render them.
- No credentials are stored or required, because only unauthenticated
  endpoints are used. If you add an authenticated source, keep the key
  out of the repository and out of the tape.

## Reporting a vulnerability

Email andre.x.ruizloera@gmail.com with details and a reproduction.
Please do not open a public issue for anything exploitable; you will
get a response within a week.

## Supported versions

Only the latest release line (0.x) receives fixes.
