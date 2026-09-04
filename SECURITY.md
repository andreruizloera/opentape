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
- opentape performs no network I/O anywhere.

## Reporting a vulnerability

Email andre.x.ruizloera@gmail.com with details and a reproduction.
Please do not open a public issue for anything exploitable; you will
get a response within a week.

## Supported versions

Only the latest release line (0.x) receives fixes.
