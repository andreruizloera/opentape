# Contributing

Thanks for considering a contribution.

## Setup

```sh
git clone https://github.com/andreruizloera/opentape
cd opentape
uv sync
```

## Before you open a PR

```sh
uv run ruff format .
uv run ruff check .
uv run pytest
```

All three must pass. CI runs the same commands.

## Ground rules

- New behavior needs a test. Adapter changes need a fixture under
  `examples/fixtures/` that exercises the documented input shape.
- Tests never touch the network. Live sources take an injected
  fetcher, so a new venue is tested against a recorded payload in
  `tests/fixtures/live/`; record a real response and trim it rather
  than inventing one, and strip anything that identifies a trader.
  `./demo_live.sh` is the only script that goes online, and CI does
  not run it.
- Schema changes are the most sensitive kind of change: they need an
  update to SCHEMA.md, a decision about `SCHEMA_VERSION`, and
  round-trip tests. Open an issue first.
- If you regenerate `examples/sample.parquet`, use the committed
  script (`uv run python examples/generate_sample.py`); a test
  verifies the file matches the generator output exactly.
- Keep error messages clean and actionable; expected failures should
  never surface as raw tracebacks through the CLI.

## Reporting bugs

Open a GitHub issue with the opentape version, a minimal input file
if the bug involves an adapter, and the full command plus output.
