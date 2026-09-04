#!/usr/bin/env python3
"""Regenerate the bundled SYNTHETIC sample tape (examples/sample.parquet).

The data is entirely synthetic: three invented markets driven by a
seeded random walk with bursts and a resolution. Run this script after
changing opentape.synthetic; the output is deterministic for a given
seed, so the committed parquet stays reproducible.

Usage: python examples/generate_sample.py [--seed 42] [--out examples/sample.parquet]
"""

from __future__ import annotations

import argparse
from pathlib import Path

from opentape.synthetic import generate


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--out",
        type=Path,
        default=Path(__file__).parent / "sample.parquet",
    )
    args = parser.parse_args()
    tape = generate(seed=args.seed)
    tape.write(args.out)
    rng = tape.time_range()
    assert rng is not None
    print(
        f"wrote {args.out}: {len(tape):,} synthetic events, "
        f"{len(tape.market_ids())} markets, {rng[0]:%Y-%m-%dT%H:%M:%SZ} to "
        f"{rng[1]:%Y-%m-%dT%H:%M:%SZ}"
    )


if __name__ == "__main__":
    main()
