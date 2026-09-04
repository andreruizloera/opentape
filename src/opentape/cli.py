"""The opentape command line interface: inspect, replay, convert."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

from opentape import __version__
from opentape.adapters import ADAPTERS, convert_file
from opentape.errors import OpenTapeError
from opentape.events import (
    BookDelta,
    Event,
    Market,
    MarketStatus,
    OrderBookSnapshot,
    Resolution,
    Trade,
)
from opentape.tape import Tape


def _fmt_ts(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"


def _fmt_duration(seconds: float) -> str:
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def _describe(event: Event) -> str:
    ts = _fmt_ts(event.ts)
    head = f"[{ts}] seq={event.seq:>6}"
    match event:
        case Trade():
            side = (event.side or "?").ljust(4)
            return (
                f"{head} TRADE       {event.market_id:<16} {event.outcome or '-':<4} "
                f"{side} {event.price:.2f} x {event.size:g}"
            )
        case BookDelta():
            action = "remove" if event.size == 0 else "set"
            return (
                f"{head} BOOK_DELTA  {event.market_id:<16} {event.side:<4} "
                f"{event.price:.2f} {action} {event.size:g}"
            )
        case OrderBookSnapshot():
            bb = f"{event.bids[0].price:.2f}" if event.bids else "-"
            ba = f"{event.asks[0].price:.2f}" if event.asks else "-"
            return (
                f"{head} SNAPSHOT    {event.market_id:<16} "
                f"{len(event.bids)}x{len(event.asks)} levels, best {bb}/{ba}"
            )
        case Market():
            return f'{head} MARKET      {event.market_id:<16} "{event.title}"'
        case MarketStatus():
            return f"{head} STATUS      {event.market_id:<16} {event.status}"
        case Resolution():
            return (
                f"{head} RESOLUTION  {event.market_id:<16} "
                f"{event.outcome} settles at {event.settlement:g}"
            )
    return f"{head} {event!r}"


def _cmd_inspect(args: argparse.Namespace) -> int:
    tape = Tape.read(args.tape)
    s = tape.summary()
    print(f"tape           : {args.tape}")
    print(f"schema version : {s.schema_version}")
    print(f"events         : {s.n_events:,}")
    print(f"markets        : {len(s.markets)}")
    print(f"sources        : {', '.join(s.sources) or '-'}")
    if s.start is not None and s.end is not None:
        span = (s.end - s.start).total_seconds()
        print(f"time range     : {_fmt_ts(s.start)} to {_fmt_ts(s.end)} ({_fmt_duration(span)})")
    print()
    print("event counts")
    for event_type, count in s.event_counts.items():
        print(f"  {event_type:<15} {count:>7,}")
    if s.price_stats:
        print()
        print("trade prices")
        print(
            f"  {'market':<18} {'trades':>6} {'min':>6} {'mean':>6} {'max':>6} "
            f"{'last':>6} {'vwap':>6}"
        )
        for st in s.price_stats:
            print(
                f"  {st.market_id:<18} {st.trades:>6} {st.min_price:>6.2f} "
                f"{st.mean_price:>6.2f} {st.max_price:>6.2f} {st.last_price:>6.2f} "
                f"{st.vwap:>6.2f}"
            )
    return 0


def _parse_speed(raw: str) -> float | str:
    if raw == "max":
        return "max"
    try:
        return float(raw)
    except ValueError:
        raise OpenTapeError(f'--speed must be a positive number or "max", got {raw!r}') from None


def _cmd_replay(args: argparse.Namespace) -> int:
    tape = Tape.read(args.tape)
    speed = _parse_speed(args.speed)
    shown = 0
    try:
        for event in tape.replay(speed=speed):
            print(_describe(event), flush=(speed != "max"))
            shown += 1
            if args.limit is not None and shown >= args.limit:
                break
    except BrokenPipeError:
        return 0
    return 0


def _cmd_convert(args: argparse.Namespace) -> int:
    tape = convert_file(args.input, args.format)
    out = Path(args.output) if args.output else Path(args.input).with_suffix(".parquet")
    tape.write(out)
    rng = tape.time_range()
    span = f", {_fmt_ts(rng[0])} to {_fmt_ts(rng[1])}" if rng else ""
    print(f"wrote {out}: {len(tape):,} events, {len(tape.market_ids())} market(s){span}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="opentape",
        description="A standard format and replay engine for prediction-market data.",
    )
    parser.add_argument("--version", action="version", version=f"opentape {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    p_inspect = sub.add_parser("inspect", help="summarize a tape file")
    p_inspect.add_argument("tape", help="path to a .parquet tape")
    p_inspect.set_defaults(func=_cmd_inspect)

    p_replay = sub.add_parser("replay", help="stream a tape's events to stdout")
    p_replay.add_argument("tape", help="path to a .parquet tape")
    p_replay.add_argument(
        "--speed",
        default="max",
        help='playback speed: a positive multiplier of real time (e.g. 10), or "max" '
        "(default) for no pacing",
    )
    p_replay.add_argument("--limit", type=int, default=None, help="stop after N events")
    p_replay.set_defaults(func=_cmd_replay)

    p_convert = sub.add_parser("convert", help="convert source data into a canonical tape")
    p_convert.add_argument("input", help="input file (.json or .csv depending on format)")
    p_convert.add_argument(
        "--format",
        required=True,
        choices=sorted(ADAPTERS),
        help="input data shape",
    )
    p_convert.add_argument(
        "-o", "--output", default=None, help="output .parquet path (default: input with .parquet)"
    )
    p_convert.set_defaults(func=_cmd_convert)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except OpenTapeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130


def entrypoint() -> None:
    sys.exit(main())


if __name__ == "__main__":
    entrypoint()
