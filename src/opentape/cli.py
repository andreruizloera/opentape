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
from opentape.live import (
    SOURCES,
    CaptureConfig,
    CaptureDaemon,
    HttpFetcher,
    StreamConfig,
    StreamDaemon,
    TapeStats,
    build_source,
    build_stream_source,
    parse_duration,
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


def _parse_at(raw: str | None) -> datetime | None:
    if raw is None:
        return None
    try:
        ts = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        raise OpenTapeError(
            f"--at must be an ISO 8601 timestamp such as 2026-09-07T04:25:00Z, got {raw!r}"
        ) from None
    if ts.tzinfo is None:
        raise OpenTapeError(f"--at timestamp {raw!r} has no timezone; use an offset or a Z suffix")
    return ts


def _cmd_book(args: argparse.Namespace) -> int:
    tape = Tape.read(args.tape)
    market = args.market
    if market is None:
        markets = tape.market_ids()
        if len(markets) != 1:
            raise OpenTapeError(
                f"this tape has {len(markets)} markets, so --market is required: "
                f"{', '.join(markets) or 'none'}"
            )
        market = markets[0]

    book = tape.book_at(market, _parse_at(args.at))
    shown = book.depth(args.depth)
    print(f"market   : {book.market_id}")
    print(f"as of    : {_fmt_ts(book.as_of)}")
    print(f"built    : snapshot at {_fmt_ts(book.snapshot_ts)} plus {book.deltas_applied:,} deltas")
    if book.spread is not None and book.mid is not None:
        print(f"top      : {book.best_bid.price:.4f} / {book.best_ask.price:.4f}", end="")  # type: ignore[union-attr]
        print(f"  (mid {book.mid:.4f}, spread {book.spread:.4f})")
    else:
        print("top      : one-sided book, no spread or mid")
    print(f"levels   : {len(book.bids)} bid, {len(book.asks)} ask")
    print()
    print(f"{'bid size':>14}  {'bid':>6} | {'ask':<6}  {'ask size':<14}")
    for i in range(max(len(shown.bids), len(shown.asks))):
        bid = shown.bids[i] if i < len(shown.bids) else None
        ask = shown.asks[i] if i < len(shown.asks) else None
        left = f"{bid.size:>14,.2f}  {bid.price:>6.4f}" if bid else " " * 22
        right = f"{ask.price:<6.4f}  {ask.size:<,.2f}" if ask else ""
        print(f"{left} | {right}".rstrip())
    return 0


def _cmd_markets(args: argparse.Namespace) -> int:
    source = build_source(args.venue, HttpFetcher(timeout=args.timeout))
    refs = source.list_markets(limit=args.limit, search=args.search)
    if not refs:
        what = f" matching {args.search!r}" if args.search else ""
        print(f"no open {args.venue} markets{what}")
        return 0
    width = max(len(r.market_id) for r in refs)
    for ref in refs:
        detail = f"  ({ref.detail})" if ref.detail else ""
        print(f"{ref.market_id:<{width}}  {ref.title}{detail}")
    return 0


def _report_status(stats: TapeStats) -> None:
    """Print what the lifecycle checks saw, and only when they saw something.

    A failed check is printed even though it is not fatal, because it
    is the one case where the tape's status rows might be out of date
    and nothing else in the output would say so.
    """
    if stats.status_changes:
        print(
            f"{stats.status_changes:,} market status change(s) were observed and "
            f"written to the tape"
        )
    if stats.status_check_failures:
        print(
            f"{stats.status_check_failures:,} status check(s) failed; the tape's status "
            f"rows are as of the last check that succeeded"
        )


def _status_every(args: argparse.Namespace) -> float | None:
    """Resolve --status-every and --no-status-check into one setting.

    Two flags rather than a magic zero, because "ask every N seconds"
    and "never ask again" are different intentions and a reader of a
    command line should not have to know that 0 means never.
    """
    if args.no_status_check:
        if args.status_every:
            raise OpenTapeError(
                "--status-every and --no-status-check contradict each other; pass one or the other"
            )
        return None
    return parse_duration(args.status_every or "30s", flag="--status-every")


def _cmd_capture(args: argparse.Namespace) -> int:
    if args.transport == "websocket":
        return _cmd_capture_stream(args)
    source = build_source(args.venue, HttpFetcher(timeout=args.timeout))
    config = CaptureConfig(
        markets=tuple(args.market),
        output=Path(args.output),
        poll_interval=parse_duration(args.poll or "2s", flag="--poll"),
        duration=parse_duration(args.duration, flag="--duration") if args.duration else None,
        rotate_after=parse_duration(args.rotate, flag="--rotate") if args.rotate else None,
        resnapshot_every=args.snapshot_every,
        backfill=args.backfill,
        status_every=_status_every(args),
    )
    log = (lambda msg: None) if args.quiet else (lambda msg: print(msg, flush=True))
    if not args.quiet:
        span = f" for {args.duration}" if args.duration else " until interrupted"
        print(f"capturing {len(config.markets)} market(s) from {args.venue}{span}")
        print(
            f"polling every {args.poll or '2s'}; "
            f"press Ctrl-C to stop and write what has been captured"
        )
    daemon = CaptureDaemon(source, config, log=log)
    stats = daemon.run()
    print(
        f"captured {stats.events:,} events over {stats.polls:,} polls "
        f"({stats.snapshots:,} snapshots, {stats.deltas:,} deltas, {stats.trades:,} trades"
        + (f", {stats.failed_polls:,} failed polls" if stats.failed_polls else "")
        + ")"
    )
    _report_status(stats)
    if not stats.files:
        print("no events were captured, so no tape was written")
        return 1
    for path in stats.files:
        print(f"wrote {path}")
    return 0


def _cmd_capture_stream(args: argparse.Namespace) -> int:
    """capture --transport websocket: subscribe instead of polling."""
    for flag, value in (("--poll", args.poll), ("--snapshot-every", args.snapshot_every)):
        if value:
            raise OpenTapeError(
                f"{flag} applies to --transport rest-poll only; a websocket capture has no "
                f"poll interval, and the venue publishes its own snapshots"
            )
    if args.backfill:
        raise OpenTapeError(
            "--backfill applies to --transport rest-poll only; a streamed tape holds "
            "exactly the events published while it was connected, which is the property "
            "that makes it worth streaming"
        )
    source = build_stream_source(args.venue, HttpFetcher(timeout=args.timeout))
    config = StreamConfig(
        markets=tuple(args.market),
        output=Path(args.output),
        duration=parse_duration(args.duration, flag="--duration") if args.duration else None,
        rotate_after=parse_duration(args.rotate, flag="--rotate") if args.rotate else None,
        status_every=_status_every(args),
    )
    log = (lambda msg: None) if args.quiet else (lambda msg: print(msg, flush=True))
    if not args.quiet:
        span = f" for {args.duration}" if args.duration else " until interrupted"
        print(f"streaming {len(config.markets)} market(s) from {args.venue}{span}")
        print(f"connecting to {source.stream_url()}; press Ctrl-C to stop and write the tape")
    stats = StreamDaemon(source, config, log=log).run()
    checked = (
        f", {stats.checks:,} snapshot check(s) with {stats.divergences:,} divergence(s)"
        if stats.checks
        else ""
    )
    print(
        f"captured {stats.events:,} events from {stats.messages:,} messages "
        f"({stats.snapshots:,} snapshots, {stats.deltas:,} deltas, {stats.trades:,} trades"
        + (f", {stats.reconnects:,} reconnects" if stats.reconnects else "")
        + ")"
        + checked
    )
    _report_status(stats)
    if stats.dropped_updates:
        print(
            f"{stats.dropped_updates:,} level update(s) arrived before the first snapshot "
            f"for their market and were dropped rather than applied to an empty book"
        )
    if not stats.files:
        print("no events were captured, so no tape was written")
        return 1
    for path in stats.files:
        print(f"wrote {path}")
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

    p_book = sub.add_parser(
        "book",
        help="rebuild an order book from a tape's snapshots and deltas",
        description="Fold a tape's snapshots and the deltas that follow them back into a "
        "readable ladder. A book cannot be rebuilt from deltas alone, so a time with no "
        "snapshot before it is an error rather than a partial book.",
    )
    p_book.add_argument("tape", help="path to a .parquet tape")
    p_book.add_argument(
        "--market", default=None, help="market id (optional when the tape holds only one)"
    )
    p_book.add_argument(
        "--at",
        default=None,
        metavar="TS",
        help="ISO 8601 timestamp to rebuild at (default: the end of the tape)",
    )
    p_book.add_argument("--depth", type=int, default=10, help="levels to print per side")
    p_book.set_defaults(func=_cmd_book)

    p_markets = sub.add_parser("markets", help="list open markets on a live venue")
    p_markets.add_argument("--venue", required=True, choices=sorted(SOURCES), help="venue to query")
    p_markets.add_argument("--limit", type=int, default=20, help="how many markets to show")
    p_markets.add_argument("--search", default=None, help="only show markets matching this text")
    p_markets.add_argument(
        "--timeout", type=float, default=10.0, help="per-request timeout in seconds"
    )
    p_markets.set_defaults(func=_cmd_markets)

    p_capture = sub.add_parser(
        "capture",
        help="capture a live venue and write canonical tapes",
        description="Capture a venue's public market data into a canonical tape. Only "
        "unauthenticated endpoints are used, so no credentials are needed. Two transports "
        "are available and they produce different kinds of tape. --transport rest-poll "
        "samples the book on an interval: changes that happen and reverse between two polls "
        "are not captured. --transport websocket subscribes to the venue's own change "
        "stream, so every delta in the tape is a change the venue published. The source "
        "column records which transport was used.",
    )
    p_capture.add_argument("--venue", required=True, choices=sorted(SOURCES), help="venue to poll")
    p_capture.add_argument(
        "--transport",
        choices=("rest-poll", "websocket"),
        default="rest-poll",
        help="how to read the venue: rest-poll (default) samples on --poll, websocket "
        "subscribes to the venue's change stream. Websocket is available for venues whose "
        "stream needs no credentials, which today is polymarket only",
    )
    p_capture.add_argument(
        "--market",
        action="append",
        required=True,
        metavar="ID",
        help="market to capture; repeat for several (see 'opentape markets')",
    )
    p_capture.add_argument("-o", "--output", required=True, help="output .parquet path")
    p_capture.add_argument(
        "--poll",
        default=None,
        help="poll interval for --transport rest-poll (default 2s)",
    )
    p_capture.add_argument(
        "--duration", default=None, help="stop after this long (default: run until interrupted)"
    )
    p_capture.add_argument(
        "--rotate",
        default=None,
        help="write a numbered tape segment this often, instead of one file at the end",
    )
    p_capture.add_argument(
        "--snapshot-every",
        type=int,
        default=0,
        metavar="N",
        help="re-emit a full book snapshot every N polls, so a tape has recovery points "
        "(default 0, meaning only on the first poll and after a failed one)",
    )
    p_capture.add_argument(
        "--backfill",
        type=int,
        default=0,
        metavar="N",
        help="also keep up to N already-executed trades per market from the first poll "
        "(default 0: the tape holds only trades observed to arrive during the capture). "
        "Backfilled trades carry their venue timestamp, so they sort before the market "
        "definition row that opens the tape",
    )
    p_capture.add_argument(
        "--status-every",
        default=None,
        metavar="DURATION",
        help="re-read each market's lifecycle status this often, so a market that closes "
        "or halts mid-capture is recorded when it happens (default 30s). Applies to both "
        "transports: a change stream carries book and trade messages, not lifecycle",
    )
    p_capture.add_argument(
        "--no-status-check",
        action="store_true",
        help="read each market's status once at the start and never again. The tape then "
        "states the status the capture opened with, whatever happened after",
    )
    p_capture.add_argument(
        "--timeout", type=float, default=10.0, help="per-request timeout in seconds"
    )
    p_capture.add_argument("--quiet", action="store_true", help="only print the final summary")
    p_capture.set_defaults(func=_cmd_capture)
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
