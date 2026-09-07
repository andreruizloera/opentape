"""The capture daemon: poll a live source, write canonical tapes.

The loop is deliberately small. Everything venue-specific is in the
source, everything network-specific is in the fetcher, and everything
about turning repeated full books into an event stream is in
:class:`~opentape.live.base.BookTracker`. What is left here is the part
a daemon is actually responsible for: pacing, deciding when a partial
capture becomes a file, making each of those files a tape that can be
read on its own, and not losing data when the run ends or a poll
fails.

Two behaviours are worth stating plainly because they bound what a
polled tape can be trusted to say:

- A poll interval is a sampling rate, not a subscription. Anything that
  appears and disappears between two polls is not in the tape, and a
  book delta means "this level's size differs from the last poll", not
  "the venue published this change". The ``source`` column records the
  transport (``kalshi-rest-poll``, ``polymarket-rest-poll``) so a
  consumer can tell.
- A failed poll is a gap. The book held in memory may be stale by an
  unknown amount, so the next successful poll for that market emits a
  full snapshot rather than deltas measured against a book that was
  never confirmed.
"""

from __future__ import annotations

import contextlib
import re
import signal
import time
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path

from opentape.errors import LiveError, OpenTapeError
from opentape.events import (
    BookDelta,
    Event,
    Market,
    MarketStatus,
    OrderBookSnapshot,
    Trade,
)
from opentape.live.base import BookTracker, LiveSource, MarketDescription, TradeDeduper
from opentape.tape import Tape

_DURATION_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(ms|s|m|h)?\s*$", re.IGNORECASE)
_UNITS = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0}


def parse_duration(raw: str, *, flag: str) -> float:
    """Parse "30s", "5m", "1h", "500ms", or a bare number of seconds."""
    match = _DURATION_RE.match(raw)
    if not match:
        raise OpenTapeError(
            f"{flag} must be a duration like 30s, 5m, 1h, or a number of seconds, got {raw!r}"
        )
    value = float(match.group(1))
    unit = (match.group(2) or "s").lower()
    seconds = value * _UNITS[unit]
    if seconds <= 0:
        raise OpenTapeError(f"{flag} must be greater than zero, got {raw!r}")
    return seconds


@dataclass(frozen=True, slots=True)
class CaptureConfig:
    """Everything the daemon needs that is not the venue itself."""

    markets: tuple[str, ...]
    output: Path
    poll_interval: float = 2.0
    duration: float | None = None
    rotate_after: float | None = None
    resnapshot_every: int = 0
    trade_limit: int = 100
    max_consecutive_failures: int = 5
    #: How many already-executed trades per market to keep from the very
    #: first poll. Zero, the default, means the tape contains only
    #: trades that were seen to arrive while the capture was running.
    backfill: int = 0


@dataclass(slots=True)
class CaptureStats:
    """What a finished capture did."""

    polls: int = 0
    failed_polls: int = 0
    events: int = 0
    trades: int = 0
    snapshots: int = 0
    deltas: int = 0
    files: list[Path] = field(default_factory=list)


class CaptureDaemon:
    """Poll a :class:`LiveSource` on an interval and write tapes."""

    def __init__(
        self,
        source: LiveSource,
        config: CaptureConfig,
        *,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        log: Callable[[str], None] | None = None,
    ) -> None:
        if not config.markets:
            raise OpenTapeError("capture needs at least one market")
        self.source = source
        self.config = config
        self._now = now
        self._monotonic = monotonic
        self._sleep = sleep
        self._log = log or (lambda _msg: None)
        self._tracker = BookTracker(resnapshot_every=config.resnapshot_every)
        self._deduper = TradeDeduper()
        self._buffer: list[Event] = []
        self._segment = 0
        self._stopping = False
        self._resnapshot: set[str] = set()
        self._canonical: dict[str, str] = {}
        self._polled: set[str] = set()
        self._descriptions: dict[str, MarketDescription] = {}
        self.stats = CaptureStats()

    # -- control -----------------------------------------------------------

    def stop(self) -> None:
        """Ask the loop to finish the current poll and flush."""
        self._stopping = True

    # -- main loop ---------------------------------------------------------

    def run(self) -> CaptureStats:
        """Run until the duration elapses, or until stopped, then flush."""
        with _interrupt_handler(self.stop):
            self._open_markets()
            started = self._monotonic()
            last_rotation = started
            consecutive_failures = 0

            while not self._stopping:
                # Checked before polling, not after, so "--duration 30s
                # --poll 1s" does thirty polls rather than thirty one:
                # a poll starting exactly when the window closes is a
                # poll outside the window the user asked for.
                if (
                    self.config.duration is not None
                    and self._monotonic() - started >= self.config.duration
                ):
                    break
                poll_started = self._monotonic()
                failures = self._poll_once()
                self.stats.polls += 1
                if failures:
                    self.stats.failed_polls += 1
                    consecutive_failures += 1
                    if consecutive_failures >= self.config.max_consecutive_failures:
                        self._flush()
                        raise LiveError(
                            f"every market failed to poll {consecutive_failures} times in a row; "
                            f"last error: {failures[-1]}"
                        )
                else:
                    consecutive_failures = 0

                now = self._monotonic()
                if self.config.rotate_after and now - last_rotation >= self.config.rotate_after:
                    self._flush()
                    last_rotation = now

                remaining = self.config.poll_interval - (now - poll_started)
                if self.config.duration is not None:
                    left = self.config.duration - (now - started)
                    remaining = min(remaining, left)
                if remaining > 0:
                    self._sleep(remaining)

            self._flush()
        return self.stats

    # -- steps -------------------------------------------------------------

    def _open_markets(self) -> None:
        """Resolve every market the user named, before any polling starts.

        Each id the user typed is pinned here to the id the tape will
        use. A venue can accept several spellings of the same market
        (Polymarket takes a slug or a condition id), and a tape whose
        market rows and trade rows disagree about the identifier is not
        queryable by market. The source decides the canonical spelling
        once, here, and every later row uses it.

        The market definition rows themselves are not written yet; see
        :meth:`_prepare_segment`, which writes them into each segment.
        """
        for market_id in self.config.markets:
            described = self.source.describe(market_id)
            self._canonical[market_id] = described.market_id
            self._descriptions[described.market_id] = described
            self._log(f"tracking {described.market_id} ({described.status}): {described.title}")

    def _poll_once(self) -> list[str]:
        """Poll every market once. Returns the errors, one per failed market."""
        errors: list[str] = []
        for market_id in self.config.markets:
            try:
                self._poll_market(market_id)
            except LiveError as exc:
                errors.append(str(exc))
                # The held book is now of unknown age, so do not measure
                # deltas against it once the feed comes back.
                self._resnapshot.add(market_id)
                self._log(f"poll failed for {market_id}: {exc}")
        return errors if len(errors) == len(self.config.markets) else []

    def _poll_market(self, market_id: str) -> None:
        canonical = self._canonical.get(market_id, market_id)
        quote = self.source.book(market_id)
        captured = self._now()
        events = self._tracker.update(
            canonical,
            quote.ts or captured,
            quote,
            source=self.source.source_tag,
            force_snapshot=market_id in self._resnapshot,
        )
        self._resnapshot.discard(market_id)
        for event in events:
            self._emit(event)

        # The trades endpoint answers with recent history, not with
        # what happened since the last poll, so the very first poll
        # would otherwise dump a page of trades that executed before
        # the capture began. Those are recorded only when asked for,
        # because "every row here was observed live" is the property
        # that makes a capture worth trusting, and it should not be
        # given up as a side effect of a page size.
        first_poll = market_id not in self._polled
        self._polled.add(market_id)
        ticks = [
            tick
            for tick in self.source.trades(market_id, limit=self.config.trade_limit)
            if self._deduper.is_new(f"{canonical}:{tick.trade_id}")
        ]
        if first_poll:
            ticks = ticks[-self.config.backfill :] if self.config.backfill > 0 else []

        for tick in ticks:
            self._emit(
                Trade(
                    seq=0,
                    ts=tick.ts,
                    market_id=canonical,
                    source=self.source.source_tag,
                    outcome="YES",
                    side=tick.side,
                    price=tick.price,
                    size=tick.size,
                    trade_id=tick.trade_id,
                )
            )

    def _emit(self, event: Event) -> None:
        """Buffer an observed event, in the order it was observed."""
        self._buffer.append(event)
        if isinstance(event, Trade):
            self.stats.trades += 1
        elif isinstance(event, OrderBookSnapshot):
            self.stats.snapshots += 1
        elif isinstance(event, BookDelta):
            self.stats.deltas += 1

    # -- output ------------------------------------------------------------

    def _segment_path(self) -> Path:
        out = self.config.output
        if not self.config.rotate_after:
            return out
        self._segment += 1
        return out.with_name(f"{out.stem}-{self._segment:04d}{out.suffix or '.parquet'}")

    def _prepare_segment(self, observed: list[Event]) -> list[Event]:
        """Turn a buffer of observed events into a self-contained tape.

        Two things happen here that cannot be done while polling.

        Each segment gets its own market definition and status rows.
        Emitting them once at the start of the capture would leave every
        rotated file after the first with no title for the markets in
        it, which makes a segment unreadable on its own; a rotated tape
        should be a tape.

        Those rows are then dated to the earliest event in the segment,
        rather than to the moment the daemon asked the venue what the
        market was. A tape is sorted by ``(ts, seq)``, and a venue's own
        book timestamp can be older than the local clock reading that
        follows it, so a header dated to capture time can sort after the
        book it describes. Dating the header to the start of the
        segment's own coverage is not a claim about when the market came
        into existence; it is the statement that for the whole of this
        tape, this is what the market was.

        Sequence numbers are then assigned in that final order. Each
        segment is its own tape, so its numbering starts at zero, and
        because the renumbering is a stable pass over the observed
        order, events sharing a timestamp still replay in the order they
        were seen.
        """
        body = [e for e in observed if not isinstance(e, (Market, MarketStatus))]
        earliest: dict[str, datetime] = {}
        for event in body:
            current = earliest.get(event.market_id)
            if current is None or event.ts < current:
                earliest[event.market_id] = event.ts

        headers: list[Event] = []
        for market_id, described in self._descriptions.items():
            if market_id not in earliest:
                continue
            ts = earliest[market_id]
            headers.append(
                Market(
                    seq=0,
                    ts=ts,
                    market_id=market_id,
                    source=self.source.source_tag,
                    title=described.title,
                    outcomes=described.outcomes,
                )
            )
            headers.append(
                MarketStatus(
                    seq=0,
                    ts=ts,
                    market_id=market_id,
                    source=self.source.source_tag,
                    status=described.status,
                )
            )
        return [replace(event, seq=i) for i, event in enumerate(headers + body)]

    def _flush(self) -> None:
        """Write the buffered events to a tape file and clear the buffer.

        A rotation with nothing observed writes no file: an empty tape
        is a valid tape but a directory of them says nothing, and a
        quiet market should not manufacture files.
        """
        events = self._prepare_segment(self._buffer)
        self._buffer = []
        if not events:
            return
        path = self._segment_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        Tape.from_events(events).write(path)
        self.stats.files.append(path)
        self.stats.events += len(events)
        self._log(f"wrote {path}: {len(events):,} events")


class _interrupt_handler:
    """Turn the first Ctrl-C into a graceful stop instead of a lost capture.

    A second Ctrl-C is left to the default handler, so a wedged run can
    still be killed. Installing a handler only works on the main thread,
    so a failure to install is not an error: the capture just ends the
    ordinary way.
    """

    def __init__(self, stop: Callable[[], None]) -> None:
        self._stop = stop
        self._previous: object = None
        self._installed = False

    def __enter__(self) -> _interrupt_handler:
        def handler(signum: int, frame: object) -> None:
            signal.signal(signal.SIGINT, signal.default_int_handler)
            self._stop()

        try:
            self._previous = signal.signal(signal.SIGINT, handler)
            self._installed = True
        except (ValueError, OSError):
            self._installed = False
        return self

    def __exit__(self, *exc: object) -> None:
        if self._installed:
            with contextlib.suppress(ValueError, OSError):
                signal.signal(signal.SIGINT, self._previous)  # type: ignore[arg-type]
