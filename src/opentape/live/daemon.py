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
- A market's lifecycle status is re-read on an interval, not once. A
  market that closes or halts while the capture is running gets a
  ``market_status`` row at the point the change was observed, and a
  segment written after that point opens with the new status rather
  than the one the capture started with.
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
from opentape.live.stream import (
    BookMirror,
    StreamBook,
    StreamLevel,
    StreamSource,
    StreamTrade,
)
from opentape.live.ws import Connector, WebSocketConnection
from opentape.live.ws import connect as ws_connect
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
    #: Seconds between lifecycle re-reads, or None to ask only once at
    #: the start. This is separate from ``poll_interval`` because it
    #: costs an extra request per market and a market's status changes
    #: on a different timescale than its book.
    status_every: float | None = 30.0


@dataclass(slots=True)
class TapeStats:
    """The part of a capture's result that does not depend on transport."""

    events: int = 0
    trades: int = 0
    snapshots: int = 0
    deltas: int = 0
    files: list[Path] = field(default_factory=list)
    #: Lifecycle transitions observed while capturing, excluding the
    #: opening status every tape carries anyway.
    status_changes: int = 0
    #: Lifecycle re-reads that raised. These are counted rather than
    #: fatal; see :meth:`_TapeCapture._recheck_status`.
    status_check_failures: int = 0


@dataclass(slots=True)
class CaptureStats(TapeStats):
    """What a finished polling capture did."""

    polls: int = 0
    failed_polls: int = 0


class _TapeCapture:
    """Everything a capture does with events once it has them.

    Buffering, segment headers, rotation, sequence numbering, and
    writing are identical whether the events came from polling REST or
    from a venue's websocket, so they live here and each transport's
    daemon subclasses this with its own loop. Splitting it this way is
    what keeps a streamed tape and a polled tape the same kind of file.
    """

    def __init__(
        self,
        *,
        output: Path,
        rotate_after: float | None,
        source_tag: str,
        stats: TapeStats,
        log: Callable[[str], None] | None = None,
    ) -> None:
        self._output = output
        self._rotate_after = rotate_after
        self._source_tag = source_tag
        self._log = log or (lambda _msg: None)
        self._buffer: list[Event] = []
        self._segment = 0
        self._stopping = False
        self._descriptions: dict[str, MarketDescription] = {}
        #: The status each market had when the CURRENT segment began,
        #: which is what that segment's header row must state. It lags
        #: :attr:`_descriptions` by one flush on purpose; see
        #: :meth:`_prepare_segment`.
        self._segment_status: dict[str, str] = {}
        #: Canonical market id back to the spelling the user typed,
        #: which is the one a later ``describe()`` is given. A venue can
        #: accept several spellings and only the user's is known to
        #: work, since it is the one that resolved at the start.
        self._requested: dict[str, str] = {}
        self.stats = stats

    def stop(self) -> None:
        """Ask the loop to finish what it is doing and flush."""
        self._stopping = True

    # -- lifecycle ---------------------------------------------------------

    def _track(self, described: MarketDescription, requested: str) -> None:
        """Record a market's opening description, before anything is captured."""
        self._descriptions[described.market_id] = described
        self._segment_status[described.market_id] = described.status
        self._requested[described.market_id] = requested
        self._log(f"tracking {described.market_id} ({described.status}): {described.title}")

    def _observe_status(self, market_id: str, status: str, ts: datetime) -> bool:
        """Record a lifecycle status, emitting a row only if it changed.

        The tape already opens with a status row per segment, so
        re-emitting an unchanged status on every check would fill a
        long capture with rows that say nothing. Only a transition is
        an event.
        """
        held = self._descriptions.get(market_id)
        if held is None or held.status == status:
            return False
        self._descriptions[market_id] = replace(held, status=status)
        self._emit(
            MarketStatus(
                seq=0,
                ts=ts,
                market_id=market_id,
                source=self._source_tag,
                status=status,
            )
        )
        self.stats.status_changes += 1
        self._log(f"{market_id} changed status: {held.status} -> {status}")
        return True

    def _recheck_status(self, describe: Callable[[str], MarketDescription], ts: datetime) -> None:
        """Re-read every tracked market's status and record any change.

        A failed re-read is counted and logged, never fatal, and never
        touches the held status. The lifecycle check is a secondary
        reader of a different endpoint than the book, so letting it end
        a capture would mean losing book and trade data that was
        arriving perfectly well. An unknown status is also not a
        change: writing one down because a request timed out would put
        a claim on the tape that nothing observed.
        """
        for canonical in list(self._descriptions):
            try:
                described = describe(self._requested.get(canonical, canonical))
            except LiveError as exc:
                self.stats.status_check_failures += 1
                self._log(f"status check failed for {canonical}: {exc}")
                continue
            # Recorded against the id the tape already uses, not against
            # whatever this call resolved to. The canonical spelling was
            # decided once at the start; a tape whose rows disagree
            # about the identifier is not queryable by market.
            self._observe_status(canonical, described.status, ts)

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
        out = self._output
        if not self._rotate_after:
            return out
        self._segment += 1
        return out.with_name(f"{out.stem}-{self._segment:04d}{out.suffix or '.parquet'}")

    def _prepare_segment(self, observed: list[Event]) -> list[Event]:
        """Turn a buffer of observed events into a self-contained tape.

        Two things happen here that cannot be done while capturing.

        Each segment gets its own market definition and status rows.
        Emitting them once at the start of the capture would leave every
        rotated file after the first with no title for the markets in
        it, which makes a segment unreadable on its own; a rotated tape
        should be a tape.

        The status a header states is the one the market had when this
        segment's coverage BEGAN, not the one it has now. A segment that
        opened while the market was trading and saw it close carries
        ``open`` in its header and the observed ``closed`` row in its
        body, in that order, which is what a reader replaying that
        segment forward should see. The next segment then opens with
        ``closed``. Status rows observed during the capture stay in the
        body for exactly this reason: they are events, not headers.

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
        body = [e for e in observed if not isinstance(e, Market)]
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
                    source=self._source_tag,
                    title=described.title,
                    outcomes=described.outcomes,
                )
            )
            headers.append(
                MarketStatus(
                    seq=0,
                    ts=ts,
                    market_id=market_id,
                    source=self._source_tag,
                    status=self._segment_status.get(market_id, described.status),
                )
            )
        # Whatever the markets are now is what the NEXT segment opens
        # with, so this is advanced once the segment it describes has
        # been built and never while events are still being buffered.
        for market_id, described in self._descriptions.items():
            self._segment_status[market_id] = described.status
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


class CaptureDaemon(_TapeCapture):
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
        super().__init__(
            output=config.output,
            rotate_after=config.rotate_after,
            source_tag=source.source_tag,
            stats=CaptureStats(),
            log=log,
        )
        self.source = source
        self.config = config
        self._now = now
        self._monotonic = monotonic
        self._sleep = sleep
        self._tracker = BookTracker(resnapshot_every=config.resnapshot_every)
        self._deduper = TradeDeduper()
        self._resnapshot: set[str] = set()
        self._canonical: dict[str, str] = {}
        self._polled: set[str] = set()
        #: Narrowed from the base class's :class:`TapeStats` for readers.
        self.stats: CaptureStats = CaptureStats()

    # -- main loop ---------------------------------------------------------

    def run(self) -> CaptureStats:
        """Run until the duration elapses, or until stopped, then flush."""
        with _interrupt_handler(self.stop):
            self._open_markets()
            started = self._monotonic()
            last_rotation = started
            last_status_check = started
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
                # Before rotation, so a status change observed in this
                # pass lands in the segment whose coverage contains it
                # rather than opening the next one.
                if (
                    self.config.status_every is not None
                    and now - last_status_check >= self.config.status_every
                ):
                    self._recheck_status(self.source.describe, self._now())
                    last_status_check = now

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
            self._track(described, market_id)

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


@dataclass(frozen=True, slots=True)
class StreamConfig:
    """Everything the streaming daemon needs that is not the venue."""

    markets: tuple[str, ...]
    output: Path
    duration: float | None = None
    rotate_after: float | None = None
    #: How long a single read waits before the loop does its periodic
    #: work. This is not a timeout in the failure sense: a quiet market
    #: publishes nothing for minutes and that is not an error. It only
    #: bounds how long a rotation or a stop can be delayed by silence.
    poll_wait: float = 1.0
    #: How many times a dropped connection is re-established before the
    #: capture gives up.
    max_reconnects: int = 5
    reconnect_backoff: float = 1.0
    #: Seconds between lifecycle re-reads, or None to ask only once at
    #: the start. A change stream carries book and trade messages, not
    #: the market's lifecycle, so this stays a REST question even on a
    #: streamed capture.
    status_every: float | None = 30.0


@dataclass(slots=True)
class StreamStats(TapeStats):
    """What a finished streaming capture did."""

    messages: int = 0
    reconnects: int = 0
    #: Venue snapshots that were compared against the mirrored book.
    checks: int = 0
    #: Comparisons where the mirrored book did not match the venue's.
    divergences: int = 0
    #: Level changes discarded because no snapshot had arrived yet for
    #: that market, so there was no book to apply them to.
    dropped_updates: int = 0


class StreamDaemon(_TapeCapture):
    """Subscribe to a :class:`StreamSource` and write tapes.

    The loop is smaller than the polling one because the venue does the
    work the poller was doing: there is no interval, no diffing of
    consecutive books, and no backfill question, since every event on a
    stream was published while the capture was connected.

    What replaces them is connection management, and its rule is the
    streaming version of "a failed poll is a gap". A dropped connection
    means an unknown number of changes were missed, so every mirrored
    book is discarded on reconnect and nothing is written for a market
    until the venue sends a fresh snapshot. Emitting deltas across that
    hole would produce a book that never existed.
    """

    def __init__(
        self,
        source: StreamSource,
        config: StreamConfig,
        *,
        connect: Connector = ws_connect,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        log: Callable[[str], None] | None = None,
    ) -> None:
        if not config.markets:
            raise OpenTapeError("capture needs at least one market")
        super().__init__(
            output=config.output,
            rotate_after=config.rotate_after,
            source_tag=source.source_tag,
            stats=StreamStats(),
            log=log,
        )
        self.source = source
        self.config = config
        self._connect = connect
        self._now = now
        self._monotonic = monotonic
        self._sleep = sleep
        self._last_status_check = monotonic()
        self._mirror = BookMirror()
        self._deduper = TradeDeduper()
        self._canonical: dict[str, str] = {}
        #: Narrowed from the base class's :class:`TapeStats` for readers.
        self.stats: StreamStats = StreamStats()

    # -- main loop ---------------------------------------------------------

    def run(self) -> StreamStats:
        """Stream until the duration elapses, or until stopped, then flush."""
        with _interrupt_handler(self.stop):
            self._open_markets()
            started = self._monotonic()
            last_rotation = started
            self._last_status_check = started
            attempts = 0

            while not self._stopping and not self._expired(started):
                try:
                    connection = self._open_connection()
                except LiveError as exc:
                    attempts += 1
                    if attempts > self.config.max_reconnects:
                        self._flush()
                        raise LiveError(
                            f"could not stay connected to {self.source.stream_url()} after "
                            f"{attempts} attempt(s); last error: {exc}"
                        ) from exc
                    self.stats.reconnects += 1
                    self._sleep(self.config.reconnect_backoff * attempts)
                    continue

                with connection:
                    attempts = 0
                    last_rotation = self._consume(connection, started, last_rotation)
                if not self._stopping and not self._expired(started):
                    # The server closed. Everything mirrored is now of
                    # unknown age, so it is dropped rather than carried
                    # across the gap.
                    self._reset_books("the connection dropped")
                    attempts += 1
                    self.stats.reconnects += 1
                    if attempts > self.config.max_reconnects:
                        self._flush()
                        raise LiveError(
                            f"connection to {self.source.stream_url()} dropped "
                            f"{attempts} time(s) in a row"
                        )
                    self._sleep(self.config.reconnect_backoff * attempts)

            self._flush()
        return self.stats

    def _expired(self, started: float) -> bool:
        return (
            self.config.duration is not None and self._monotonic() - started >= self.config.duration
        )

    def _open_markets(self) -> None:
        """Resolve every market before subscribing, as the poller does."""
        for market_id in self.config.markets:
            described = self.source.describe(market_id)
            self._canonical[market_id] = described.market_id
            self._track(described, market_id)

    def _open_connection(self) -> WebSocketConnection:
        url = self.source.stream_url()
        connection = self._connect(url, timeout=20.0)
        connection.send_text(
            self.source.subscribe_message([self._canonical[m] for m in self.config.markets])
        )
        self._log(f"subscribed to {len(self.config.markets)} market(s) on {url}")
        return connection

    def _consume(
        self, connection: WebSocketConnection, started: float, last_rotation: float
    ) -> float:
        """Read messages until the connection ends or the run should stop."""
        while not self._stopping and not self._expired(started):
            try:
                message = connection.recv_text(timeout=self.config.poll_wait)
            except LiveError as exc:
                self._log(f"stream ended: {exc}")
                return last_rotation
            if message is not None:
                self.stats.messages += 1
                self._handle(message)
            now = self._monotonic()
            # The read above returns after at most ``poll_wait``
            # whether or not a message arrived, which is what makes a
            # lifecycle check possible on a market so quiet that the
            # stream says nothing. A market that halts is exactly that
            # kind of market.
            if (
                self.config.status_every is not None
                and now - self._last_status_check >= self.config.status_every
            ):
                self._recheck_status(self.source.describe, self._now())
                self._last_status_check = now
            if self.config.rotate_after and now - last_rotation >= self.config.rotate_after:
                self._flush()
                last_rotation = now
        return last_rotation

    def _reset_books(self, why: str) -> None:
        for market_id in self._descriptions:
            self._mirror.drop(market_id)
        self._log(f"dropped every mirrored book because {why}")

    # -- updates -----------------------------------------------------------

    def _handle(self, message: str) -> None:
        """Turn one venue message into tape events."""
        for update in self.source.parse(message):
            if isinstance(update, StreamBook):
                events, check = self._mirror.snapshot(update, source=self._source_tag)
                if check is not None:
                    self.stats.checks += 1
                    if not check.agreed:
                        self.stats.divergences += 1
                        self._log(check.summary())
                for event in events:
                    self._emit(event)
            elif isinstance(update, StreamLevel):
                if not self._mirror.has(update.market_id):
                    self.stats.dropped_updates += 1
                    continue
                for event in self._mirror.level(update, source=self._source_tag):
                    self._emit(event)
            elif isinstance(update, StreamTrade):
                tick = update.tick
                if not self._deduper.is_new(f"{update.market_id}:{tick.trade_id}"):
                    continue
                self._emit(
                    Trade(
                        seq=0,
                        ts=tick.ts,
                        market_id=update.market_id,
                        source=self._source_tag,
                        outcome="YES",
                        side=tick.side,
                        price=tick.price,
                        size=tick.size,
                        trade_id=tick.trade_id,
                    )
                )


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
