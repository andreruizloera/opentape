"""Tests for the capture daemon and the state it keeps.

The source is a scripted fake and the clock is injected, so a capture
that "runs for an hour" finishes instantly and nothing sleeps.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import ClassVar

import pytest

from opentape.errors import LiveError, OpenTapeError
from opentape.events import BookDelta, BookLevel, Market, MarketStatus, OrderBookSnapshot, Trade
from opentape.live.base import (
    BookQuote,
    BookTracker,
    LiveSource,
    MarketDescription,
    MarketRef,
    TradeDeduper,
    TradeTick,
)
from opentape.live.daemon import CaptureConfig, CaptureDaemon, parse_duration
from opentape.tape import Tape

START = datetime(2026, 3, 2, 14, 30, tzinfo=UTC)


def quote(bids: dict[float, float], asks: dict[float, float]) -> BookQuote:
    return BookQuote(
        bids=tuple(BookLevel(p, s) for p, s in sorted(bids.items(), reverse=True)),
        asks=tuple(BookLevel(p, s) for p, s in sorted(asks.items())),
    )


# -- BookTracker ----------------------------------------------------------


def test_the_first_sight_of_a_market_is_a_snapshot() -> None:
    tracker = BookTracker()
    events = tracker.update("M", START, quote({0.6: 10}, {0.62: 20}), source="s")
    assert len(events) == 1
    snap = events[0]
    assert isinstance(snap, OrderBookSnapshot)
    assert [(lv.price, lv.size) for lv in snap.bids] == [(0.6, 10.0)]
    assert snap.outcome == "YES"


def test_an_unchanged_book_produces_nothing() -> None:
    tracker = BookTracker()
    tracker.update("M", START, quote({0.6: 10}, {}), source="s")
    assert tracker.update("M", START, quote({0.6: 10}, {}), source="s") == []


def test_only_the_levels_that_moved_become_deltas() -> None:
    tracker = BookTracker()
    tracker.update("M", START, quote({0.6: 10, 0.59: 5}, {}), source="s")
    events = tracker.update("M", START, quote({0.6: 25, 0.59: 5}, {}), source="s")
    assert len(events) == 1
    delta = events[0]
    assert isinstance(delta, BookDelta)
    assert (delta.side, delta.price, delta.size) == ("bid", 0.6, 25.0)


def test_a_level_that_vanished_becomes_a_delta_of_size_zero() -> None:
    # That is what the canonical schema means by a removal.
    tracker = BookTracker()
    tracker.update("M", START, quote({0.6: 10}, {}), source="s")
    events = tracker.update("M", START, quote({}, {}), source="s")
    assert [(e.side, e.price, e.size) for e in events] == [("bid", 0.6, 0.0)]  # type: ignore[union-attr]


def test_bids_and_asks_are_tracked_separately() -> None:
    tracker = BookTracker()
    tracker.update("M", START, quote({0.6: 10}, {0.6: 10}), source="s")
    events = tracker.update("M", START, quote({0.6: 10}, {0.6: 99}), source="s")
    assert [(e.side, e.size) for e in events] == [("ask", 99.0)]  # type: ignore[union-attr]


def test_markets_do_not_share_a_book() -> None:
    tracker = BookTracker()
    tracker.update("A", START, quote({0.6: 10}, {}), source="s")
    events = tracker.update("B", START, quote({0.3: 1}, {}), source="s")
    assert isinstance(events[0], OrderBookSnapshot)


def test_a_forced_snapshot_replaces_deltas_after_a_gap() -> None:
    # After a failed poll the held book is of unknown age, so measuring
    # deltas against it would assert changes nobody observed.
    tracker = BookTracker()
    tracker.update("M", START, quote({0.6: 10}, {}), source="s")
    events = tracker.update("M", START, quote({0.6: 11}, {}), source="s", force_snapshot=True)
    assert len(events) == 1 and isinstance(events[0], OrderBookSnapshot)


def test_periodic_resnapshotting_gives_a_tape_recovery_points() -> None:
    tracker = BookTracker(resnapshot_every=3)
    kinds = []
    for i in range(7):
        events = tracker.update("M", START, quote({0.6: float(i)}, {}), source="s")
        kinds.append(type(events[0]).__name__ if events else "none")
    assert kinds == [
        "OrderBookSnapshot",
        "BookDelta",
        "BookDelta",
        "OrderBookSnapshot",
        "BookDelta",
        "BookDelta",
        "OrderBookSnapshot",
    ]


def test_complementary_prices_do_not_split_a_level_through_float_noise() -> None:
    # 1 - 0.07 is not exactly 0.93 in binary floating point, and an
    # unrounded key would make every poll look like a book change.
    tracker = BookTracker()
    asks = (BookLevel(round(1.0 - 0.07, 6), 5.0),)
    first = BookQuote(bids=(), asks=asks)
    tracker.update("M", START, first, source="s")
    assert tracker.update("M", START, BookQuote(bids=(), asks=asks), source="s") == []


def test_dropping_a_market_forces_the_next_update_to_resnapshot() -> None:
    tracker = BookTracker()
    tracker.update("M", START, quote({0.6: 10}, {}), source="s")
    tracker.drop("M")
    events = tracker.update("M", START, quote({0.6: 10}, {}), source="s")
    assert isinstance(events[0], OrderBookSnapshot)


# -- TradeDeduper ---------------------------------------------------------


def test_a_trade_id_is_new_exactly_once() -> None:
    deduper = TradeDeduper()
    assert deduper.is_new("t1") is True
    assert deduper.is_new("t1") is False


def test_the_deduper_is_bounded_and_evicts_the_oldest_first() -> None:
    deduper = TradeDeduper(capacity=3)
    for i in range(5):
        deduper.is_new(f"t{i}")
    assert len(deduper) == 3
    assert deduper.is_new("t0") is True  # evicted, so it looks new again
    assert deduper.is_new("t4") is False  # still remembered


# -- parse_duration -------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("30s", 30.0), ("5m", 300.0), ("1h", 3600.0), ("500ms", 0.5), ("45", 45.0), ("1.5m", 90.0)],
)
def test_durations_parse(raw: str, expected: float) -> None:
    assert parse_duration(raw, flag="--poll") == expected


@pytest.mark.parametrize("raw", ["", "soon", "5 years", "-3s", "0s", "0"])
def test_a_bad_duration_names_the_flag(raw: str) -> None:
    with pytest.raises(OpenTapeError, match="--poll"):
        parse_duration(raw, flag="--poll")


# -- the daemon -----------------------------------------------------------


class FakeSource(LiveSource):
    """A source whose answers are scripted per poll."""

    key: ClassVar[str] = "fake"
    source_tag: ClassVar[str] = "fake-rest-poll"

    def __init__(
        self,
        books: list[BookQuote | Exception],
        trades: list[list[TradeTick]] | None = None,
        *,
        market_id: str = "M1",
        status: str = "open",
    ) -> None:
        self.books = books
        self.trade_pages = trades or []
        self.market_id = market_id
        self.status = status
        self.book_calls = 0
        self.trade_calls = 0

    def list_markets(self, *, limit: int, search: str | None = None) -> list[MarketRef]:
        return [MarketRef(market_id=self.market_id, title="Fake")]

    def describe(self, market_id: str) -> MarketDescription:
        return MarketDescription(
            market_id=self.market_id, title="A fake market", status=self.status
        )

    def book(self, market_id: str) -> BookQuote:
        result = self.books[min(self.book_calls, len(self.books) - 1)]
        self.book_calls += 1
        if isinstance(result, Exception):
            raise result
        return result

    def trades(self, market_id: str, *, limit: int = 100) -> list[TradeTick]:
        index = self.trade_calls
        self.trade_calls += 1
        if index < len(self.trade_pages):
            return self.trade_pages[index]
        return self.trade_pages[-1] if self.trade_pages else []


class Clock:
    """A monotonic clock that only moves when something sleeps."""

    def __init__(self) -> None:
        self.t = 0.0
        self.slept: list[float] = []

    def monotonic(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.t += seconds

    def now(self) -> datetime:
        return START + timedelta(seconds=self.t)


def run(source: FakeSource, tmp_path: Path, **config: object) -> tuple[CaptureDaemon, list[str]]:
    logs: list[str] = []
    clock = Clock()
    settings = {
        "markets": ("M1",),
        "output": tmp_path / "out.parquet",
        "poll_interval": 1.0,
        "duration": 3.0,
    }
    settings.update(config)
    daemon = CaptureDaemon(
        source,
        CaptureConfig(**settings),  # type: ignore[arg-type]
        now=clock.now,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        log=logs.append,
    )
    daemon.run()
    return daemon, logs


def test_a_capture_opens_with_the_market_and_its_status(tmp_path: Path) -> None:
    source = FakeSource([quote({0.6: 10}, {0.62: 5})])
    daemon, _ = run(source, tmp_path)
    tape = Tape.read(daemon.stats.files[0])
    events = list(tape.replay(speed="max"))
    assert isinstance(events[0], Market)
    assert isinstance(events[1], MarketStatus)
    assert events[1].status == "open"  # type: ignore[union-attr]


def test_a_capture_writes_a_readable_tape_tagged_with_the_transport(tmp_path: Path) -> None:
    source = FakeSource([quote({0.6: 10}, {})])
    daemon, _ = run(source, tmp_path)
    tape = Tape.read(daemon.stats.files[0])
    assert tape.summary().sources == ("fake-rest-poll",)


def test_sequence_numbers_are_unique_and_ordered_by_capture(tmp_path: Path) -> None:
    source = FakeSource([quote({0.6: float(i)}, {}) for i in range(1, 5)])
    daemon, _ = run(source, tmp_path)
    seqs = Tape.read(daemon.stats.files[0]).frame.get_column("seq").to_list()
    assert seqs == sorted(seqs)
    assert len(set(seqs)) == len(seqs)


def test_the_poll_interval_paces_the_loop(tmp_path: Path) -> None:
    source = FakeSource([quote({0.6: 1}, {})])
    daemon, _ = run(source, tmp_path, poll_interval=0.5, duration=2.0)
    assert daemon.stats.polls == 4


def test_a_capture_stops_at_its_duration(tmp_path: Path) -> None:
    source = FakeSource([quote({0.6: 1}, {})])
    daemon, _ = run(source, tmp_path, poll_interval=1.0, duration=3.0)
    assert daemon.stats.polls == 3


def test_stop_ends_the_loop_after_the_current_poll(tmp_path: Path) -> None:
    source = FakeSource([quote({0.6: 1}, {})])
    clock = Clock()
    daemon = CaptureDaemon(
        source,
        CaptureConfig(markets=("M1",), output=tmp_path / "o.parquet", duration=None),
        now=clock.now,
        monotonic=clock.monotonic,
        sleep=lambda s: (clock.sleep(s), daemon.stop())[0],
    )
    daemon.run()
    assert daemon.stats.polls == 1
    assert daemon.stats.files


def test_a_quiet_market_still_produces_one_snapshot_not_one_per_poll(tmp_path: Path) -> None:
    source = FakeSource([quote({0.6: 10}, {0.62: 5})])
    daemon, _ = run(source, tmp_path, poll_interval=1.0, duration=5.0)
    assert daemon.stats.polls == 5
    assert daemon.stats.snapshots == 1
    assert daemon.stats.deltas == 0


# -- trades ---------------------------------------------------------------


def tick(trade_id: str, second: float, price: float = 0.6) -> TradeTick:
    return TradeTick(
        ts=START + timedelta(seconds=second), price=price, size=1.0, side="buy", trade_id=trade_id
    )


def test_the_first_poll_does_not_backfill_history_by_default(tmp_path: Path) -> None:
    # The trades endpoint answers with recent history, not with what
    # happened since the last poll, so without this the tape would
    # silently contain trades from before the capture began.
    source = FakeSource([quote({}, {})], [[tick("old1", -60), tick("old2", -30)], []])
    daemon, _ = run(source, tmp_path)
    assert daemon.stats.trades == 0


def test_backfill_keeps_the_most_recent_history_when_asked(tmp_path: Path) -> None:
    source = FakeSource(
        [quote({}, {})], [[tick("old1", -60), tick("old2", -30), tick("old3", -10)], []]
    )
    daemon, _ = run(source, tmp_path, backfill=2)
    assert daemon.stats.trades == 2
    tape = Tape.read(daemon.stats.files[0])
    kept = set(tape.sql("SELECT trade_id FROM trades").get_column("trade_id").to_list())
    assert kept == {"old2", "old3"}


def test_a_trade_seen_on_two_polls_is_written_once(tmp_path: Path) -> None:
    repeated = [tick("t1", 1), tick("t2", 2)]
    source = FakeSource([quote({}, {})], [[], repeated, repeated])
    daemon, _ = run(source, tmp_path, poll_interval=1.0, duration=3.0)
    assert daemon.stats.trades == 2


def test_a_new_trade_is_written_when_it_first_appears(tmp_path: Path) -> None:
    source = FakeSource([quote({}, {})], [[], [tick("t1", 1)], [tick("t1", 1), tick("t2", 2)]])
    daemon, _ = run(source, tmp_path, poll_interval=1.0, duration=3.0)
    assert daemon.stats.trades == 2
    tape = Tape.read(daemon.stats.files[0])
    trades = [e for e in tape.replay(speed="max") if isinstance(e, Trade)]
    assert [t.trade_id for t in trades] == ["t1", "t2"]


# -- failures and gaps ----------------------------------------------------


def test_a_failed_poll_is_counted_and_does_not_end_the_capture(tmp_path: Path) -> None:
    source = FakeSource(
        [quote({0.6: 10}, {}), LiveError("venue hiccup"), quote({0.6: 10}, {})],
    )
    daemon, logs = run(source, tmp_path, poll_interval=1.0, duration=3.0)
    assert daemon.stats.failed_polls == 1
    assert daemon.stats.polls == 3
    assert any("venue hiccup" in line for line in logs)


def test_the_poll_after_a_failure_resnapshots_instead_of_guessing(tmp_path: Path) -> None:
    # The book we hold is of unknown age after a gap, so a delta
    # measured against it would claim a change nobody saw.
    source = FakeSource(
        [quote({0.6: 10}, {}), LiveError("dropped"), quote({0.6: 11}, {})],
    )
    daemon, _ = run(source, tmp_path, poll_interval=1.0, duration=3.0)
    assert daemon.stats.snapshots == 2
    assert daemon.stats.deltas == 0


def test_a_feed_that_never_comes_back_aborts_rather_than_running_all_night(
    tmp_path: Path,
) -> None:
    source = FakeSource([LiveError("gone")])
    with pytest.raises(LiveError, match="times in a row"):
        run(source, tmp_path, poll_interval=1.0, duration=100.0, max_consecutive_failures=3)


def test_an_aborting_capture_still_writes_what_it_had(tmp_path: Path) -> None:
    source = FakeSource([quote({0.6: 10}, {}), LiveError("gone")])
    with pytest.raises(LiveError):
        run(source, tmp_path, poll_interval=1.0, duration=100.0, max_consecutive_failures=2)
    assert (tmp_path / "out.parquet").exists()
    assert len(Tape.read(tmp_path / "out.parquet")) > 0


# -- output ---------------------------------------------------------------


def test_rotation_writes_numbered_segments_that_are_each_valid(tmp_path: Path) -> None:
    source = FakeSource([quote({0.6: float(i)}, {}) for i in range(1, 12)])
    daemon, _ = run(source, tmp_path, poll_interval=1.0, duration=6.0, rotate_after=2.0)
    assert len(daemon.stats.files) >= 3
    assert [p.name for p in daemon.stats.files[:3]] == [
        "out-0001.parquet",
        "out-0002.parquet",
        "out-0003.parquet",
    ]
    for path in daemon.stats.files:
        assert len(Tape.read(path)) > 0


def test_without_rotation_one_file_is_written(tmp_path: Path) -> None:
    source = FakeSource([quote({0.6: 1}, {})])
    daemon, _ = run(source, tmp_path)
    assert [p.name for p in daemon.stats.files] == ["out.parquet"]


def test_a_rotation_with_nothing_buffered_writes_no_file(tmp_path: Path) -> None:
    # A quiet market should not manufacture a directory of empty tapes.
    source = FakeSource([quote({0.6: 10}, {})])
    daemon, _ = run(source, tmp_path, poll_interval=1.0, duration=6.0, rotate_after=1.0)
    assert len(daemon.stats.files) == 1


def test_the_output_directory_is_created(tmp_path: Path) -> None:
    source = FakeSource([quote({0.6: 1}, {})])
    daemon, _ = run(source, tmp_path, output=tmp_path / "nested" / "deep" / "out.parquet")
    assert daemon.stats.files[0].exists()


def test_every_row_uses_the_id_the_source_considers_canonical(tmp_path: Path) -> None:
    # Polymarket accepts a slug or a condition id for the same market; a
    # tape whose market row and trade rows disagree is not queryable.
    source = FakeSource([quote({0.6: 10}, {})], [[], [tick("t1", 1)]], market_id="the-slug")
    daemon, _ = run(source, tmp_path, markets=("0xdeadbeef",), poll_interval=1.0, duration=2.0)
    ids = set(Tape.read(daemon.stats.files[0]).market_ids())
    assert ids == {"the-slug"}


def test_a_capture_with_no_markets_is_refused(tmp_path: Path) -> None:
    with pytest.raises(OpenTapeError, match="at least one market"):
        CaptureDaemon(FakeSource([]), CaptureConfig(markets=(), output=tmp_path / "o.parquet"))


# -- segments are self-contained tapes ------------------------------------


def test_the_market_row_replays_before_the_book_it_describes(tmp_path: Path) -> None:
    # A venue's own book timestamp can be older than the local clock
    # reading that follows it, so a header dated to capture time would
    # sort after the snapshot it is supposed to introduce.
    stale = BookQuote(bids=(BookLevel(0.6, 10.0),), asks=(), ts=START - timedelta(minutes=5))
    source = FakeSource([stale])
    daemon, _ = run(source, tmp_path)
    events = list(Tape.read(daemon.stats.files[0]).replay(speed="max"))
    assert isinstance(events[0], Market)
    assert isinstance(events[1], MarketStatus)
    assert events[0].ts == stale.ts


def test_every_rotated_segment_carries_its_own_market_rows(tmp_path: Path) -> None:
    # Otherwise every file after the first has no title for its markets,
    # and a rotated segment cannot be read on its own.
    source = FakeSource([quote({0.6: float(i)}, {}) for i in range(1, 12)])
    daemon, _ = run(source, tmp_path, poll_interval=1.0, duration=6.0, rotate_after=2.0)
    assert len(daemon.stats.files) >= 3
    for path in daemon.stats.files:
        events = list(Tape.read(path).replay(speed="max"))
        assert isinstance(events[0], Market)
        assert events[0].title == "A fake market"  # type: ignore[union-attr]


def test_each_segment_numbers_its_own_events_from_zero(tmp_path: Path) -> None:
    source = FakeSource([quote({0.6: float(i)}, {}) for i in range(1, 12)])
    daemon, _ = run(source, tmp_path, poll_interval=1.0, duration=6.0, rotate_after=2.0)
    for path in daemon.stats.files:
        seqs = Tape.read(path).frame.get_column("seq").to_list()
        assert seqs == list(range(len(seqs)))


def test_a_capture_that_observed_nothing_writes_no_tape(tmp_path: Path) -> None:
    # Resolving the market is not the same as capturing it, so a run
    # whose every poll failed should not leave a header-only file
    # looking like a capture that worked.
    source = FakeSource([LiveError("gone")])
    with pytest.raises(LiveError):
        run(source, tmp_path, poll_interval=1.0, duration=100.0, max_consecutive_failures=2)
    assert not (tmp_path / "out.parquet").exists()
