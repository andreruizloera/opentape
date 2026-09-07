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
from opentape.events import (
    BookDelta,
    BookLevel,
    Market,
    MarketStatus,
    OrderBookSnapshot,
    Resolution,
    Trade,
)
from opentape.live.base import (
    BookQuote,
    BookTracker,
    LiveSource,
    MarketDescription,
    MarketRef,
    MarketResolution,
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
        statuses: list[str | Exception] | None = None,
        resolutions: list[MarketResolution | None] | None = None,
    ) -> None:
        self.books = books
        self.trade_pages = trades or []
        self.market_id = market_id
        self.status = status
        #: One answer per describe() call, the last one repeating. None
        #: means every call answers ``status``.
        self.statuses = statuses
        #: One settlement per describe() call, the last one repeating,
        #: on the same rule as ``statuses``.
        self.resolutions = resolutions
        self.book_calls = 0
        self.trade_calls = 0
        self.describe_calls = 0
        self.described: list[str] = []

    def list_markets(self, *, limit: int, search: str | None = None) -> list[MarketRef]:
        return [MarketRef(market_id=self.market_id, title="Fake")]

    def describe(self, market_id: str) -> MarketDescription:
        index = self.describe_calls
        self.describe_calls += 1
        self.described.append(market_id)
        status = self.status
        if self.statuses:
            answer = self.statuses[min(index, len(self.statuses) - 1)]
            if isinstance(answer, Exception):
                raise answer
            status = answer
        resolution = None
        if self.resolutions:
            resolution = self.resolutions[min(index, len(self.resolutions) - 1)]
        return MarketDescription(
            market_id=self.market_id,
            title="A fake market",
            status=status,
            resolution=resolution,
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


# -- lifecycle status while capturing -------------------------------------


def _statuses(path: Path) -> list[tuple[int, str]]:
    """Every market_status row on a tape, in replay order."""
    return [
        (e.seq, e.status)
        for e in Tape.read(path).replay(speed="max")
        if isinstance(e, MarketStatus)
    ]


def test_a_market_that_closes_mid_capture_gets_a_status_row_when_it_happens(
    tmp_path: Path,
) -> None:
    # The tape used to carry only the status the capture opened with,
    # so a market that closed halfway through read as open forever.
    source = FakeSource(
        [quote({0.6: float(i)}, {}) for i in range(1, 8)],
        statuses=["open", "open", "closed"],
    )
    daemon, logs = run(source, tmp_path, poll_interval=1.0, duration=6.0, status_every=2.0)

    assert daemon.stats.status_changes == 1
    assert _statuses(daemon.stats.files[0]) == [(1, "open"), (7, "closed")]
    assert "M1 changed status: open -> closed" in logs


def test_the_closing_row_carries_the_time_the_change_was_observed(tmp_path: Path) -> None:
    source = FakeSource(
        [quote({0.6: float(i)}, {}) for i in range(1, 8)],
        statuses=["open", "open", "closed"],
    )
    daemon, _ = run(source, tmp_path, poll_interval=1.0, duration=6.0, status_every=2.0)

    rows = list(Tape.read(daemon.stats.files[0]).replay(speed="max"))
    change = next(e for e in rows if isinstance(e, MarketStatus) and e.status == "closed")
    # The opening read answers "open", the check at +2s answers "open"
    # again, and the one at +4s is the first to see the close. The row
    # is stamped when it was OBSERVED, which is +4s, not when the venue
    # closed the market: a poller cannot know that and must not guess.
    assert change.ts == START + timedelta(seconds=4)


def test_an_unchanged_status_writes_no_extra_rows(tmp_path: Path) -> None:
    # A long capture of a market that never moves should not accumulate
    # a status row per check saying the same thing.
    source = FakeSource([quote({0.6: float(i)}, {}) for i in range(1, 12)])
    daemon, _ = run(source, tmp_path, poll_interval=1.0, duration=10.0, status_every=1.0)

    assert source.describe_calls > 5
    assert daemon.stats.status_changes == 0
    assert _statuses(daemon.stats.files[0]) == [(1, "open")]


def test_a_reopened_market_records_both_transitions(tmp_path: Path) -> None:
    # A halt is not a terminal state, so the tape has to be able to say
    # the market came back.
    source = FakeSource(
        [quote({0.6: float(i)}, {}) for i in range(1, 12)],
        statuses=["open", "halted", "open"],
    )
    daemon, _ = run(source, tmp_path, poll_interval=1.0, duration=8.0, status_every=1.0)

    assert daemon.stats.status_changes == 2
    assert [s for _, s in _statuses(daemon.stats.files[0])] == ["open", "halted", "open"]


def test_status_checks_are_paced_independently_of_polls(tmp_path: Path) -> None:
    # describe() is a second endpoint per market, so a two-second poll
    # loop must not turn into two requests every two seconds.
    source = FakeSource([quote({0.6: float(i)}, {}) for i in range(1, 30)])
    run(source, tmp_path, poll_interval=1.0, duration=20.0, status_every=10.0)

    assert source.book_calls == 20
    # Twenty book requests against one opening read plus a single check
    # at +10s. The run ends at +20s before another check comes due.
    assert source.describe_calls == 2


def test_no_status_check_asks_exactly_once(tmp_path: Path) -> None:
    source = FakeSource(
        [quote({0.6: float(i)}, {}) for i in range(1, 30)],
        statuses=["open", "closed"],
    )
    daemon, _ = run(source, tmp_path, poll_interval=1.0, duration=20.0, status_every=None)

    assert source.describe_calls == 1
    assert daemon.stats.status_changes == 0
    assert _statuses(daemon.stats.files[0]) == [(1, "open")]


def test_a_failed_status_check_is_counted_and_does_not_end_the_capture(
    tmp_path: Path,
) -> None:
    # The lifecycle endpoint is not the book endpoint. Losing it should
    # not throw away book and trade data that is arriving fine.
    source = FakeSource(
        [quote({0.6: float(i)}, {}) for i in range(1, 12)],
        statuses=["open", LiveError("status endpoint 503"), LiveError("status endpoint 503")],
    )
    daemon, logs = run(source, tmp_path, poll_interval=1.0, duration=6.0, status_every=2.0)

    assert daemon.stats.status_check_failures == 2
    assert daemon.stats.status_changes == 0
    assert daemon.stats.polls == 6
    assert _statuses(daemon.stats.files[0]) == [(1, "open")]
    assert any("status check failed for M1: status endpoint 503" in line for line in logs)


def test_a_failed_status_check_does_not_overwrite_the_held_status(tmp_path: Path) -> None:
    # An unknown status is not a change. Writing one down because a
    # request timed out would put a claim on the tape that nothing
    # observed.
    source = FakeSource(
        [quote({0.6: float(i)}, {}) for i in range(1, 12)],
        statuses=["open", "closed", LiveError("gone"), LiveError("gone")],
    )
    daemon, _ = run(source, tmp_path, poll_interval=1.0, duration=8.0, status_every=2.0)

    assert [s for _, s in _statuses(daemon.stats.files[0])] == ["open", "closed"]
    assert daemon.stats.status_check_failures == 2


def test_the_segment_after_a_close_opens_with_the_new_status(tmp_path: Path) -> None:
    # A rotated segment must be readable on its own, which means its
    # header states what the market was for THAT segment, not what it
    # was when the capture started.
    source = FakeSource(
        [quote({0.6: float(i)}, {}) for i in range(1, 20)],
        statuses=["open", "open", "closed"],
    )
    daemon, _ = run(
        source, tmp_path, poll_interval=1.0, duration=8.0, rotate_after=2.0, status_every=2.0
    )

    assert len(daemon.stats.files) == 4
    per_file = [[s for _, s in _statuses(p)] for p in daemon.stats.files]
    # Segment 1 was entirely before the close. Segment 2 contains it, so
    # it opens with what the market was when that segment began and
    # carries the transition in its body. Every later segment opens
    # closed, which is what makes a rotated segment readable alone.
    assert per_file == [["open"], ["open", "closed"], ["closed"], ["closed"]]


def test_the_status_row_in_a_segment_sorts_after_that_segment_header(tmp_path: Path) -> None:
    source = FakeSource(
        [quote({0.6: float(i)}, {}) for i in range(1, 20)],
        statuses=["open", "open", "closed"],
    )
    daemon, _ = run(source, tmp_path, poll_interval=1.0, duration=6.0, status_every=2.0)

    events = list(Tape.read(daemon.stats.files[0]).replay(speed="max"))
    assert isinstance(events[0], Market)
    assert isinstance(events[1], MarketStatus)
    assert events[1].status == "open"  # type: ignore[union-attr]
    seqs = [e.seq for e in events]
    assert seqs == sorted(seqs)


def test_a_status_check_uses_the_id_the_user_typed(tmp_path: Path) -> None:
    # A venue can accept several spellings and only the user's is known
    # to resolve, since it is the one that worked at the start.
    source = FakeSource(
        [quote({0.6: float(i)}, {}) for i in range(1, 12)],
        market_id="0xCANONICAL",
    )
    run(source, tmp_path, markets=("some-slug",), poll_interval=1.0, duration=4.0, status_every=2.0)

    assert source.describe_calls == 2
    assert set(source.described) == {"some-slug"}


# -- settlement while capturing -------------------------------------------


def _resolutions(path: Path) -> list[tuple[int, str, float]]:
    """Every resolution row on a tape, in replay order."""
    return [
        (e.seq, e.outcome, e.settlement)
        for e in Tape.read(path).replay(speed="max")
        if isinstance(e, Resolution)
    ]


SETTLED = MarketResolution(outcome="YES", settlement=1.0)


def test_a_market_that_settles_mid_capture_gets_a_resolution_row(tmp_path: Path) -> None:
    # market_status says a market stopped trading. It does not say how
    # it settled, and on a real venue the two arrive minutes apart.
    source = FakeSource(
        [quote({0.6: float(i)}, {}) for i in range(1, 20)],
        statuses=["open", "closed", "closed"],
        resolutions=[None, None, SETTLED],
    )
    daemon, logs = run(source, tmp_path, poll_interval=1.0, duration=8.0, status_every=2.0)

    assert daemon.stats.resolutions == 1
    assert _resolutions(daemon.stats.files[0]) == [(8, "YES", 1.0)]
    assert "M1 resolved: YES settles at 1" in logs
    # The check at +2s saw it close and the check at +4s saw it settle,
    # which is the shape a real venue produces. The tape reads forward
    # in that order.
    rows = list(Tape.read(daemon.stats.files[0]).replay(speed="max"))
    closed = next(e for e in rows if isinstance(e, MarketStatus) and e.status == "closed")
    settled = next(e for e in rows if isinstance(e, Resolution))
    assert closed.seq < settled.seq
    assert closed.ts < settled.ts


def test_a_settlement_is_written_once_no_matter_how_often_it_is_reported(
    tmp_path: Path,
) -> None:
    # A resolution is final, so a venue repeating it on every check must
    # not fill the tape with rows that say the same thing.
    source = FakeSource(
        [quote({0.6: float(i)}, {}) for i in range(1, 20)],
        statuses=["open", "closed"],
        resolutions=[None, SETTLED],
    )
    daemon, _ = run(source, tmp_path, poll_interval=1.0, duration=10.0, status_every=1.0)

    assert daemon.stats.resolutions == 1
    assert len(_resolutions(daemon.stats.files[0])) == 1


def test_an_unsettled_market_writes_no_resolution_row(tmp_path: Path) -> None:
    source = FakeSource(
        [quote({0.6: float(i)}, {}) for i in range(1, 20)],
        statuses=["open", "closed"],
    )
    daemon, _ = run(source, tmp_path, poll_interval=1.0, duration=8.0, status_every=2.0)

    assert daemon.stats.resolutions == 0
    assert _resolutions(daemon.stats.files[0]) == []
    # The status row is still there. Closed is a real observation; it
    # just is not a settlement.
    assert [s for _, s in _statuses(daemon.stats.files[0])] == ["open", "closed"]


def test_a_resolution_row_carries_the_venue_settlement_time_when_there_is_one(
    tmp_path: Path,
) -> None:
    # Kalshi publishes settlement_ts, so the row does not have to be
    # stamped with the observation time that --status-every bounds.
    venue_time = START + timedelta(seconds=1)
    source = FakeSource(
        [quote({0.6: float(i)}, {}) for i in range(1, 20)],
        statuses=["open", "closed"],
        resolutions=[None, MarketResolution(outcome="NO", settlement=1.0, ts=venue_time)],
    )
    daemon, _ = run(source, tmp_path, poll_interval=1.0, duration=8.0, status_every=4.0)

    rows = list(Tape.read(daemon.stats.files[0]).replay(speed="max"))
    resolution = next(e for e in rows if isinstance(e, Resolution))
    assert resolution.ts == venue_time
    # The check that observed it ran at +4s, so the venue's own stamp is
    # three seconds earlier than the moment the capture noticed.
    assert resolution.ts < START + timedelta(seconds=4)


def test_a_resolution_without_a_venue_time_is_stamped_when_it_was_observed(
    tmp_path: Path,
) -> None:
    # Polymarket publishes no settlement time, so the row's timestamp is
    # an upper bound set by --status-every, exactly like a status row.
    source = FakeSource(
        [quote({0.6: float(i)}, {}) for i in range(1, 20)],
        statuses=["open", "closed"],
        resolutions=[None, MarketResolution(outcome="No", settlement=1.0)],
    )
    daemon, _ = run(source, tmp_path, poll_interval=1.0, duration=8.0, status_every=4.0)

    rows = list(Tape.read(daemon.stats.files[0]).replay(speed="max"))
    resolution = next(e for e in rows if isinstance(e, Resolution))
    assert resolution.ts == START + timedelta(seconds=4)


def test_the_segment_after_a_settlement_opens_with_a_resolution_header(
    tmp_path: Path,
) -> None:
    # The same rotation rule the status header follows, one level on: a
    # reader who picks up a late segment alone still learns how the
    # market ended.
    source = FakeSource(
        [quote({0.6: float(i)}, {}) for i in range(1, 20)],
        statuses=["open", "open", "closed"],
        resolutions=[None, None, SETTLED],
    )
    daemon, _ = run(
        source, tmp_path, poll_interval=1.0, duration=8.0, rotate_after=2.0, status_every=2.0
    )

    assert len(daemon.stats.files) == 4
    per_file = [[o for _, o, _ in _resolutions(p)] for p in daemon.stats.files]
    # Segment 1 was before the settlement. Segment 2 watched it happen
    # and carries the observed row. Segments 3 and 4 open with a header.
    assert per_file == [[], ["YES"], ["YES"], ["YES"]]
    # Only the one in segment 2 was observed; the rest are headers.
    assert daemon.stats.resolutions == 1


def test_a_resolution_header_sorts_after_the_status_header_of_its_segment(
    tmp_path: Path,
) -> None:
    source = FakeSource(
        [quote({0.6: float(i)}, {}) for i in range(1, 20)],
        statuses=["open", "closed"],
        resolutions=[None, SETTLED],
    )
    daemon, _ = run(
        source, tmp_path, poll_interval=1.0, duration=8.0, rotate_after=2.0, status_every=2.0
    )

    last = list(Tape.read(daemon.stats.files[-1]).replay(speed="max"))
    assert isinstance(last[0], Market)
    assert isinstance(last[1], MarketStatus)
    assert isinstance(last[2], Resolution)
    seqs = [e.seq for e in last]
    assert seqs == sorted(seqs)


def test_a_market_already_settled_when_the_capture_starts_is_a_header_not_an_event(
    tmp_path: Path,
) -> None:
    # Nothing about it happened while the tape was being written, so it
    # is not counted as something this capture observed.
    source = FakeSource(
        [quote({0.6: float(i)}, {}) for i in range(1, 20)],
        status="closed",
        resolutions=[SETTLED],
    )
    daemon, _ = run(source, tmp_path, poll_interval=1.0, duration=4.0, status_every=2.0)

    assert daemon.stats.resolutions == 0
    assert _resolutions(daemon.stats.files[0]) == [(2, "YES", 1.0)]


def test_no_status_check_still_records_a_settlement_known_at_the_start(
    tmp_path: Path,
) -> None:
    source = FakeSource(
        [quote({0.6: float(i)}, {}) for i in range(1, 20)],
        status="closed",
        resolutions=[SETTLED],
    )
    daemon, _ = run(source, tmp_path, poll_interval=1.0, duration=4.0, status_every=None)

    assert source.describe_calls == 1
    assert [o for _, o, _ in _resolutions(daemon.stats.files[0])] == ["YES"]


def test_a_failed_status_check_cannot_invent_a_settlement(tmp_path: Path) -> None:
    source = FakeSource(
        [quote({0.6: float(i)}, {}) for i in range(1, 12)],
        statuses=["open", LiveError("status endpoint 503")],
        resolutions=[None, SETTLED],
    )
    daemon, _ = run(source, tmp_path, poll_interval=1.0, duration=6.0, status_every=2.0)

    assert daemon.stats.status_check_failures == 2
    assert daemon.stats.resolutions == 0
    assert _resolutions(daemon.stats.files[0]) == []


def test_a_refused_settlement_also_costs_that_checks_status_update(
    tmp_path: Path,
) -> None:
    # The status and the settlement come from one document, so a source
    # that refuses the settlement gives up the status for that check
    # too. The next check recovers it. Written down because the
    # alternative, blessing a status out of a document the source could
    # not fully parse, is the worse trade.
    class RefusingSource(FakeSource):
        def describe(self, market_id: str) -> MarketDescription:
            self.describe_calls += 1
            self.described.append(market_id)
            if self.describe_calls == 2:
                raise LiveError("two winning outcomes")
            return MarketDescription(
                market_id=self.market_id,
                title="A fake market",
                status="open" if self.describe_calls == 1 else "closed",
            )

    source = RefusingSource([quote({0.6: float(i)}, {}) for i in range(1, 12)])
    daemon, _ = run(source, tmp_path, poll_interval=1.0, duration=6.0, status_every=2.0)

    assert daemon.stats.status_check_failures == 1
    assert daemon.stats.resolutions == 0
    # The check at +2s was refused; the one at +4s recorded the close.
    assert _statuses(daemon.stats.files[0]) == [(1, "open"), (7, "closed")]


# -- retiring a market that is over ---------------------------------------


class MultiSource(LiveSource):
    """A source for several markets whose lifecycles move independently.

    :class:`FakeSource` answers for one market and ignores the id it is
    given, which is enough for everything above and useless here: the
    whole question is what a capture does when one of its markets is
    over and another is still trading. Every call records the market it
    was asked about, so a test can assert what STOPPED being requested,
    which is the point of the feature.
    """

    key: ClassVar[str] = "fake"
    source_tag: ClassVar[str] = "fake-rest-poll"

    def __init__(
        self,
        market_ids: list[str],
        *,
        settles: dict[str, int] | None = None,
        closes: dict[str, int] | None = None,
        book_errors: set[str] | None = None,
    ) -> None:
        self.market_ids = market_ids
        #: market id to the describe() call index, counted per market,
        #: at which it starts answering closed AND settled.
        self.settles = settles or {}
        #: market id to the index at which it starts answering closed
        #: with no winner, which must NOT retire it.
        self.closes = closes or {}
        self.book_errors = book_errors or set()
        self.describe_calls: dict[str, int] = {}
        self.book_calls: dict[str, int] = {}
        self.trade_calls: dict[str, int] = {}
        self.size = 1.0

    def list_markets(self, *, limit: int, search: str | None = None) -> list[MarketRef]:
        return [MarketRef(market_id=m, title=m) for m in self.market_ids]

    def describe(self, market_id: str) -> MarketDescription:
        index = self.describe_calls.get(market_id, 0)
        self.describe_calls[market_id] = index + 1
        settles_at = self.settles.get(market_id)
        closes_at = self.closes.get(market_id)
        if settles_at is not None and index >= settles_at:
            return MarketDescription(
                market_id=market_id, title=market_id, status="closed", resolution=SETTLED
            )
        if closes_at is not None and index >= closes_at:
            return MarketDescription(market_id=market_id, title=market_id, status="closed")
        return MarketDescription(market_id=market_id, title=market_id, status="open")

    def book(self, market_id: str) -> BookQuote:
        self.book_calls[market_id] = self.book_calls.get(market_id, 0) + 1
        if market_id in self.book_errors:
            raise LiveError(f"{market_id} is gone")
        self.size += 1.0
        return quote({0.6: self.size}, {})

    def trades(self, market_id: str, *, limit: int = 100) -> list[TradeTick]:
        self.trade_calls[market_id] = self.trade_calls.get(market_id, 0) + 1
        return []


def run_multi(
    source: MultiSource, tmp_path: Path, **config: object
) -> tuple[CaptureDaemon, list[str]]:
    logs: list[str] = []
    clock = Clock()
    settings: dict[str, object] = {
        "markets": tuple(source.market_ids),
        "output": tmp_path / "out.parquet",
        "poll_interval": 1.0,
        "duration": 10.0,
        "status_every": 1.0,
        # Every test using this helper is about retirement, so the flag
        # is on by default here and the "off" case is covered against
        # the single-market source above.
        "stop_when_settled": True,
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


def test_the_predicate_needs_both_halves() -> None:
    # A status can flap and a settlement cannot, which is the whole
    # reason both are required before anything irreversible acts on it.
    closed_and_settled = MarketDescription(
        market_id="M", title="M", status="closed", resolution=SETTLED
    )
    assert closed_and_settled.finished
    assert not MarketDescription(market_id="M", title="M", status="closed").finished
    assert not MarketDescription(
        market_id="M", title="M", status="open", resolution=SETTLED
    ).finished
    assert not MarketDescription(market_id="M", title="M", status="halted").finished


def test_a_capture_ends_when_its_only_market_closes_and_settles(tmp_path: Path) -> None:
    source = FakeSource(
        [quote({0.6: float(i)}, {}) for i in range(1, 40)],
        statuses=["open", "closed", "closed"],
        resolutions=[None, None, SETTLED],
    )
    daemon, logs = run(
        source,
        tmp_path,
        poll_interval=1.0,
        duration=30.0,
        status_every=1.0,
        stop_when_settled=True,
    )

    assert daemon.stats.stopped_early is True
    assert daemon.stats.retired == 1
    # The third describe() is the one that reports the settlement, and
    # it runs after the third poll, so the capture is over long before
    # the thirty seconds asked for.
    assert daemon.stats.polls == 3
    assert any("closed and settled" in line for line in logs)
    # The settlement it stopped for is on the tape, so the file says why
    # it is short without needing the log.
    assert _resolutions(daemon.stats.files[0]) == [(6, "YES", 1.0)]


def test_without_the_flag_the_same_capture_runs_its_full_duration(tmp_path: Path) -> None:
    # The regression guard for the default. Ending early is a real
    # change in what a tape covers, so it happens only when asked for.
    source = FakeSource(
        [quote({0.6: float(i)}, {}) for i in range(1, 40)],
        statuses=["open", "closed", "closed"],
        resolutions=[None, None, SETTLED],
    )
    daemon, _ = run(source, tmp_path, poll_interval=1.0, duration=30.0, status_every=1.0)

    assert daemon.stats.stopped_early is False
    assert daemon.stats.retired == 0
    assert daemon.stats.polls == 30


def test_a_closed_market_that_never_settles_does_not_end_a_capture(tmp_path: Path) -> None:
    # A Kalshi market recorded for these fixtures closed at 17:30 UTC
    # and was still unsettled 28 minutes later. Ending on the close
    # alone would have thrown that wait away.
    source = FakeSource(
        [quote({0.6: float(i)}, {}) for i in range(1, 20)],
        statuses=["open", "closed"],
    )
    daemon, _ = run(
        source,
        tmp_path,
        poll_interval=1.0,
        duration=8.0,
        status_every=1.0,
        stop_when_settled=True,
    )

    assert daemon.stats.stopped_early is False
    assert daemon.stats.retired == 0
    assert daemon.stats.polls == 8


def test_a_flapping_status_cannot_end_a_capture_on_its_own(tmp_path: Path) -> None:
    # Polymarket answered closed, open, closed on three reads twenty
    # seconds apart on 2026-09-07. With no settlement published, none of
    # those reads is grounds for stopping.
    source = FakeSource(
        [quote({0.6: float(i)}, {}) for i in range(1, 20)],
        statuses=["open", "closed", "open", "closed"],
    )
    daemon, _ = run(
        source,
        tmp_path,
        poll_interval=1.0,
        duration=6.0,
        status_every=1.0,
        stop_when_settled=True,
    )

    assert daemon.stats.stopped_early is False
    assert daemon.stats.polls == 6


def test_one_settled_market_stops_being_polled_while_the_others_run(tmp_path: Path) -> None:
    source = MultiSource(["A", "B"], settles={"A": 2})
    daemon, logs = run_multi(source, tmp_path, duration=8.0)

    # A settles on its third describe() and is retired; B is untouched
    # and the capture runs its full eight polls for B.
    assert daemon.stats.retired == 1
    assert daemon.stats.stopped_early is False
    assert daemon.stats.polls == 8
    assert source.book_calls["B"] == 8
    assert source.book_calls["A"] < 8
    assert source.trade_calls["A"] == source.book_calls["A"]
    assert any("A has closed and settled" in line for line in logs)


def test_a_retired_market_stops_being_asked_about_at_all(tmp_path: Path) -> None:
    # The lifecycle re-read is a separate request per market per check,
    # and it is the only per-market cost a streamed capture pays, so
    # narrowing it is half the feature rather than a detail.
    source = MultiSource(["A", "B"], settles={"A": 2})
    run_multi(source, tmp_path, duration=8.0)

    assert source.describe_calls["B"] > source.describe_calls["A"]
    # One at open plus the checks that ran before it retired, and not
    # one after.
    assert source.describe_calls["A"] == 3


def test_a_retired_market_leaves_the_segment_that_ended_it_complete(tmp_path: Path) -> None:
    # A segment describes the markets it has events for and no others,
    # which is the rule that already governed a quiet market and is
    # what a retired one becomes. So the segment that watched A settle
    # carries A's whole story, and the segment after it does not open
    # with a header for a market it observed nothing about.
    source = MultiSource(["A", "B"], settles={"A": 1})
    daemon, _ = run_multi(source, tmp_path, duration=8.0, rotate_after=4.0)

    assert len(daemon.stats.files) == 2
    first = list(Tape.read(daemon.stats.files[0]).replay(speed="max"))
    assert {e.market_id for e in first if isinstance(e, Market)} == {"A", "B"}
    a_rows = [e for e in first if e.market_id == "A"]
    assert [type(e).__name__ for e in a_rows if not isinstance(e, BookDelta)] == [
        "Market",
        "MarketStatus",
        "OrderBookSnapshot",
        "MarketStatus",
        "Resolution",
    ]
    assert [e.status for e in a_rows if isinstance(e, MarketStatus)] == ["open", "closed"]

    last = list(Tape.read(daemon.stats.files[-1]).replay(speed="max"))
    assert {e.market_id for e in last} == {"B"}


def test_the_capture_ends_only_once_every_market_has_settled(tmp_path: Path) -> None:
    source = MultiSource(["A", "B"], settles={"A": 1, "B": 4})
    daemon, _ = run_multi(source, tmp_path, duration=30.0)

    assert daemon.stats.retired == 2
    assert daemon.stats.stopped_early is True
    # B settles on its fifth describe(): one at open, then one per poll
    # from the second poll onward, since the first poll happens before
    # the clock has advanced far enough for a check to be due.
    assert daemon.stats.polls == 5
    # A settled at the check that followed the second poll, so it was
    # polled twice and then never again, while B was polled all five
    # times. That difference IS the narrowing.
    assert source.book_calls == {"A": 2, "B": 5}


def test_a_market_closed_but_unsettled_holds_the_capture_open(tmp_path: Path) -> None:
    # B closes and never settles, so the capture must run its duration
    # even though A is finished. Stopping here would end the capture in
    # exactly the window a settlement is most likely to arrive.
    source = MultiSource(["A", "B"], settles={"A": 1}, closes={"B": 1})
    daemon, _ = run_multi(source, tmp_path, duration=6.0)

    assert daemon.stats.retired == 1
    assert daemon.stats.stopped_early is False
    assert daemon.stats.polls == 6


def test_a_market_already_settled_at_the_start_still_gets_one_poll(tmp_path: Path) -> None:
    # Retirement is evaluated after the poll, so a capture asked for a
    # market that is already over produces a picture of how it ended
    # rather than an empty directory.
    source = MultiSource(["A"], settles={"A": 0})
    daemon, _ = run_multi(source, tmp_path, duration=30.0)

    assert daemon.stats.stopped_early is True
    assert daemon.stats.polls == 1
    assert source.book_calls["A"] == 1
    rows = list(Tape.read(daemon.stats.files[0]).replay(speed="max"))
    assert [type(e).__name__ for e in rows] == [
        "Market",
        "MarketStatus",
        "Resolution",
        "OrderBookSnapshot",
    ]


def test_the_failure_limit_counts_against_the_markets_still_being_polled(
    tmp_path: Path,
) -> None:
    # With A retired, B failing every poll IS every market failing.
    # Comparing against the markets the user named instead would let the
    # only live market fail forever without ever tripping the limit.
    source = MultiSource(["A", "B"], settles={"A": 1}, book_errors={"B"})
    with pytest.raises(LiveError, match="every market failed to poll"):
        run_multi(source, tmp_path, duration=30.0, max_consecutive_failures=3)


def test_a_settled_market_whose_book_is_gone_still_ends_the_capture(tmp_path: Path) -> None:
    # The venue 404s a dead market's book, so the one poll it gets
    # yields nothing and no tape is written. That is not a failure to
    # report as one; there was nothing to record.
    source = MultiSource(["A"], settles={"A": 0}, book_errors={"A"})
    daemon, _ = run_multi(source, tmp_path, duration=30.0, max_consecutive_failures=5)

    assert daemon.stats.stopped_early is True
    assert daemon.stats.files == []
