"""Replay ordering and speed math tests."""

from __future__ import annotations

import pytest

from opentape import OpenTapeError, Tape
from opentape.events import BookDelta, Trade
from tests.conftest import ts


def test_replay_yields_all_events_in_seq_order(sample_tape: Tape, sample_events) -> None:
    replayed = list(sample_tape.replay(speed="max"))
    assert replayed == sample_events
    assert [e.seq for e in replayed] == sorted(e.seq for e in replayed)


def test_timestamp_ties_break_by_sequence_across_event_types(sample_tape: Tape) -> None:
    tied = [e for e in sample_tape.replay(speed="max") if e.ts == ts(2)]
    assert [type(e) for e in tied] == [Trade, BookDelta, Trade]
    assert [e.seq for e in tied] == [3, 4, 5]


def test_replay_order_is_independent_of_construction_order(sample_events) -> None:
    shuffled = list(reversed(sample_events))
    tape = Tape.from_events(shuffled)
    assert list(tape.replay(speed="max")) == sample_events


def test_speed_max_never_sleeps(sample_tape: Tape) -> None:
    sleeps: list[float] = []
    list(sample_tape.replay(speed="max", sleep=sleeps.append))
    assert sleeps == []


def test_speed_scales_sleeps(sample_tape: Tape) -> None:
    sleeps: list[float] = []
    list(sample_tape.replay(speed=10, sleep=sleeps.append))
    # Tape spans 11 seconds (t=0 to t=11); at 10x that is 1.1s of sleeping.
    assert sum(sleeps) == pytest.approx(11.0 / 10.0)
    # Gaps: 1s, 1s, 3s, 5s, 1s between distinct timestamps, each divided by 10.
    assert sorted(sleeps) == pytest.approx(sorted([0.1, 0.1, 0.3, 0.5, 0.1]))


def test_speed_one_sleeps_real_gaps(sample_tape: Tape) -> None:
    sleeps: list[float] = []
    list(sample_tape.replay(speed=1, sleep=sleeps.append))
    assert sum(sleeps) == pytest.approx(11.0)


def test_no_sleep_between_simultaneous_events(sample_tape: Tape) -> None:
    sleeps: list[float] = []
    list(sample_tape.replay(speed=1, sleep=sleeps.append))
    # Five distinct-timestamp gaps only; ties at t=0 and t=2 add no sleeps.
    assert len(sleeps) == 5
    assert all(s > 0 for s in sleeps)


def test_each_sleep_is_bounded_by_largest_gap(sample_tape: Tape) -> None:
    sleeps: list[float] = []
    list(sample_tape.replay(speed=2, sleep=sleeps.append))
    assert max(sleeps) == pytest.approx(5.0 / 2.0)  # largest gap is 5s


def test_replay_is_lazy_no_sleep_before_first_event(sample_tape: Tape) -> None:
    sleeps: list[float] = []
    iterator = sample_tape.replay(speed=1, sleep=sleeps.append)
    first = next(iterator)
    assert first.seq == 0
    assert sleeps == []  # no sleep before the first event


@pytest.mark.parametrize("bad", [0, -1, -0.5, "fast", ""])
def test_invalid_speed_rejected(sample_tape: Tape, bad) -> None:
    with pytest.raises(OpenTapeError, match="speed"):
        next(iter(sample_tape.replay(speed=bad)))


def test_empty_tape_replays_to_nothing() -> None:
    assert list(Tape.from_events([]).replay(speed="max")) == []
