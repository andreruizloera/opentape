"""CLI tests: inspect, replay, convert."""

from __future__ import annotations

from pathlib import Path

import pytest

from opentape.cli import main
from opentape.tape import Tape
from tests.conftest import FIXTURES


@pytest.fixture
def tape_file(sample_tape: Tape, tmp_path: Path) -> Path:
    path = tmp_path / "m1.parquet"
    sample_tape.write(path)
    return path


def test_inspect_reports_summary(tape_file: Path, capsys) -> None:
    assert main(["inspect", str(tape_file)]) == 0
    out = capsys.readouterr().out
    assert "schema version : 1" in out
    assert "events         : 9" in out
    assert "markets        : 1" in out
    assert "trade                 3" in out
    assert "resolution            1" in out
    assert "2026-03-02T14:30:00.000Z to 2026-03-02T14:30:11.000Z" in out
    assert "M1" in out  # price stats row


def test_replay_streams_events(tape_file: Path, capsys) -> None:
    assert main(["replay", str(tape_file)]) == 0
    lines = capsys.readouterr().out.strip().splitlines()
    assert len(lines) == 9
    assert "MARKET" in lines[0]
    assert "RESOLUTION" in lines[-1]
    assert "YES settles at 1" in lines[-1]


def test_replay_limit(tape_file: Path, capsys) -> None:
    assert main(["replay", str(tape_file), "--limit", "3"]) == 0
    lines = capsys.readouterr().out.strip().splitlines()
    assert len(lines) == 3


def test_replay_speed_flag_paces_output(tape_file: Path, capsys, monkeypatch) -> None:
    sleeps: list[float] = []
    monkeypatch.setattr("opentape.tape.time.sleep", sleeps.append)
    assert main(["replay", str(tape_file), "--speed", "1000"]) == 0
    assert sum(sleeps) == pytest.approx(11.0 / 1000.0)


def test_replay_bad_speed_errors(tape_file: Path, capsys) -> None:
    assert main(["replay", str(tape_file), "--speed", "warp"]) == 1
    assert "--speed" in capsys.readouterr().err


def test_inspect_missing_file_exits_nonzero(tmp_path: Path, capsys) -> None:
    assert main(["inspect", str(tmp_path / "ghost.parquet")]) == 1
    err = capsys.readouterr().err
    assert err.startswith("error:")
    assert "no such file" in err


def test_convert_kalshi_style(tmp_path: Path, capsys) -> None:
    out = tmp_path / "k.parquet"
    code = main(
        ["convert", str(FIXTURES / "kalshi_style.json"), "--format", "kalshi-style", "-o", str(out)]
    )
    assert code == 0
    assert "9 events, 1 market(s)" in capsys.readouterr().out
    tape = Tape.read(out)
    assert len(tape) == 9


def test_convert_default_output_path(tmp_path: Path, capsys) -> None:
    src = tmp_path / "events.json"
    src.write_bytes((FIXTURES / "generic_events.json").read_bytes())
    assert main(["convert", str(src), "--format", "generic"]) == 0
    assert (tmp_path / "events.parquet").exists()


def test_convert_then_inspect_round_trip(tmp_path: Path, capsys) -> None:
    out = tmp_path / "p.parquet"
    assert (
        main(
            [
                "convert",
                str(FIXTURES / "polymarket_style.json"),
                "--format",
                "polymarket-style",
                "-o",
                str(out),
            ]
        )
        == 0
    )
    capsys.readouterr()
    assert main(["inspect", str(out)]) == 0
    inspect_out = capsys.readouterr().out
    assert "events         : 8" in inspect_out
    assert "polymarket-style" in inspect_out


def test_convert_bad_input_errors_cleanly(tmp_path: Path, capsys) -> None:
    bad = tmp_path / "bad.json"
    bad.write_text("{not json")
    assert main(["convert", str(bad), "--format", "generic"]) == 1
    assert "not valid JSON" in capsys.readouterr().err


# -- book -----------------------------------------------------------------


def test_book_prints_a_ladder_with_its_provenance(tape_file: Path, capsys) -> None:
    assert main(["book", str(tape_file)]) == 0
    out = capsys.readouterr().out
    assert "market   : M1" in out
    assert "built    : snapshot at 2026-03-02T14:30:01.000Z plus 1 deltas" in out
    assert "top      : 0.5800 / 0.6000  (mid 0.5900, spread 0.0200)" in out
    assert "0.5800 | 0.6000" in out


def test_book_rebuilds_at_a_given_time(tape_file: Path, capsys) -> None:
    assert main(["book", str(tape_file), "--at", "2026-03-02T14:30:01Z"]) == 0
    out = capsys.readouterr().out
    assert "plus 0 deltas" in out
    # The ask at 0.60 is still 450 before the delta at t=2 sets it to 350.
    assert "450.00" in out


def test_book_depth_limits_the_ladder(tape_file: Path, capsys) -> None:
    assert main(["book", str(tape_file), "--depth", "1"]) == 0
    rows = [ln for ln in capsys.readouterr().out.splitlines() if "|" in ln]
    assert len(rows) == 2  # the header row plus one level


def test_book_requires_a_market_when_the_tape_has_several(tmp_path: Path, capsys) -> None:
    tape = Tape.read(FIXTURES.parent / "sample.parquet")
    path = tmp_path / "many.parquet"
    tape.write(path)
    assert main(["book", str(path)]) == 1
    assert "--market is required" in capsys.readouterr().err


def test_book_rejects_a_timestamp_it_cannot_parse(tape_file: Path, capsys) -> None:
    assert main(["book", str(tape_file), "--at", "yesterday"]) == 1
    assert "--at must be an ISO 8601 timestamp" in capsys.readouterr().err


def test_book_rejects_a_timestamp_with_no_timezone(tape_file: Path, capsys) -> None:
    assert main(["book", str(tape_file), "--at", "2026-03-02T14:30:05"]) == 1
    assert "no timezone" in capsys.readouterr().err


def test_book_before_the_first_snapshot_errors_without_a_traceback(tape_file: Path, capsys) -> None:
    assert main(["book", str(tape_file), "--at", "2026-03-02T14:30:00Z"]) == 1
    err = capsys.readouterr().err
    assert "cannot be rebuilt from deltas alone" in err
    assert "Traceback" not in err


# -- capture and markets --------------------------------------------------
#
# The CLI builds its own HttpFetcher, so these swap that class for one
# that replays the recorded payloads in tests/fixtures/live. No socket
# is opened.

from tests.test_live_sources import FakeFetcher, polymarket  # noqa: E402


@pytest.fixture
def offline(monkeypatch) -> FakeFetcher:
    fetch = polymarket()[1]
    monkeypatch.setattr("opentape.cli.HttpFetcher", lambda **kwargs: fetch)
    return fetch


def test_markets_lists_open_markets(offline: FakeFetcher, capsys) -> None:
    assert main(["markets", "--venue", "polymarket", "--limit", "2"]) == 0
    lines = capsys.readouterr().out.strip().splitlines()
    assert len(lines) == 2
    assert "xi-jinping-out-before-2027" in lines[0]


def test_markets_says_so_when_nothing_matches(offline: FakeFetcher, capsys) -> None:
    assert main(["markets", "--venue", "polymarket", "--search", "zzzz"]) == 0
    assert "no open polymarket markets matching" in capsys.readouterr().out


def test_capture_writes_a_tape_it_can_then_inspect(
    offline: FakeFetcher, tmp_path: Path, capsys
) -> None:
    out = tmp_path / "live.parquet"
    code = main(
        [
            "capture",
            "--venue",
            "polymarket",
            "--market",
            "xi-jinping-out-before-2027",
            "-o",
            str(out),
            "--poll",
            "1ms",
            "--duration",
            "3ms",
            "--quiet",
        ]
    )
    assert code == 0
    printed = capsys.readouterr().out
    assert "captured" in printed and str(out) in printed
    assert main(["inspect", str(out)]) == 0
    assert "polymarket-rest-poll" in capsys.readouterr().out


def test_capture_reports_a_bad_duration_without_a_traceback(
    offline: FakeFetcher, tmp_path: Path, capsys
) -> None:
    code = main(
        [
            "capture",
            "--venue",
            "polymarket",
            "--market",
            "x",
            "-o",
            str(tmp_path / "o.parquet"),
            "--poll",
            "soon",
        ]
    )
    assert code == 1
    assert "--poll must be a duration" in capsys.readouterr().err


def test_capture_reports_an_unknown_market_without_a_traceback(
    monkeypatch, tmp_path: Path, capsys
) -> None:
    fetch = FakeFetcher({"gamma-api": []})
    monkeypatch.setattr("opentape.cli.HttpFetcher", lambda **kwargs: fetch)
    code = main(
        [
            "capture",
            "--venue",
            "polymarket",
            "--market",
            "no-such-market",
            "-o",
            str(tmp_path / "o.parquet"),
            "--duration",
            "1ms",
        ]
    )
    assert code == 1
    err = capsys.readouterr().err
    assert "no market has slug" in err
    assert "Traceback" not in err


def test_an_unknown_venue_is_rejected_by_the_parser(capsys) -> None:
    with pytest.raises(SystemExit):
        main(["markets", "--venue", "nyse"])
    assert "invalid choice" in capsys.readouterr().err


# -- capture --transport websocket ----------------------------------------


def _ws_args(tmp_path: Path, *extra: str) -> list[str]:
    return [
        "capture",
        "--venue",
        "polymarket",
        "--transport",
        "websocket",
        "--market",
        "xi-jinping-out-before-2027",
        "-o",
        str(tmp_path / "o.parquet"),
        *extra,
    ]


def test_a_websocket_capture_of_kalshi_says_why_it_cannot(tmp_path: Path, capsys) -> None:
    """Kalshi's stream needs an API key, which is a different answer
    from "not implemented yet" and is worth saying out loud."""
    code = main(
        [
            "capture",
            "--venue",
            "kalshi",
            "--transport",
            "websocket",
            "--market",
            "X",
            "-o",
            str(tmp_path / "o.parquet"),
        ]
    )
    assert code == 1
    err = capsys.readouterr().err
    assert "requires an API key" in err
    assert "--transport rest-poll" in err
    assert "Traceback" not in err


@pytest.mark.parametrize(
    "flag, value, expected",
    [
        ("--poll", "1s", "--poll applies to --transport rest-poll only"),
        ("--snapshot-every", "5", "--snapshot-every applies to --transport rest-poll only"),
        ("--backfill", "5", "--backfill applies to --transport rest-poll only"),
    ],
)
def test_polling_only_flags_are_refused_for_a_websocket_capture(
    tmp_path: Path, capsys, flag: str, value: str, expected: str
) -> None:
    assert main(_ws_args(tmp_path, flag, value)) == 1
    err = capsys.readouterr().err
    assert expected in err
    assert "Traceback" not in err


def test_the_default_transport_is_still_polling(offline: FakeFetcher, tmp_path: Path) -> None:
    """The flag is additive: a command written before it behaves the same."""
    out = tmp_path / "live.parquet"
    assert (
        main(
            [
                "capture",
                "--venue",
                "polymarket",
                "--market",
                "xi-jinping-out-before-2027",
                "-o",
                str(out),
                "--duration",
                "3ms",
                "--quiet",
            ]
        )
        == 0
    )
    assert "polymarket-rest-poll" in set(Tape.read(out).frame["source"].to_list())


# -- capture --status-every -----------------------------------------------


def _capture_args(tmp_path: Path, *extra: str) -> list[str]:
    return [
        "capture",
        "--venue",
        "polymarket",
        "--market",
        "xi-jinping-out-before-2027",
        "-o",
        str(tmp_path / "o.parquet"),
        "--poll",
        "1ms",
        "--duration",
        "3ms",
        *extra,
    ]


def test_status_every_and_no_status_check_contradict_each_other(
    offline: FakeFetcher, tmp_path: Path, capsys
) -> None:
    code = main(_capture_args(tmp_path, "--status-every", "10s", "--no-status-check"))
    assert code == 1
    err = capsys.readouterr().err
    assert "contradict each other" in err
    assert "Traceback" not in err


def test_a_bad_status_interval_names_its_own_flag(
    offline: FakeFetcher, tmp_path: Path, capsys
) -> None:
    code = main(_capture_args(tmp_path, "--status-every", "whenever"))
    assert code == 1
    assert "--status-every must be a duration" in capsys.readouterr().err


def test_no_status_check_is_accepted_on_its_own(
    offline: FakeFetcher, tmp_path: Path, capsys
) -> None:
    assert main(_capture_args(tmp_path, "--no-status-check", "--quiet")) == 0
    assert "captured" in capsys.readouterr().out


def test_a_capture_that_saw_no_status_change_says_nothing_about_status(
    offline: FakeFetcher, tmp_path: Path, capsys
) -> None:
    # A quiet run should stay quiet. The line only appears when there is
    # something to report.
    assert main(_capture_args(tmp_path, "--quiet")) == 0
    printed = capsys.readouterr().out
    assert "status change" not in printed
    assert "status check" not in printed


def test_the_websocket_transport_accepts_the_status_flags(tmp_path: Path) -> None:
    # --status-every applies to BOTH transports, unlike --poll and
    # --snapshot-every, so it must not land in the refusal list. Checked
    # against the parser rather than by running a capture, because the
    # flag being accepted is exactly the case that goes on to open a
    # real socket.
    from opentape.cli import _status_every, build_parser

    args = build_parser().parse_args(_ws_args(tmp_path, "--status-every", "5s"))
    assert args.transport == "websocket"
    assert _status_every(args) == 5.0


def test_the_status_interval_defaults_to_thirty_seconds(tmp_path: Path) -> None:
    from opentape.cli import _status_every, build_parser

    args = build_parser().parse_args(_capture_args(tmp_path))
    assert _status_every(args) == 30.0


def test_no_status_check_resolves_to_never_asking_again(tmp_path: Path) -> None:
    from opentape.cli import _status_every, build_parser

    args = build_parser().parse_args(_capture_args(tmp_path, "--no-status-check"))
    assert _status_every(args) is None


# -- capture --stop-when-settled -------------------------------------------


@pytest.fixture
def offline_settled(monkeypatch) -> FakeFetcher:
    """The offline fetcher, but the market has closed and settled.

    This is the recorded `polymarket_market_resolved.json`, a real
    market that closed with the NO side winning at 1. Its book is the
    ordinary recorded one, which is what a venue actually serves for a
    settled market: a book nobody will trade against again.
    """
    from tests.test_live_sources import load, polymarket

    resolved = load("polymarket_market_resolved.json")
    fetch = polymarket(
        **{
            "/markets/": resolved,
            "gamma-api": [{"conditionId": resolved["condition_id"]}],
        }
    )[1]
    monkeypatch.setattr("opentape.cli.HttpFetcher", lambda **kwargs: fetch)
    return fetch


def _settled_args(tmp_path: Path, *extra: str) -> list[str]:
    return [
        "capture",
        "--venue",
        "polymarket",
        "--market",
        "will-andrew-bailey-be-the-next-attorney-general-20260731142118983",
        "-o",
        str(tmp_path / "settled.parquet"),
        "--poll",
        "1ms",
        "--duration",
        "1h",
        *extra,
    ]


def test_stop_when_settled_ends_a_capture_of_a_finished_market(
    offline_settled: FakeFetcher, tmp_path: Path, capsys
) -> None:
    # The duration asked for is an hour. Without the flag this test
    # could not exist, which is the point of it.
    code = main(_settled_args(tmp_path, "--stop-when-settled", "--quiet"))
    assert code == 0
    printed = capsys.readouterr().out
    assert "1 market(s) had closed and settled and were dropped" in printed
    assert "ended before its duration" in printed
    # One poll happened before the market was retired, so the tape is a
    # picture of how the market ended rather than an empty file.
    assert (tmp_path / "settled.parquet").exists()
    assert main(["inspect", str(tmp_path / "settled.parquet")]) == 0
    inspected = capsys.readouterr().out
    assert "resolution" in inspected


def test_a_capture_says_nothing_about_settling_when_nothing_settled(
    offline: FakeFetcher, tmp_path: Path, capsys
) -> None:
    # The summary lines appear only when they have something to report,
    # the same rule the status lines already follow.
    assert main(_capture_args(tmp_path, "--stop-when-settled", "--quiet")) == 0
    printed = capsys.readouterr().out
    assert "closed and settled" not in printed
    assert "ended before its duration" not in printed


def test_stop_when_settled_is_off_unless_asked_for(tmp_path: Path) -> None:
    from opentape.cli import build_parser

    assert build_parser().parse_args(_capture_args(tmp_path)).stop_when_settled is False
    args = build_parser().parse_args(_capture_args(tmp_path, "--stop-when-settled"))
    assert args.stop_when_settled is True


def test_stop_when_settled_with_no_status_check_says_it_is_half_blind(
    offline_settled: FakeFetcher, tmp_path: Path, capsys
) -> None:
    # Not a contradiction, so not an error: the opening read still fires
    # and this market is still recognised as over. What the user loses
    # is noticing a settlement that arrives mid-capture, and being told
    # that beats watching an hour elapse.
    code = main(_settled_args(tmp_path, "--stop-when-settled", "--no-status-check"))
    assert code == 0
    printed = capsys.readouterr().out
    assert "note: --no-status-check" in printed
    assert "ended before its duration" in printed


def test_the_websocket_transport_accepts_stop_when_settled(tmp_path: Path) -> None:
    # It applies to both transports, so it must not land in the
    # polling-only refusal list next to --poll and --snapshot-every.
    from opentape.cli import build_parser

    args = build_parser().parse_args(_ws_args(tmp_path, "--stop-when-settled"))
    assert args.transport == "websocket"
    assert args.stop_when_settled is True


def test_a_capture_that_stopped_early_with_nothing_to_record_is_not_a_failure(
    monkeypatch, tmp_path: Path, capsys
) -> None:
    # An empty capture normally exits 1, because the user asked for a
    # tape and has none. This is the one case where it should not:
    # the venue no longer serves a book for a market that is over, so
    # the flag did exactly what it promised and there was nothing left
    # to write. Exiting 1 there would make working as documented look
    # like a failure.
    from opentape.errors import LiveError
    from tests.test_live_sources import FakeFetcher as Routed
    from tests.test_live_sources import load

    resolved = load("polymarket_market_resolved.json")
    fetch = Routed(
        {
            "/book": LiveError("polymarket: 404 for a settled market's book"),
            "data-api": [],
            "gamma-api": [{"conditionId": resolved["condition_id"]}],
            "/markets/": resolved,
        }
    )
    monkeypatch.setattr("opentape.cli.HttpFetcher", lambda **kwargs: fetch)

    code = main(_settled_args(tmp_path, "--stop-when-settled", "--quiet"))
    printed = capsys.readouterr().out
    assert code == 0
    assert "nothing left to record and no tape was written" in printed
    assert not (tmp_path / "settled.parquet").exists()


def test_a_capture_that_wrote_nothing_for_any_other_reason_still_fails(
    monkeypatch, tmp_path: Path, capsys
) -> None:
    # The guard for the carve-out above: the ordinary empty capture must
    # keep exiting 1.
    from opentape.errors import LiveError
    from tests.test_live_sources import FakeFetcher as Routed
    from tests.test_live_sources import load

    market = load("polymarket_market.json")
    fetch = Routed(
        {
            "/book": LiveError("polymarket: the book endpoint is down"),
            "data-api": [],
            "gamma-api": [{"conditionId": market["condition_id"]}],
            "/markets/": market,
        }
    )
    monkeypatch.setattr("opentape.cli.HttpFetcher", lambda **kwargs: fetch)

    code = main(_capture_args(tmp_path, "--stop-when-settled", "--quiet"))
    assert code == 1
    assert "no events were captured" in capsys.readouterr().out
