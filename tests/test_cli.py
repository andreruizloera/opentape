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
