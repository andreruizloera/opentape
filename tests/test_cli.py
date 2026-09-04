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
