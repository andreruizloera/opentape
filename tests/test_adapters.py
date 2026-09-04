"""Adapter conversion tests against the bundled fixtures."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from opentape import AdapterError
from opentape.adapters import ADAPTERS, convert_file
from opentape.events import (
    BookDelta,
    Market,
    MarketStatus,
    OrderBookSnapshot,
    Resolution,
    Trade,
)
from tests.conftest import FIXTURES


def events_of(tape, cls):
    return [e for e in tape.replay(speed="max") if isinstance(e, cls)]


# -- kalshi-style ----------------------------------------------------------


@pytest.fixture(scope="module")
def kalshi_tape():
    return convert_file(FIXTURES / "kalshi_style.json", "kalshi-style")


def test_kalshi_event_counts(kalshi_tape) -> None:
    assert len(kalshi_tape) == 9
    assert len(events_of(kalshi_tape, Trade)) == 3
    assert len(events_of(kalshi_tape, BookDelta)) == 3
    assert len(events_of(kalshi_tape, OrderBookSnapshot)) == 1


def test_kalshi_cents_become_fractions(kalshi_tape) -> None:
    trades = events_of(kalshi_tape, Trade)
    assert [t.price for t in trades] == [0.62, 0.61, 0.63]
    assert all(0.0 <= t.price <= 1.0 for t in trades)


def test_kalshi_taker_side_maps_to_buy_sell(kalshi_tape) -> None:
    trades = {t.trade_id: t for t in events_of(kalshi_tape, Trade)}
    assert trades["a1f0"].side == "buy"  # taker_side yes
    assert trades["a1f1"].side == "sell"  # taker_side no


def test_kalshi_no_bids_become_complementary_asks(kalshi_tape) -> None:
    (snap,) = events_of(kalshi_tape, OrderBookSnapshot)
    assert [lv.price for lv in snap.bids] == [0.61, 0.60, 0.59]  # yes bids, best first
    # no bids at 37, 36, 35 cents become asks at 0.63, 0.64, 0.65
    assert [lv.price for lv in snap.asks] == [0.63, 0.64, 0.65]
    assert [lv.size for lv in snap.asks] == [450.0, 800.0, 1500.0]


def test_kalshi_signed_deltas_become_absolute_sizes(kalshi_tape) -> None:
    deltas = events_of(kalshi_tape, BookDelta)
    # yes 61 starts at 500: -100 leaves 400, then -400 leaves 0.
    yes_deltas = [d for d in deltas if d.side == "bid"]
    assert [(d.price, d.size) for d in yes_deltas] == [(0.61, 400.0), (0.61, 0.0)]
    # no 38 is new: +250 creates an ask at 0.62 with size 250.
    (ask_delta,) = [d for d in deltas if d.side == "ask"]
    assert (ask_delta.price, ask_delta.size) == (0.62, 250.0)


def test_kalshi_market_and_status(kalshi_tape) -> None:
    (market,) = events_of(kalshi_tape, Market)
    assert market.market_id == "FEDCUT-26SEP"
    assert market.outcomes == ("YES", "NO")
    (status,) = events_of(kalshi_tape, MarketStatus)
    assert status.status == "open"  # "active" normalized


def test_kalshi_negative_delta_without_book_rejected(tmp_path: Path) -> None:
    doc = {
        "orderbook_deltas": [
            {"ticker": "T", "ts": "2026-01-01T00:00:00Z", "price": 50, "delta": -10, "side": "yes"}
        ]
    }
    path = tmp_path / "bad.json"
    path.write_text(json.dumps(doc))
    with pytest.raises(AdapterError, match="below zero"):
        convert_file(path, "kalshi-style")


# -- polymarket-style ------------------------------------------------------


@pytest.fixture(scope="module")
def poly_tape():
    return convert_file(FIXTURES / "polymarket_style.json", "polymarket-style")


def test_poly_event_counts(poly_tape) -> None:
    assert len(poly_tape) == 8
    assert len(events_of(poly_tape, Trade)) == 3
    assert len(events_of(poly_tape, BookDelta)) == 2
    assert len(events_of(poly_tape, OrderBookSnapshot)) == 1


def test_poly_decimal_strings_parse(poly_tape) -> None:
    trades = {t.trade_id: t for t in events_of(poly_tape, Trade)}
    assert trades["tr-9001"].price == 0.62
    assert trades["tr-9001"].size == 150.5
    assert trades["tr-9001"].side == "buy"
    assert trades["tr-9003"].side == "sell"


def test_poly_epoch_timestamps(poly_tape) -> None:
    trades = {t.trade_id: t for t in events_of(poly_tape, Trade)}
    assert trades["tr-9001"].ts == datetime.fromtimestamp(1772461805, tz=UTC)
    (snap,) = events_of(poly_tape, OrderBookSnapshot)
    # book timestamp given in epoch milliseconds
    assert snap.ts == datetime.fromtimestamp(1772461800, tz=UTC)


def test_poly_book_sorted_best_first(poly_tape) -> None:
    (snap,) = events_of(poly_tape, OrderBookSnapshot)
    assert [lv.price for lv in snap.bids] == [0.61, 0.60, 0.58]
    assert [lv.price for lv in snap.asks] == [0.63, 0.64, 0.66]


def test_poly_price_changes_become_deltas(poly_tape) -> None:
    deltas = events_of(poly_tape, BookDelta)
    assert [(d.side, d.price, d.size) for d in deltas] == [
        ("bid", 0.61, 250.0),
        ("ask", 0.63, 0.0),
    ]


def test_poly_out_of_range_price_rejected(tmp_path: Path) -> None:
    doc = {
        "trades": [
            {
                "id": "x",
                "market": "0x1",
                "price": "1.20",
                "size": "5",
                "side": "BUY",
                "timestamp": 1772461805,
            }
        ]
    }
    path = tmp_path / "bad.json"
    path.write_text(json.dumps(doc))
    with pytest.raises(AdapterError, match="outside"):
        convert_file(path, "polymarket-style")


# -- generic ---------------------------------------------------------------


def test_generic_json_full_round(tmp_path: Path) -> None:
    tape = convert_file(FIXTURES / "generic_events.json", "generic")
    assert len(tape) == 8
    assert tape.market_ids() == ["GOV-X-2026"]
    (snap,) = events_of(tape, OrderBookSnapshot)
    # both {"price":..} objects and [p, s] pairs are accepted
    assert [lv.price for lv in snap.bids] == [0.58, 0.57]
    (res,) = events_of(tape, Resolution)
    assert (res.outcome, res.settlement) == ("YES", 1.0)
    assert all(e.source == "my-pipeline" for e in tape.replay(speed="max"))


def test_generic_csv(tmp_path: Path) -> None:
    tape = convert_file(FIXTURES / "generic_trades.csv", "generic")
    assert len(tape) == 6
    trades = events_of(tape, Trade)
    assert [t.price for t in trades] == [0.52, 0.51, 0.52]
    (market,) = events_of(tape, Market)
    assert market.outcomes == ("YES", "NO")
    assert market.title == "Example market from CSV"


def test_generic_seq_assignment_orders_by_ts_stable(tmp_path: Path) -> None:
    tape = convert_file(FIXTURES / "generic_trades.csv", "generic")
    events = list(tape.replay(speed="max"))
    assert [e.seq for e in events] == list(range(len(events)))
    # The two trades tied at 14:02:30 keep file order: sell 0.51 then buy 0.52.
    tied = [e for e in events if isinstance(e, Trade) and e.ts.second == 30]
    assert [(t.side, t.price) for t in tied] == [("sell", 0.51), ("buy", 0.52)]


def test_generic_csv_snapshot_rejected(tmp_path: Path) -> None:
    path = tmp_path / "snap.csv"
    path.write_text("type,ts,market_id\nbook_snapshot,2026-01-01T00:00:00Z,M\n")
    with pytest.raises(AdapterError, match="use JSON"):
        convert_file(path, "generic")


def test_generic_naive_timestamp_rejected(tmp_path: Path) -> None:
    path = tmp_path / "naive.json"
    path.write_text(
        json.dumps(
            [
                {
                    "type": "trade",
                    "ts": "2026-01-01T00:00:00",
                    "market_id": "M",
                    "price": 0.5,
                    "size": 1,
                }
            ]
        )
    )
    with pytest.raises(AdapterError, match="timezone"):
        convert_file(path, "generic")


# -- registry --------------------------------------------------------------


def test_unknown_format_rejected(tmp_path: Path) -> None:
    path = tmp_path / "x.json"
    path.write_text("[]")
    with pytest.raises(AdapterError, match="unknown format"):
        convert_file(path, "nasdaq-style")


def test_missing_input_file_rejected() -> None:
    with pytest.raises(AdapterError, match="no such file"):
        convert_file("does-not-exist.json", "generic")


def test_registry_covers_documented_formats() -> None:
    assert set(ADAPTERS) == {"generic", "kalshi-style", "polymarket-style"}


def test_adapter_output_is_replayable_and_writable(tmp_path: Path) -> None:
    for fmt, name in [
        ("kalshi-style", "kalshi_style.json"),
        ("polymarket-style", "polymarket_style.json"),
        ("generic", "generic_events.json"),
    ]:
        tape = convert_file(FIXTURES / name, fmt)
        out = tmp_path / f"{fmt}.parquet"
        tape.write(out)
        from opentape import Tape

        back = Tape.read(out)
        assert list(back.replay(speed="max")) == list(tape.replay(speed="max"))
