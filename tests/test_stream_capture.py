"""End-to-end tests for `capture --transport websocket`.

These run the real client in :mod:`opentape.live.ws` against the real
server in :mod:`tests.wsserver` over loopback, replaying messages
recorded from the live venue. Nothing is stubbed between the daemon and
a socket, so the handshake, the masking, the frame parsing, and the
fragment reassembly all execute on every push.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from opentape.errors import LiveError
from opentape.live.daemon import StreamConfig, StreamDaemon
from opentape.live.polymarket import PolymarketLive
from opentape.live.polymarket_stream import PolymarketStream
from opentape.tape import Tape
from tests.wsserver import StreamReplayServer, load_messages

LIVE_FIXTURES = Path(__file__).parent / "fixtures" / "live"
MARKET = "lol-ig1-lgd-2026-09-08"
YES = "114602688616062054693464639818826590702731982636255124778328943187816056373369"


def recorded() -> list[str]:
    return load_messages(LIVE_FIXTURES / "polymarket_ws.jsonl")


class FakeFetcher:
    def __init__(self, market: Any) -> None:
        self.market = market

    def __call__(self, url: str, params: dict[str, Any] | None = None) -> Any:
        if "gamma-api" in url:
            return [{"conditionId": self.market["condition_id"]}]
        return self.market


def source_for(server: StreamReplayServer) -> PolymarketStream:
    market = json.loads((LIVE_FIXTURES / "polymarket_ws_market.json").read_text())
    return PolymarketStream(PolymarketLive(FakeFetcher(market)), url=server.url)


def run(
    server: StreamReplayServer,
    tmp_path: Path,
    *,
    duration: float = 1.0,
    rotate_after: float | None = None,
    max_reconnects: int = 0,
    log: list[str] | None = None,
    status_every: float | None = None,
    source: PolymarketStream | None = None,
    stop_when_settled: bool = False,
) -> Any:
    config = StreamConfig(
        markets=(MARKET,),
        output=tmp_path / "tape.parquet",
        duration=duration,
        rotate_after=rotate_after,
        poll_wait=0.05,
        max_reconnects=max_reconnects,
        reconnect_backoff=0.01,
        status_every=status_every,
        stop_when_settled=stop_when_settled,
    )
    daemon = StreamDaemon(
        source or source_for(server), config, log=(log.append if log is not None else None)
    )
    return daemon.run()


# -- the happy path -------------------------------------------------------


def test_a_recorded_session_becomes_a_readable_tape(tmp_path: Path) -> None:
    with StreamReplayServer(recorded()) as server:
        stats = run(server, tmp_path)

    assert stats.files == [tmp_path / "tape.parquet"]
    tape = Tape.read(tmp_path / "tape.parquet")
    rows = tape.frame
    assert set(rows["source"].to_list()) == {"polymarket-ws"}
    assert set(rows["market_id"].to_list()) == {MARKET}
    kinds = rows["event_type"].value_counts().to_dicts()
    counts = {row["event_type"]: row["count"] for row in kinds}
    assert counts["book_snapshot"] == 3
    assert counts["trade"] == 2
    assert counts["book_delta"] > 50
    # Each segment opens with its own market definition and status rows.
    assert counts["market"] == 1 and counts["market_status"] == 1


def test_the_subscription_names_both_outcome_tokens(tmp_path: Path) -> None:
    with StreamReplayServer(recorded()) as server:
        run(server, tmp_path)
        assert len(server.subscriptions) == 1
        payload = json.loads(server.subscriptions[0])
    assert payload["type"] == "market"
    assert len(payload["assets_ids"]) == 2
    assert YES in payload["assets_ids"]


def test_the_mirror_is_checked_against_the_venues_snapshots(tmp_path: Path) -> None:
    """The integrity claim, running through the whole daemon."""
    with StreamReplayServer(recorded()) as server:
        stats = run(server, tmp_path)
    assert stats.checks == 2
    assert stats.divergences == 0


def test_a_fragmented_replay_produces_the_same_tape(tmp_path: Path) -> None:
    """A real book arrives split across frames."""
    with StreamReplayServer(recorded()) as server:
        whole = run(server, tmp_path)
    with StreamReplayServer(recorded(), fragment_every=3) as server:
        split = run(server, tmp_path / "split")
    assert (split.snapshots, split.deltas, split.trades) == (
        whole.snapshots,
        whole.deltas,
        whole.trades,
    )


def test_a_ping_mid_stream_does_not_disturb_the_capture(tmp_path: Path) -> None:
    with StreamReplayServer(recorded(), ping_every=5) as server:
        stats = run(server, tmp_path)
    assert stats.snapshots == 3 and stats.trades == 2


def test_an_empty_keepalive_frame_is_counted_but_writes_nothing(tmp_path: Path) -> None:
    with StreamReplayServer(["", "", ""]) as server:
        stats = run(server, tmp_path)
    assert stats.messages == 3
    assert stats.events == 0
    assert stats.files == []


# -- gaps and failures ----------------------------------------------------


def test_a_level_update_before_the_first_snapshot_is_dropped_and_counted(
    tmp_path: Path,
) -> None:
    """Applying it to an empty book would invent a book that never was."""
    messages = recorded()
    first_change = next(m for m in messages if '"price_change"' in m)
    first_book = messages[0]
    with StreamReplayServer([first_change, first_book, first_change]) as server:
        stats = run(server, tmp_path)
    assert stats.dropped_updates == 1
    assert stats.snapshots == 1


def test_a_dropped_connection_is_reconnected_and_counted(tmp_path: Path) -> None:
    log: list[str] = []
    with StreamReplayServer(recorded(), close_after=10, connections=2) as server:
        stats = run(server, tmp_path, max_reconnects=2, log=log)
    assert stats.reconnects >= 1
    assert any("dropped every mirrored book" in line for line in log)
    assert len(server.subscriptions) == 2, "the client re-subscribed after reconnecting"


def test_giving_up_still_writes_what_was_captured(tmp_path: Path) -> None:
    """A partial tape is worth more than a lost one."""
    with (
        StreamReplayServer(recorded(), close_after=10, connections=1) as server,
        pytest.raises(LiveError, match="dropped"),
    ):
        run(server, tmp_path, max_reconnects=0)
    assert (tmp_path / "tape.parquet").exists()
    assert Tape.read(tmp_path / "tape.parquet").frame.height > 0


def test_a_dead_endpoint_is_reported_by_name(tmp_path: Path) -> None:
    with StreamReplayServer([]) as server:
        url = server.url
    # The server is now shut down, so the port refuses connections.
    market = json.loads((LIVE_FIXTURES / "polymarket_ws_market.json").read_text())
    source = PolymarketStream(PolymarketLive(FakeFetcher(market)), url=url)
    config = StreamConfig(
        markets=(MARKET,),
        output=tmp_path / "tape.parquet",
        duration=1.0,
        poll_wait=0.05,
        max_reconnects=1,
        reconnect_backoff=0.01,
    )
    with pytest.raises(LiveError, match="could not stay connected"):
        StreamDaemon(source, config).run()


# -- output ---------------------------------------------------------------


def test_rotation_writes_numbered_segments_that_each_stand_alone(tmp_path: Path) -> None:
    with StreamReplayServer(recorded(), delay=0.01) as server:
        stats = run(server, tmp_path, duration=1.2, rotate_after=0.3)
    assert len(stats.files) >= 2
    for path in stats.files:
        assert path.name.startswith("tape-")
        frame = Tape.read(path).frame
        kinds = set(frame["event_type"].to_list())
        assert "market" in kinds, f"{path.name} must name its own market"


def test_a_capture_that_saw_nothing_writes_no_file(tmp_path: Path) -> None:
    with StreamReplayServer([]) as server:
        stats = run(server, tmp_path)
    assert stats.files == []
    assert not (tmp_path / "tape.parquet").exists()


# -- lifecycle status on a streamed capture -------------------------------


class ClosingFetcher(FakeFetcher):
    """A fetcher whose market closes after the first description."""

    def __init__(self, market: Any) -> None:
        super().__init__(market)
        self.market_reads = 0

    def __call__(self, url: str, params: dict[str, Any] | None = None) -> Any:
        if "gamma-api" in url:
            return [{"conditionId": self.market["condition_id"]}]
        self.market_reads += 1
        if self.market_reads > 1:
            return dict(self.market, closed=True)
        return self.market


def test_a_streamed_capture_records_a_market_that_closes(tmp_path: Path) -> None:
    # A change stream carries book and trade messages. The venue does
    # not push "this market closed" down the same socket, so a streamed
    # tape needs the same periodic REST question a polled one asks.
    market = json.loads((LIVE_FIXTURES / "polymarket_ws_market.json").read_text())
    fetcher = ClosingFetcher(market)
    logs: list[str] = []
    with StreamReplayServer(recorded()) as server:
        source = PolymarketStream(PolymarketLive(fetcher), url=server.url)
        stats = run(server, tmp_path, source=source, status_every=0.01, log=logs)

    assert stats.status_changes >= 1
    statuses = [
        e.status
        for e in Tape.read(tmp_path / "tape.parquet").replay(speed="max")
        if type(e).__name__ == "MarketStatus"
    ]
    assert statuses[0] == "open"
    assert "closed" in statuses
    assert any("changed status: open -> closed" in line for line in logs)


def test_a_streamed_capture_asks_once_when_status_checks_are_off(tmp_path: Path) -> None:
    market = json.loads((LIVE_FIXTURES / "polymarket_ws_market.json").read_text())
    fetcher = ClosingFetcher(market)
    with StreamReplayServer(recorded()) as server:
        source = PolymarketStream(PolymarketLive(fetcher), url=server.url)
        stats = run(server, tmp_path, source=source, status_every=None)

    assert stats.status_changes == 0
    assert fetcher.market_reads == 1
    statuses = [
        e.status
        for e in Tape.read(tmp_path / "tape.parquet").replay(speed="max")
        if type(e).__name__ == "MarketStatus"
    ]
    assert statuses == ["open"]


def test_a_failed_status_check_does_not_end_a_streamed_capture(tmp_path: Path) -> None:
    class BrokenFetcher(FakeFetcher):
        def __init__(self, market: Any) -> None:
            super().__init__(market)
            self.reads = 0

        def __call__(self, url: str, params: dict[str, Any] | None = None) -> Any:
            if "gamma-api" in url:
                return [{"conditionId": self.market["condition_id"]}]
            self.reads += 1
            if self.reads > 1:
                raise LiveError("market endpoint 503")
            return self.market

    market = json.loads((LIVE_FIXTURES / "polymarket_ws_market.json").read_text())
    fetcher = BrokenFetcher(market)
    with StreamReplayServer(recorded()) as server:
        source = PolymarketStream(PolymarketLive(fetcher), url=server.url)
        stats = run(server, tmp_path, source=source, status_every=0.01)

    # The socket was fine, so the tape is fine. Only the lifecycle
    # answer is missing, and the count is how a reader learns that.
    assert stats.status_check_failures >= 1
    assert stats.status_changes == 0
    assert stats.events > 0
    assert (tmp_path / "tape.parquet").exists()


# -- settlement on a streamed capture -------------------------------------


class SettlingFetcher(FakeFetcher):
    """A fetcher whose market closes, then settles, on later reads.

    The two happen on separate reads on purpose. A venue closes a market
    and settles it at different moments, and the streamed path has to
    survive a market that is closed but carries no winner yet exactly as
    the polled one does.
    """

    def __init__(self, market: Any) -> None:
        super().__init__(market)
        self.market_reads = 0

    def __call__(self, url: str, params: dict[str, Any] | None = None) -> Any:
        if "gamma-api" in url:
            return [{"conditionId": self.market["condition_id"]}]
        self.market_reads += 1
        if self.market_reads == 1:
            return self.market
        closed = dict(self.market, closed=True)
        if self.market_reads == 2:
            # Closed, no winner yet. This must not produce a resolution.
            return closed
        tokens = [dict(t) for t in closed["tokens"]]
        tokens[1]["winner"] = True
        tokens[1]["price"] = 1
        return dict(closed, tokens=tokens)


def test_a_streamed_capture_records_a_settlement(tmp_path: Path) -> None:
    # The change stream carries no lifecycle message at all, so a
    # settlement is a REST question on a streamed tape exactly as a
    # close is. This is the transport the last two capture bugs were
    # found on, so it is exercised rather than assumed to share code.
    market = json.loads((LIVE_FIXTURES / "polymarket_ws_market.json").read_text())
    fetcher = SettlingFetcher(market)
    logs: list[str] = []
    with StreamReplayServer(recorded()) as server:
        source = PolymarketStream(PolymarketLive(fetcher), url=server.url)
        stats = run(server, tmp_path, source=source, status_every=0.01, log=logs)

    assert fetcher.market_reads >= 3
    assert stats.resolutions == 1
    rows = list(Tape.read(tmp_path / "tape.parquet").replay(speed="max"))
    resolutions = [e for e in rows if type(e).__name__ == "Resolution"]
    assert len(resolutions) == 1
    assert resolutions[0].outcome == market["tokens"][1]["outcome"]
    assert resolutions[0].settlement == 1.0
    # Closed came first and settled after it, on the tape as in reality.
    closed = next(e for e in rows if type(e).__name__ == "MarketStatus" and e.status == "closed")
    assert closed.seq < resolutions[0].seq
    assert any("resolved:" in line for line in logs)


def test_a_streamed_capture_of_a_closed_market_writes_no_settlement(
    tmp_path: Path,
) -> None:
    # The read that sees closed-and-unsettled must write a status row
    # and nothing else. A real Kalshi market sat in exactly this state
    # for 28 minutes; see tests/fixtures/live/kalshi_market_closed.json.
    market = json.loads((LIVE_FIXTURES / "polymarket_ws_market.json").read_text())

    class ClosedNeverSettles(FakeFetcher):
        def __init__(self, m: Any) -> None:
            super().__init__(m)
            self.market_reads = 0

        def __call__(self, url: str, params: dict[str, Any] | None = None) -> Any:
            if "gamma-api" in url:
                return [{"conditionId": self.market["condition_id"]}]
            self.market_reads += 1
            return self.market if self.market_reads == 1 else dict(self.market, closed=True)

    fetcher = ClosedNeverSettles(market)
    with StreamReplayServer(recorded()) as server:
        source = PolymarketStream(PolymarketLive(fetcher), url=server.url)
        stats = run(server, tmp_path, source=source, status_every=0.01)

    assert stats.status_changes >= 1
    assert stats.resolutions == 0
    rows = list(Tape.read(tmp_path / "tape.parquet").replay(speed="max"))
    assert [e for e in rows if type(e).__name__ == "Resolution"] == []


# -- retiring a market on a streamed capture ------------------------------


def test_a_streamed_capture_ends_when_its_market_closes_and_settles(
    tmp_path: Path,
) -> None:
    # The duration asked for is ten seconds and the replay is short, so
    # a capture that runs to its duration and one that stops early are
    # told apart by the flag rather than by the server running out.
    market = json.loads((LIVE_FIXTURES / "polymarket_ws_market.json").read_text())
    logs: list[str] = []
    with StreamReplayServer(recorded()) as server:
        source = PolymarketStream(PolymarketLive(SettlingFetcher(market)), url=server.url)
        stats = run(
            server,
            tmp_path,
            source=source,
            duration=10.0,
            status_every=0.01,
            stop_when_settled=True,
            log=logs,
        )

    assert stats.stopped_early is True
    assert stats.retired == 1
    assert stats.resolutions == 1
    assert any("closed and settled" in line for line in logs)
    # The tape is still a tape: it holds everything the stream published
    # before the settlement, not just the settlement.
    assert stats.snapshots >= 1


def test_a_streamed_capture_of_a_market_that_is_already_over_does_not_connect(
    tmp_path: Path,
) -> None:
    # A stream has no equivalent of the polling daemon's one poll: a
    # snapshot arrives when the venue chooses to send one, and on a
    # market that settled hours ago it may never arrive at all. So the
    # only honest thing is not to open the connection.
    market = json.loads((LIVE_FIXTURES / "polymarket_ws_market.json").read_text())
    tokens = [dict(t) for t in market["tokens"]]
    tokens[1]["winner"] = True
    tokens[1]["price"] = 1
    settled = dict(market, closed=True, tokens=tokens)

    logs: list[str] = []
    with StreamReplayServer(recorded()) as server:
        source = PolymarketStream(PolymarketLive(FakeFetcher(settled)), url=server.url)
        stats = run(
            server,
            tmp_path,
            source=source,
            duration=10.0,
            status_every=0.01,
            stop_when_settled=True,
            log=logs,
        )
        assert server.subscriptions == []

    assert stats.stopped_early is True
    assert stats.retired == 1
    assert stats.messages == 0
    assert stats.files == []
    assert any("not connecting" in line for line in logs)


def test_without_the_flag_a_settled_streamed_market_keeps_its_connection(
    tmp_path: Path,
) -> None:
    # The regression guard for the streamed default, matching the
    # polling one.
    market = json.loads((LIVE_FIXTURES / "polymarket_ws_market.json").read_text())
    with StreamReplayServer(recorded()) as server:
        source = PolymarketStream(PolymarketLive(SettlingFetcher(market)), url=server.url)
        stats = run(server, tmp_path, source=source, status_every=0.01)
        assert len(server.subscriptions) == 1

    assert stats.stopped_early is False
    assert stats.retired == 0
    assert stats.resolutions == 1
