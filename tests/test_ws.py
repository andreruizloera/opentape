"""Tests for the websocket client.

Frame-level behaviour is tested over a real ``socketpair``, so exact
bytes can be fed in and the parser is exercised rather than a mock of
it. Connection-level behaviour is tested against the replay server in
:mod:`tests.wsserver`, which performs a real handshake on loopback.
"""

from __future__ import annotations

import re
import socket
import struct
import threading

import pytest

from opentape.errors import LiveError
from opentape.live import ws
from tests.wsserver import CLOSE, CONTINUATION, PING, StreamReplayServer, frame


def pair() -> tuple[ws.WebSocket, socket.socket]:
    """A client speaking to a raw socket a test writes frames into."""
    mine, theirs = socket.socketpair()
    return ws.WebSocket(mine, url="ws://test/socket"), theirs


# -- frames ---------------------------------------------------------------


def test_reads_one_text_message() -> None:
    client, peer = pair()
    peer.sendall(frame(b'{"event_type":"book"}'))
    assert client.recv_text(timeout=2) == '{"event_type":"book"}'


def test_reassembles_a_fragmented_message() -> None:
    client, peer = pair()
    peer.sendall(frame(b'{"a":', fin=False))
    peer.sendall(frame(b"1}", CONTINUATION, fin=True))
    assert client.recv_text(timeout=2) == '{"a":1}'


def test_a_ping_arriving_mid_message_is_answered_without_corrupting_it() -> None:
    """Control frames may interleave with a fragmented message."""
    client, peer = pair()
    peer.sendall(frame(b'{"a":', fin=False))
    peer.sendall(frame(b"are you there", PING))
    peer.sendall(frame(b"1}", CONTINUATION, fin=True))
    assert client.recv_text(timeout=2) == '{"a":1}'
    # The pong is a masked client frame carrying the ping's payload.
    peer.settimeout(2)
    reply = peer.recv(1024)
    assert reply[0] & 0x0F == 0xA
    key = reply[2:6]
    assert bytes(b ^ key[i % 4] for i, b in enumerate(reply[6:])) == b"are you there"


def test_an_empty_text_frame_is_a_message_not_an_error() -> None:
    """The venue really does send these as a keepalive."""
    client, peer = pair()
    peer.sendall(frame(b""))
    assert client.recv_text(timeout=2) == ""


def test_a_timeout_returns_none_rather_than_raising() -> None:
    client, _peer = pair()
    assert client.recv_text(timeout=0.05) is None


def test_a_quiet_feed_then_a_message_still_reads() -> None:
    client, peer = pair()
    assert client.recv_text(timeout=0.05) is None
    peer.sendall(frame(b"late"))
    assert client.recv_text(timeout=2) == "late"


def test_a_close_frame_raises_with_the_code_and_reason() -> None:
    client, peer = pair()
    peer.sendall(frame(struct.pack("!H", 1001) + b"going away", CLOSE))
    with pytest.raises(LiveError, match=r"closed by the server.*1001.*going away"):
        client.recv_text(timeout=2)
    assert client.closed


def test_a_masked_server_frame_is_refused() -> None:
    """Reading one as masked would silently produce garbage payloads."""
    client, peer = pair()
    peer.sendall(frame(b"hello", mask=True))
    with pytest.raises(LiveError, match="masked frame"):
        client.recv_text(timeout=2)


def test_a_reserved_bit_is_refused_because_no_extension_was_negotiated() -> None:
    client, peer = pair()
    peer.sendall(b"\xc1\x02hi")  # RSV1 set
    with pytest.raises(LiveError, match="reserved bit"):
        client.recv_text(timeout=2)


def test_a_fragmented_control_frame_is_refused() -> None:
    client, peer = pair()
    peer.sendall(frame(b"x", PING, fin=False))
    with pytest.raises(LiveError, match="fragmented control frame"):
        client.recv_text(timeout=2)


def test_a_continuation_with_nothing_to_continue_is_refused() -> None:
    client, peer = pair()
    peer.sendall(frame(b"orphan", CONTINUATION, fin=True))
    with pytest.raises(LiveError, match="nothing to continue"):
        client.recv_text(timeout=2)


def test_an_oversized_announced_frame_is_refused_before_it_is_read() -> None:
    """The length is checked against the cap before anything is buffered."""
    client, peer = pair()
    peer.sendall(struct.pack("!BBQ", 0x81, 127, ws.MAX_MESSAGE_BYTES + 1))
    with pytest.raises(LiveError, match="refusing to read it"):
        client.recv_text(timeout=2)


def test_invalid_utf8_is_reported_rather_than_replaced() -> None:
    client, peer = pair()
    peer.sendall(frame(b"\xff\xfe"))
    with pytest.raises(LiveError, match="invalid UTF-8"):
        client.recv_text(timeout=2)


def test_a_dropped_connection_raises_rather_than_looking_quiet() -> None:
    client, peer = pair()
    peer.close()
    with pytest.raises(LiveError, match="closed by the server"):
        client.recv_text(timeout=2)


def test_sending_on_a_closed_socket_is_an_error() -> None:
    client, _peer = pair()
    client.close()
    with pytest.raises(LiveError, match="is closed"):
        client.send_text("{}")


def test_a_client_frame_is_masked_as_the_rfc_requires() -> None:
    client, peer = pair()
    client.send_text("hi")
    peer.settimeout(2)
    sent = peer.recv(1024)
    assert sent[1] & 0x80, "the mask bit must be set on a client frame"
    key = sent[2:6]
    assert bytes(b ^ key[i % 4] for i, b in enumerate(sent[6:])) == b"hi"


def test_a_large_message_uses_the_extended_length_and_round_trips() -> None:
    """A real book runs to tens of kilobytes, past both length forms.

    Written from a thread on purpose: a payload this size does not fit
    in a socket pair's buffer, so a blocking sendall with nobody reading
    would deadlock the test rather than exercise the client.
    """
    client, peer = pair()
    body = "x" * 70_000
    writer = threading.Thread(target=peer.sendall, args=(frame(body.encode()),), daemon=True)
    writer.start()
    assert client.recv_text(timeout=10) == body
    writer.join(timeout=5)


# -- connecting -----------------------------------------------------------


def test_connect_completes_a_real_handshake_and_reads_a_message() -> None:
    with StreamReplayServer(['{"hello":true}']) as server:
        with ws.connect(server.url, timeout=5) as client:
            client.send_text('{"type":"market"}')
            assert client.recv_text(timeout=5) == '{"hello":true}'
        assert server.subscriptions == ['{"type":"market"}']


def test_connect_refuses_a_bad_accept_token() -> None:
    """A proxy can answer 101 without speaking websocket."""
    with (
        StreamReplayServer([], accept_token="not-the-right-digest") as server,
        pytest.raises(LiveError, match="bad Sec-WebSocket-Accept"),
    ):
        ws.connect(server.url, timeout=5)


def test_connect_reports_a_refused_upgrade() -> None:
    with (
        StreamReplayServer([], status_line="HTTP/1.1 403 Forbidden") as server,
        pytest.raises(LiveError, match=re.escape("refused: HTTP/1.1 403 Forbidden")),
    ):
        ws.connect(server.url, timeout=5)


@pytest.mark.parametrize(
    "url, message",
    [
        ("http://example.com/ws", "must be ws:// or wss://"),
        ("wss:///ws", "has no host"),
    ],
)
def test_connect_validates_the_url(url: str, message: str) -> None:
    with pytest.raises(LiveError, match=message):
        ws.connect(url, timeout=1)


def test_connect_reports_a_dead_port_clearly() -> None:
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    port = listener.getsockname()[1]
    listener.close()
    with pytest.raises(LiveError, match="could not connect"):
        ws.connect(f"ws://127.0.0.1:{port}/ws", timeout=2)
