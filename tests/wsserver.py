"""A real websocket server that replays recorded messages, for the tests.

`opentape capture --transport websocket` has one code path and it should
be the path the tests exercise. A fake connector could stand in for the
socket, but then the handshake, the frame parsing, the client masking,
and the fragment reassembly in :mod:`opentape.live.ws` would only ever
run during a live capture against a real venue, which is precisely the
code most worth running offline and on every push.

So this speaks the same RFC 6455 subset the client does, over a real
loopback socket, and replays messages recorded from the live venue. It
implements only what a replay needs and is not part of the shipped
package.
"""

from __future__ import annotations

import base64
import hashlib
import socket
import struct
import threading
import time
from pathlib import Path

_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

TEXT, CONTINUATION, CLOSE, PING = 0x1, 0x0, 0x8, 0x9


def frame(payload: bytes, opcode: int = TEXT, *, fin: bool = True, mask: bool = False) -> bytes:
    """Build one frame. Server frames are unmasked unless a test asks."""
    head = (0x80 if fin else 0x00) | opcode
    size = len(payload)
    flag = 0x80 if mask else 0x00
    if size < 126:
        header = struct.pack("!BB", head, flag | size)
    elif size < 65536:
        header = struct.pack("!BBH", head, flag | 126, size)
    else:
        header = struct.pack("!BBQ", head, flag | 127, size)
    if not mask:
        return header + payload
    key = b"\x01\x02\x03\x04"
    return header + key + bytes(b ^ key[i % 4] for i, b in enumerate(payload))


def load_messages(path: Path) -> list[str]:
    """Recorded messages, one per line.

    A blank line is kept rather than skipped: the venue really does send
    empty text frames as a keepalive, and dropping them here would mean
    the parser never sees one in a test.
    """
    text = path.read_text()
    lines = text.split("\n")
    if lines and lines[-1] == "":
        lines.pop()
    return lines


class StreamReplayServer:
    """Replay recorded text messages to one websocket client at a time."""

    def __init__(
        self,
        messages: list[str],
        *,
        delay: float = 0.0,
        fragment_every: int = 0,
        ping_every: int = 0,
        close_after: int | None = None,
        connections: int = 1,
        accept_token: str | None = None,
        status_line: str | None = None,
    ) -> None:
        self.messages = messages
        self.delay = delay
        self.fragment_every = fragment_every
        self.ping_every = ping_every
        self.close_after = close_after
        self.connections = connections
        #: Override the handshake reply, so a test can prove the client
        #: verifies it instead of trusting a 101.
        self.accept_token = accept_token
        self.status_line = status_line
        self._listener = socket.socket()
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen(4)
        self._listener.settimeout(0.25)
        self.port = self._listener.getsockname()[1]
        #: What the client actually subscribed with, per connection.
        self.subscriptions: list[str] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._serve, daemon=True)

    @property
    def url(self) -> str:
        return f"ws://127.0.0.1:{self.port}/ws/market"

    def __enter__(self) -> StreamReplayServer:
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._stop.set()
        self._thread.join(timeout=5)
        self._listener.close()

    # -- serving -----------------------------------------------------------

    def _serve(self) -> None:
        served = 0
        while not self._stop.is_set() and served < self.connections:
            try:
                client, _ = self._listener.accept()
            except (TimeoutError, OSError):
                continue
            served += 1
            try:
                self._session(client)
            except OSError:
                pass
            finally:
                client.close()

    def _session(self, client: socket.socket) -> None:
        client.settimeout(5.0)
        buffer = b""
        while b"\r\n\r\n" not in buffer:
            chunk = client.recv(4096)
            if not chunk:
                return
            buffer += chunk
        key = ""
        for line in buffer.split(b"\r\n\r\n", 1)[0].decode("latin-1").split("\r\n")[1:]:
            name, _, value = line.partition(":")
            if name.strip().lower() == "sec-websocket-key":
                key = value.strip()
        accept = (
            self.accept_token
            or base64.b64encode(hashlib.sha1((key + _GUID).encode()).digest()).decode()
        )
        status = self.status_line or "HTTP/1.1 101 Switching Protocols"
        client.sendall(
            (
                f"{status}\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                f"Sec-WebSocket-Accept: {accept}\r\n\r\n"
            ).encode()
        )
        if self.status_line or self.accept_token:
            return

        subscription = self._read_message(client)
        if subscription is not None:
            self.subscriptions.append(subscription)

        for index, message in enumerate(self.messages):
            if self._stop.is_set():
                return
            if self.close_after is not None and index >= self.close_after:
                client.sendall(frame(struct.pack("!H", 1000) + b"replay finished", CLOSE))
                return
            if self.ping_every and index and index % self.ping_every == 0:
                client.sendall(frame(b"keepalive", PING))
            payload = message.encode()
            if self.fragment_every and index and index % self.fragment_every == 0 and payload:
                middle = max(1, len(payload) // 2)
                client.sendall(frame(payload[:middle], TEXT, fin=False))
                client.sendall(frame(payload[middle:], CONTINUATION, fin=True))
            else:
                client.sendall(frame(payload))
            if self.delay:
                time.sleep(self.delay)

        # Hold the connection open afterwards, so the capture's own
        # duration decides when it ends, exactly as a quiet market would.
        while not self._stop.is_set():
            time.sleep(0.02)

    def _read_message(self, client: socket.socket) -> str | None:
        """Read one masked client text frame, which is the subscription."""
        try:
            header = client.recv(2)
            if len(header) < 2:
                return None
            length = header[1] & 0x7F
            if length == 126:
                (length,) = struct.unpack("!H", client.recv(2))
            elif length == 127:
                (length,) = struct.unpack("!Q", client.recv(8))
            key = client.recv(4) if header[1] & 0x80 else b""
            body = b""
            while len(body) < length:
                chunk = client.recv(length - len(body))
                if not chunk:
                    return None
                body += chunk
            if key:
                body = bytes(b ^ key[i % 4] for i, b in enumerate(body))
            return body.decode("utf-8", "replace")
        except (OSError, struct.error):
            return None
