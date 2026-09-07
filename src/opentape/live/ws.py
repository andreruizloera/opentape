"""The only module in opentape that opens a websocket.

The REST side of live capture has :mod:`opentape.live.http` as its one
socket-owning module, and this is the same idea for streaming: venue
sources receive an already-connected :class:`WebSocket`, parse the text
that comes out of it, and never touch a socket themselves. That is what
keeps the whole parsing layer testable offline against recorded frames.

This is a deliberately small RFC 6455 client, not a general one. It
speaks exactly the subset a public market-data feed needs:

- a client handshake whose ``Sec-WebSocket-Accept`` is verified rather
  than assumed, so a proxy that answers 101 without understanding the
  protocol is caught here instead of producing garbage frames later
- masked client frames, which the RFC requires and servers enforce
- reassembly of fragmented messages, since a large book arrives split
- automatic pong replies, so a connection is not dropped for silence
- a read deadline that reports "nothing yet" instead of blocking
  forever, which is what lets a caller do periodic work (rotating a
  tape file) on a feed that has gone quiet

Anything outside that subset (extensions, subprotocols, permessage
deflate, continuation of control frames) is refused rather than
half-implemented.
"""

from __future__ import annotations

import base64
import contextlib
import hashlib
import os
import socket
import ssl
import struct
import time
import urllib.parse
from collections.abc import Callable
from typing import Protocol

from opentape.errors import LiveError

#: RFC 6455's fixed handshake GUID.
_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

#: Sent on the upgrade request so venue operators can identify the traffic.
USER_AGENT = "opentape/live-capture (+https://github.com/andreruizloera/opentape)"

_TEXT, _BINARY, _CLOSE, _PING, _PONG = 0x1, 0x2, 0x8, 0x9, 0xA
_CONTINUATION = 0x0

#: A single message larger than this is refused rather than buffered. A
#: full Polymarket book runs a few tens of kilobytes, so this is far
#: above anything legitimate and exists only so a broken or hostile peer
#: cannot make the daemon allocate without bound.
MAX_MESSAGE_BYTES = 8 * 1024 * 1024


class Connector(Protocol):
    """Opens a websocket to ``url``. Raises :class:`LiveError` on failure."""

    def __call__(self, url: str, *, timeout: float = ...) -> WebSocketConnection: ...


class WebSocketConnection(Protocol):
    """The part of a websocket a venue source is allowed to see.

    Sources send subscriptions and read text. They do not get to close,
    reconnect, or reach the socket, because those are the daemon's
    decisions and a source that made them itself could not be replayed
    from a recording.
    """

    def send_text(self, payload: str) -> None: ...

    def recv_text(self, *, timeout: float) -> str | None: ...


class WebSocket:
    """A minimal RFC 6455 client over a blocking socket."""

    def __init__(
        self,
        sock: socket.socket,
        *,
        url: str,
        buffered: bytes = b"",
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._sock = sock
        self._url = url
        self._pending = bytearray(buffered)
        self._monotonic = monotonic
        self._closed = False

    # -- lifecycle ---------------------------------------------------------

    @property
    def closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        """Send a close frame, then drop the socket. Never raises."""
        if self._closed:
            return
        self._closed = True
        with contextlib.suppress(OSError):
            self._send_frame(_CLOSE, struct.pack("!H", 1000))
        with contextlib.suppress(OSError):
            self._sock.close()

    def __enter__(self) -> WebSocket:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- sending -----------------------------------------------------------

    def send_text(self, payload: str) -> None:
        """Send one text message."""
        if self._closed:
            raise LiveError(f"websocket {self._url} is closed")
        try:
            self._send_frame(_TEXT, payload.encode("utf-8"))
        except OSError as exc:
            raise LiveError(f"websocket {self._url} send failed: {exc}") from exc

    def _send_frame(self, opcode: int, payload: bytes) -> None:
        """Write one final, masked frame.

        Every client frame is masked with four fresh random bytes, which
        the RFC requires of clients and servers do enforce: an unmasked
        client frame is answered with a close, not with data.
        """
        mask = os.urandom(4)
        size = len(payload)
        if size < 126:
            header = struct.pack("!BB", 0x80 | opcode, 0x80 | size)
        elif size < 65536:
            header = struct.pack("!BBH", 0x80 | opcode, 0x80 | 126, size)
        else:
            header = struct.pack("!BBQ", 0x80 | opcode, 0x80 | 127, size)
        masked = bytes(byte ^ mask[i % 4] for i, byte in enumerate(payload))
        self._sock.sendall(header + mask + masked)

    # -- receiving ---------------------------------------------------------

    def recv_text(self, *, timeout: float) -> str | None:
        """Return the next text message, or None if ``timeout`` elapsed.

        None means "nothing arrived in time", which on a market feed is
        an ordinary event: a quiet market publishes nothing for minutes.
        It is not an error and must not be treated as one. A closed
        connection is a different answer and raises.

        Ping frames are answered here rather than surfaced, and binary
        and pong frames are skipped, so a caller only ever sees the
        messages it asked about.
        """
        deadline = self._monotonic() + timeout
        fragments: list[bytes] = []
        fragment_opcode: int | None = None
        while True:
            remaining = deadline - self._monotonic()
            if remaining <= 0:
                # A half-assembled message is dropped rather than
                # returned: half a JSON document is not a message, and
                # keeping it across calls would splice it onto whatever
                # arrives next.
                return None
            frame = self._read_frame(remaining)
            if frame is None:
                return None
            fin, opcode, payload = frame

            if opcode in (_CLOSE, _PING, _PONG):
                # Control frames are never fragmented and may arrive in
                # the middle of a fragmented message, so they are handled
                # without disturbing what has been assembled so far.
                if not fin:
                    raise LiveError(f"websocket {self._url} sent a fragmented control frame")
                if opcode == _CLOSE:
                    self._closed = True
                    raise LiveError(f"websocket {self._url} closed by the server{_why(payload)}")
                if opcode == _PING:
                    self._send_frame(_PONG, payload)
                continue

            if opcode == _CONTINUATION:
                if fragment_opcode is None:
                    raise LiveError(
                        f"websocket {self._url} sent a continuation with nothing to continue"
                    )
            elif opcode in (_TEXT, _BINARY):
                if fragment_opcode is not None:
                    raise LiveError(f"websocket {self._url} started a message inside another one")
                fragment_opcode = opcode
            else:
                raise LiveError(f"websocket {self._url} sent unknown opcode {opcode}")

            fragments.append(payload)
            if sum(len(part) for part in fragments) > MAX_MESSAGE_BYTES:
                raise LiveError(
                    f"websocket {self._url} sent a message over "
                    f"{MAX_MESSAGE_BYTES:,} bytes; refusing to buffer it"
                )
            if not fin:
                continue

            body = b"".join(fragments)
            fragments = []
            completed, fragment_opcode = fragment_opcode, None
            if completed != _TEXT:
                continue
            try:
                return body.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise LiveError(f"websocket {self._url} sent invalid UTF-8: {exc}") from exc

    def _read_frame(self, timeout: float) -> tuple[bool, int, bytes] | None:
        """Read one frame, or None if it did not arrive within ``timeout``."""
        header = self._read_exactly(2, timeout)
        if header is None:
            return None
        first, second = header
        if first & 0x70:
            raise LiveError(
                f"websocket {self._url} set a reserved bit; no extension was negotiated"
            )
        if second & 0x80:
            # A server frame is never masked. Reading one as masked would
            # silently produce garbage payloads.
            raise LiveError(f"websocket {self._url} sent a masked frame")
        length = second & 0x7F
        if length == 126:
            extended = self._read_exactly(2, timeout)
            if extended is None:
                raise LiveError(f"websocket {self._url} truncated a frame header")
            (length,) = struct.unpack("!H", extended)
        elif length == 127:
            extended = self._read_exactly(8, timeout)
            if extended is None:
                raise LiveError(f"websocket {self._url} truncated a frame header")
            (length,) = struct.unpack("!Q", extended)
        if length > MAX_MESSAGE_BYTES:
            raise LiveError(
                f"websocket {self._url} announced a {length:,} byte frame; refusing to read it"
            )
        payload = b""
        if length:
            # Once a header is in hand the rest of the frame is owed, so
            # this read is not allowed to give up on the caller's
            # deadline: doing so would leave the stream mid-frame.
            body = self._read_exactly(length, None)
            if body is None:
                raise LiveError(f"websocket {self._url} closed mid-frame")
            payload = bytes(body)
        return bool(first & 0x80), first & 0x0F, payload

    def _read_exactly(self, count: int, timeout: float | None) -> bytes | None:
        """Read exactly ``count`` bytes, or None on a clean timeout.

        ``timeout`` of None means "however long it takes", which is used
        once a frame header has been read and the body is owed.
        """
        while len(self._pending) < count:
            if timeout is not None:
                if timeout <= 0:
                    return None
                self._sock.settimeout(timeout)
            else:
                self._sock.settimeout(None)
            try:
                chunk = self._sock.recv(65536)
            except TimeoutError:
                return None
            except OSError as exc:
                self._closed = True
                raise LiveError(f"websocket {self._url} read failed: {exc}") from exc
            if not chunk:
                self._closed = True
                raise LiveError(f"websocket {self._url} closed by the server")
            self._pending.extend(chunk)
        out = bytes(self._pending[:count])
        del self._pending[:count]
        return out


def _why(payload: bytes) -> str:
    """The reason out of a close frame, when it carries one."""
    if len(payload) < 2:
        return ""
    (code,) = struct.unpack("!H", payload[:2])
    reason = payload[2:].decode("utf-8", "replace").strip()
    return f" (code {code}{': ' + reason if reason else ''})"


def connect(url: str, *, timeout: float = 20.0) -> WebSocket:
    """Open a websocket to ``url`` and complete the client handshake."""
    parts = urllib.parse.urlsplit(url)
    if parts.scheme not in ("ws", "wss"):
        raise LiveError(f"websocket url must be ws:// or wss://, got {url!r}")
    if not parts.hostname:
        raise LiveError(f"websocket url has no host: {url!r}")
    secure = parts.scheme == "wss"
    port = parts.port or (443 if secure else 80)
    path = parts.path or "/"
    if parts.query:
        path = f"{path}?{parts.query}"

    key = base64.b64encode(os.urandom(16)).decode()
    request = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {parts.hostname}:{port}\r\n"
        f"Upgrade: websocket\r\n"
        f"Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        f"Sec-WebSocket-Version: 13\r\n"
        f"User-Agent: {USER_AGENT}\r\n"
        f"\r\n"
    ).encode()

    try:
        raw = socket.create_connection((parts.hostname, port), timeout=timeout)
    except OSError as exc:
        raise LiveError(f"could not connect to {url}: {exc}") from exc
    try:
        if secure:
            context = ssl.create_default_context()
            raw = context.wrap_socket(raw, server_hostname=parts.hostname)
        raw.settimeout(timeout)
        raw.sendall(request)
        head, buffered = _read_handshake(raw, url)
    except LiveError:
        raw.close()
        raise
    except OSError as exc:
        raw.close()
        raise LiveError(f"websocket handshake with {url} failed: {exc}") from exc

    _check_handshake(head, key, url)
    return WebSocket(raw, url=url, buffered=buffered)


def _read_handshake(sock: socket.socket, url: str) -> tuple[str, bytes]:
    """Read the response head, returning it and any bytes read past it."""
    buffer = bytearray()
    while b"\r\n\r\n" not in buffer:
        chunk = sock.recv(4096)
        if not chunk:
            raise LiveError(f"websocket handshake with {url} got no response")
        buffer.extend(chunk)
        if len(buffer) > 64 * 1024:
            raise LiveError(f"websocket handshake with {url} sent an oversized response head")
    head, _, rest = bytes(buffer).partition(b"\r\n\r\n")
    return head.decode("latin-1"), rest


def _check_handshake(head: str, key: str, url: str) -> None:
    """Verify the 101 and the accept token, or say exactly what was wrong."""
    lines = head.split("\r\n")
    status = lines[0] if lines else ""
    if " 101" not in status:
        detail = status.strip() or "no status line"
        raise LiveError(f"websocket handshake with {url} was refused: {detail}")
    headers = {}
    for line in lines[1:]:
        name, _, value = line.partition(":")
        if name:
            headers[name.strip().lower()] = value.strip()
    if headers.get("upgrade", "").lower() != "websocket":
        raise LiveError(f"websocket handshake with {url} did not upgrade to websocket")
    expected = base64.b64encode(hashlib.sha1((key + _GUID).encode()).digest()).decode()
    if headers.get("sec-websocket-accept") != expected:
        raise LiveError(
            f"websocket handshake with {url} returned a bad Sec-WebSocket-Accept; "
            f"something between here and the venue answered 101 without speaking websocket"
        )
    if headers.get("sec-websocket-extensions"):
        raise LiveError(
            f"websocket handshake with {url} negotiated the extension "
            f"{headers['sec-websocket-extensions']!r}, which this client does not implement"
        )
