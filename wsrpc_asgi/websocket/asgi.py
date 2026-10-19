"""Small ASGI WebSocket adapter with no framework dependencies."""

import asyncio
from collections.abc import Awaitable, Callable, Mapping, MutableMapping
from http.cookies import SimpleCookie
from typing import Any, Protocol, TypeAlias
from urllib.parse import parse_qs

Scope: TypeAlias = MutableMapping[str, Any]
Receive: TypeAlias = Callable[[], Awaitable[dict[str, Any]]]
Send: TypeAlias = Callable[[dict[str, Any]], Awaitable[None]]


class WebSocketLike(Protocol):
    """The subset also implemented by Starlette's WebSocket."""

    scope: Scope

    @property
    def headers(self) -> Mapping[str, str]: ...

    @property
    def cookies(self) -> Mapping[str, str]: ...

    async def accept(self) -> None: ...
    async def receive(self) -> MutableMapping[str, Any]: ...
    async def send_text(self, data: str) -> None: ...
    async def send_bytes(self, data: bytes) -> None: ...
    async def close(self, code: int = 1000, reason: str = "") -> None: ...


class ASGIWebSocket:
    """Wrap ASGI receive/send; expose connection metadata for authorization."""

    def __init__(self, scope: Scope, receive: Receive, send: Send):
        self.scope = scope
        self._receive = receive
        self._send = send
        self._send_lock = asyncio.Lock()
        self.accepted = False
        self.closed = False

    @property
    def headers(self) -> Mapping[str, str]:
        return {
            key.decode("latin1").lower(): value.decode("latin1")
            for key, value in self.scope.get("headers", [])
        }

    @property
    def cookies(self) -> dict[str, str]:
        cookie: SimpleCookie = SimpleCookie()
        cookie.load(self.headers.get("cookie", ""))
        return {key: value.value for key, value in cookie.items()}

    @property
    def query_params(self) -> dict[str, list[str]]:
        return parse_qs(self.scope.get("query_string", b"").decode())

    async def accept(self) -> None:
        if self.accepted or self.closed:
            raise RuntimeError("WebSocket has already been accepted or closed")
        message = await self._receive()
        if message["type"] != "websocket.connect":
            self.closed = True
            raise ConnectionError("WebSocket disconnected before acceptance")
        await self._send({"type": "websocket.accept"})
        self.accepted = True

    async def receive(self) -> dict[str, Any]:
        message = await self._receive()
        if message["type"] == "websocket.disconnect":
            self.closed = True
        return message

    async def _send_data(self, **data: Any) -> None:
        async with self._send_lock:
            if self.closed:
                raise ConnectionError("WebSocket closed")
            try:
                await self._send({"type": "websocket.send", **data})
            except OSError:
                self.closed = True
                raise

    async def send_text(self, data: str) -> None:
        await self._send_data(text=data)

    async def send_bytes(self, data: bytes) -> None:
        await self._send_data(bytes=data)

    async def close(self, code: int = 1000, reason: str = "") -> None:
        async with self._send_lock:
            if self.closed:
                return
            self.closed = True
            message: dict[str, Any] = {"type": "websocket.close", "code": code}
            version = self.scope.get("asgi", {}).get("spec_version", "2.0")
            if tuple(map(int, version.split("."))) >= (2, 3):
                message["reason"] = reason
            try:
                await self._send(message)
            except OSError:
                pass
