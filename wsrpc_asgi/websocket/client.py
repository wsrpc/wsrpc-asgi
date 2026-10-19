"""Optional asyncio client transport, installed with ``wsrpc-asgi[client]``."""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit, urlunsplit

from .abc import DumpsType, LoadsType, TimeoutType
from .common import WSRPCBase

if TYPE_CHECKING:
    from websockets.asyncio.client import ClientConnection
from .tools import awaitable


class WSRPCClient(WSRPCBase):
    """Bidirectional RPC client backed by websockets.asyncio.

    Pass a ws:// or wss:// URL. Extra keyword arguments go to
    ``websockets.asyncio.client.connect``.
    """

    def __init__(
        self,
        endpoint: str,
        loop: asyncio.AbstractEventLoop | None = None,
        timeout: TimeoutType | None = None,
        loads: LoadsType = json.loads,
        dumps: DumpsType = json.dumps,
        **kwargs: Any,
    ):
        super().__init__(loop=loop, timeout=timeout, loads=loads, dumps=dumps)
        parts = urlsplit(str(endpoint))
        scheme = {"http": "ws", "https": "wss"}.get(parts.scheme, parts.scheme)
        self._url = urlunsplit(parts._replace(scheme=scheme))
        self._connect_kwargs = kwargs
        self.socket: ClientConnection | None = None
        self.send_lock = asyncio.Lock()
        self._connect_lock = asyncio.Lock()

    async def connect(self):
        from websockets.asyncio.client import connect

        async with self._connect_lock:
            if self.closed:
                raise ConnectionError("RPC client closed")
            if self.socket is not None:
                return
            self.socket = await connect(self._url, **self._connect_kwargs)
            self._create_task(self._read())

    async def _read(self):
        assert self.socket is not None
        from websockets.exceptions import ConnectionClosed

        try:
            async for message in self.socket:
                await self._on_message(message)
        except ConnectionClosed:
            pass
        finally:
            await self.close()

    async def _send(self, **kwargs):
        if self.closed or self.socket is None:
            raise ConnectionError("RPC client is not connected")
        from websockets.exceptions import ConnectionClosed

        try:
            async with self.send_lock:
                await self.socket.send(self._dumps(kwargs))
        except ConnectionClosed:
            await self.close()
            raise

    async def close(self, message=None):
        try:
            await super().close(message)
        finally:
            if self.socket is not None:
                await self.socket.close()

    async def _executor(self, func):
        return await awaitable(func)()

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


__all__ = ("WSRPCClient",)
