"""ASGI server endpoints and optional Starlette/FastAPI integration."""

import asyncio
import inspect
import json
import logging
import uuid
from functools import partial
from typing import Any, cast

from wsrpc_asgi.signal import Signal

from .asgi import ASGIWebSocket, Receive, Scope, Send, WebSocketLike
from .common import ClientException, WSRPCBase
from .tools import awaitable

log = logging.getLogger(__name__)


class WebSocketBase(WSRPCBase):
    """One RPC session. Subclass to customize authorization and hooks."""

    KEEPALIVE_PING_TIMEOUT: int | float = 30
    CLIENT_TIMEOUT: int | float = 10
    MAX_CONCURRENT_REQUESTS: int = 25
    MAX_PENDING_REQUESTS: int = 1024
    REQUEST_EXECUTION_TIMEOUT: int | float | None = None
    JSON_LOADS = staticmethod(json.loads)
    JSON_DUMPS = staticmethod(json.dumps)

    ON_AUTH_SUCCESS: Signal[...] = Signal()
    ON_AUTH_FAIL: Signal[...] = Signal()
    ON_CONN_OPEN: Signal[...] = Signal()
    ON_CONN_CLOSE: Signal[...] = Signal()
    ON_CONN_FAIL: Signal[...] = Signal()

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        for name in (
            "ON_AUTH_SUCCESS",
            "ON_AUTH_FAIL",
            "ON_CONN_OPEN",
            "ON_CONN_CLOSE",
            "ON_CONN_FAIL",
        ):
            if name not in cls.__dict__:
                setattr(cls, name, getattr(cls, name).copy())

    def __init__(self, socket: WebSocketLike, *, context: Any = None):
        super().__init__(
            timeout=self.REQUEST_EXECUTION_TIMEOUT,
            loads=self.JSON_LOADS,
            dumps=self.JSON_DUMPS,
        )
        self.socket = socket
        self.request = socket
        self.scope = socket.scope
        self.context = context
        self.store: dict[str, Any] = {}
        self.id = uuid.uuid4()
        self._send_lock = asyncio.Lock()
        self._transport_closed = False

    @classmethod
    def configure(
        cls,
        keepalive_timeout=30,
        client_timeout=10,
        max_concurrent_requests=25,
        loads=json.loads,
        dumps=json.dumps,
    ):
        if max_concurrent_requests < 1:
            raise ValueError("max_concurrent_requests must be positive")
        cls.KEEPALIVE_PING_TIMEOUT = keepalive_timeout
        cls.CLIENT_TIMEOUT = client_timeout
        cls.MAX_CONCURRENT_REQUESTS = max_concurrent_requests
        cls.JSON_LOADS = staticmethod(loads)
        cls.JSON_DUMPS = staticmethod(dumps)

    @classmethod
    def freeze(cls):
        for name in (
            "ON_AUTH_SUCCESS",
            "ON_AUTH_FAIL",
            "ON_CONN_OPEN",
            "ON_CONN_CLOSE",
            "ON_CONN_FAIL",
            "ON_CALL_START",
            "ON_CALL_SUCCESS",
            "ON_CALL_FAIL",
        ):
            getattr(cls, name).freeze()

    async def authorize(self) -> bool:
        """Return False to reject the WebSocket handshake with HTTP 403."""
        return True

    @classmethod
    async def serve(cls, socket: WebSocketLike, *, context: Any = None) -> None:
        """Accept and serve a Starlette/FastAPI WebSocket until disconnect."""
        await cls(socket, context=context).run()

    @classmethod
    def as_asgi(cls, *, path: str | None = None) -> "RPCApplication":
        """Create an isolated ASGI endpoint with an optional path."""
        return RPCApplication(cls, path=path)

    async def run(self) -> None:
        try:
            await self.ON_CONN_OPEN.call(
                socket=self.socket, request=self.request
            )
            if not await self.authorize():
                await self.ON_AUTH_FAIL.call(
                    socket=self.socket, request=self.request
                )
                return
            await self.ON_AUTH_SUCCESS.call(
                socket=self.socket, request=self.request
            )
            await self.socket.accept()
            self.clients[self.id] = self
            if self.KEEPALIVE_PING_TIMEOUT > 0:
                self._create_task(self._start_ping())
            while not self.closed:
                message = await self._create_task(self.socket.receive())
                if message["type"] == "websocket.disconnect":
                    self._transport_closed = True
                    break
                if message["type"] != "websocket.receive":
                    raise ValueError("Unexpected ASGI WebSocket message")
                text = message.get("text")
                binary = message.get("bytes")
                if text is not None:
                    await self._on_message(text)
                elif binary is not None:
                    await self._on_message(binary)
        except asyncio.CancelledError:
            if not self.closed:
                raise
        except (ConnectionError, OSError):
            self._transport_closed = True
        except Exception as err:
            await self.ON_CONN_FAIL.call(
                socket=self.socket,
                request=self.request,
                err=err,
            )
            raise
        finally:
            try:
                await self.close()
            finally:
                await self.ON_CONN_CLOSE.call(
                    socket=self.socket, request=self.request
                )

    @classmethod
    def broadcast(cls, func, callback=None, return_exceptions=True, **kwargs):
        tasks = []
        for client in tuple(cls.get_clients().values()):
            task = asyncio.ensure_future(client.call(func, **kwargs))
            if callback:
                task.add_done_callback(partial(callback, client))
            tasks.append(task)
        return asyncio.gather(*tasks, return_exceptions=return_exceptions)

    async def _send(self, **kwargs):
        data = self._dumps(kwargs)
        try:
            async with self._send_lock:
                if self.closed:
                    raise ConnectionError("RPC connection closed")
                await self.socket.send_text(data)
        except OSError:
            await self.close()
            raise

    async def close(self, message=None):
        try:
            await super().close(message)
        finally:
            self.clients.pop(self.id, None)
            if not self._transport_closed:
                self._transport_closed = True
                try:
                    await self.socket.close()
                except OSError:
                    pass

    async def _start_ping(self):
        while not self.closed:
            await asyncio.sleep(self.KEEPALIVE_PING_TIMEOUT)
            try:
                await self.call(
                    "ping", timeout=self.CLIENT_TIMEOUT, seq=self._loop.time()
                )
            except (asyncio.TimeoutError, ConnectionError, ClientException):
                await self.close()
                return


class WebSocketAsync(WebSocketBase):
    """Execute synchronous and asynchronous handlers on the event loop."""

    async def _executor(self, func):
        return await awaitable(func)()


class WebSocketThreaded(WebSocketBase):
    """Run synchronous handlers in a thread and await async handlers."""

    async def _executor(self, func):
        if inspect.iscoroutinefunction(func):
            return await func()
        result = await self._loop.run_in_executor(None, func)
        if inspect.isawaitable(result):
            return await result
        return result


class RPCApplication:
    """Bare ASGI endpoint with its own registry and lifespan cleanup.

    Available through ``Handler.as_asgi()``. Keep the instance to broadcast
    only to its clients, or to close them explicitly in a parent lifespan.
    """

    def __init__(
        self, handler: type[WebSocketBase], *, path: str | None = None
    ):
        self.handler = cast(
            type[WebSocketBase],
            type(f"{handler.__name__}Session", (handler,), {}),
        )
        self.handler.get_routes().update(handler.get_routes())
        self.path = path

    def get_clients(self):
        return self.handler.get_clients()

    def broadcast(self, func, **kwargs):
        return self.handler.broadcast(func, **kwargs)

    async def close(self) -> None:
        await asyncio.gather(
            *(client.close() for client in tuple(self.get_clients().values()))
        )

    async def __call__(
        self, scope: Scope, receive: Receive, send: Send
    ) -> None:
        if scope["type"] == "lifespan":
            while True:
                message = await receive()
                if message["type"] == "lifespan.startup":
                    await send({"type": "lifespan.startup.complete"})
                elif message["type"] == "lifespan.shutdown":
                    await self.close()
                    await send({"type": "lifespan.shutdown.complete"})
                    return
        elif scope["type"] == "websocket":
            socket = ASGIWebSocket(scope, receive, send)
            if self.path is not None and scope["path"] != self.path:
                await socket.close()
                return
            await self.handler.serve(socket)
        elif scope["type"] == "http":
            await send(
                {"type": "http.response.start", "status": 404, "headers": []}
            )
            await send({"type": "http.response.body", "body": b"Not found"})
        else:
            raise ValueError(f"Unsupported ASGI scope: {scope['type']}")


__all__ = (
    "ClientException",
    "WebSocketAsync",
    "WebSocketBase",
    "WebSocketThreaded",
    "RPCApplication",
    "RPCMiddleware",
)


class RPCMiddleware:
    """Route one WebSocket path to RPC and forward other scopes unchanged.

    Install outside a framework to add RPC to any asyncio ASGI application.
    Framework middleware inside this wrapper does not run for the RPC path.
    Put shared authentication outside it or implement ``authorize``.
    """

    def __init__(self, app, rpc: RPCApplication, *, path: str = "/ws/"):
        if not path.startswith("/"):
            raise ValueError("path must start with /")
        self.app = app
        self.rpc = rpc
        self.path = path

    async def __call__(
        self, scope: Scope, receive: Receive, send: Send
    ) -> None:
        if scope["type"] == "websocket" and scope["path"] == self.path:
            await self.rpc(scope, receive, send)
        elif scope["type"] == "lifespan":

            async def receive_lifespan():
                message = await receive()
                if message["type"] == "lifespan.shutdown":
                    await self.rpc.close()
                return message

            await self.app(scope, receive_lifespan, send)
        else:
            await self.app(scope, receive, send)
