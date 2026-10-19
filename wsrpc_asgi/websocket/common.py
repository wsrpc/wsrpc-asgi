import abc
import asyncio
import json
import logging
import typing as t
from collections import defaultdict
from collections.abc import Coroutine
from functools import partial
from uuid import UUID

from wsrpc_asgi.signal import Signal

from . import decorators
from .abc import (
    AbstractRoute,
    AbstractWSRPC,
    ClientCollectionType,
    DumpsType,
    EventListenerCollectionType,
    EventListenerType,
    FutureCollectionType,
    LoadsType,
    LocksCollectionType,
    Proxy,
    RouteCollectionType,
    RouteType,
    TimeoutType,
)
from .route import Route
from .tools import Singleton, awaitable, serializer


class WSRPCError(Exception):
    pass


class ClientException(WSRPCError):
    __slots__ = ("type", "message", "raw")

    def __init__(self, payload):
        self.type = payload.get("type")
        self.message = payload.get("message")
        self.raw = payload
        super().__init__(self.message)


class PingTimeoutError(WSRPCError):
    pass


def ping(_, **kwargs):
    return kwargs


log = logging.getLogger(__name__)
ResultT = t.TypeVar("ResultT")


class Nothing(Singleton):
    def __repr__(self):
        return self.__class__.__name__


class CallItem(t.NamedTuple):
    serial: int | None
    method: Nothing | str | None
    error: Nothing | dict[str, t.Any] | None
    result: t.Any
    params: list[t.Any] | dict[str, t.Any] | None


def _route_maker() -> dict[str, RouteType]:
    return {"ping": ping}


class WSRPCBase(AbstractWSRPC):
    """Common WSRPC abstraction"""

    _ROUTES: RouteCollectionType = defaultdict(_route_maker)
    _CLIENTS: ClientCollectionType = defaultdict(dict)

    __slots__ = (
        "_handlers",
        "_loop",
        "_pending_tasks",
        "_locks",
        "_futures",
        "_serial",
        "_timeout",
        "_event_listeners",
    )

    ON_CALL_START: Signal[...] = Signal()
    ON_CALL_SUCCESS: Signal[...] = Signal()
    ON_CALL_FAIL: Signal[...] = Signal()

    _pending_tasks: set[asyncio.Task]
    _handlers: dict[str, AbstractRoute]

    def _dumps(self, value: t.Any) -> str:
        return self._json_dumps(value, default=serializer)

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop | None = None,
        timeout: TimeoutType | None = None,
        loads: LoadsType = json.loads,
        dumps: DumpsType = json.dumps,
    ):
        self._json_dumps = dumps
        self._json_loads = loads
        self._loop = loop or asyncio.get_event_loop()
        self._handlers = {}
        self._pending_tasks = set()
        self._serial = 0
        self._timeout: TimeoutType | None = timeout
        self._locks: LocksCollectionType = defaultdict(asyncio.Lock)
        self._futures: FutureCollectionType = {}
        self._event_listeners: EventListenerCollectionType = set()
        self.semaphore = asyncio.Semaphore(
            getattr(self, "MAX_CONCURRENT_REQUESTS", 25)
        )
        self._request_tasks: set[asyncio.Task] = set()
        self._lock_users: dict[int, int] = {}
        self.closed = False
        self._closing_task: asyncio.Task | None = None
        self._close_owner: asyncio.Task | None = None

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        for name in ("ON_CALL_START", "ON_CALL_SUCCESS", "ON_CALL_FAIL"):
            if name not in cls.__dict__:
                setattr(cls, name, getattr(cls, name).copy())

    def _create_task(
        self, coro: Coroutine[t.Any, t.Any, ResultT]
    ) -> asyncio.Task[ResultT]:
        task = self._loop.create_task(coro)
        self._pending_tasks.add(task)

        def done(future: asyncio.Task[ResultT]) -> None:
            self._pending_tasks.discard(future)
            if future.cancelled():
                return
            error = future.exception()
            if error is not None:
                log.error(
                    "RPC background task failed",
                    exc_info=(type(error), error, error.__traceback__),
                )

        task.add_done_callback(done)
        return task

    async def close(self, message=None):
        """Fail pending calls, stop tasks and await route cleanup once."""
        current = asyncio.current_task()
        if self._closing_task is None:
            self.closed = True
            self._close_owner = current
            self._closing_task = self._loop.create_task(self._cleanup())
        # A cancelled handler may close itself while cleanup is joining it.
        if current is not self._closing_task and (
            current is self._close_owner or current not in self._pending_tasks
        ):
            await asyncio.shield(self._closing_task)

    async def _cleanup(self):
        for future in self._futures.values():
            if not future.done():
                future.set_exception(ConnectionError("RPC connection closed"))
        self._futures.clear()
        tasks = [
            task
            for task in self._pending_tasks
            if task is not self._close_owner
        ]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._pending_tasks.clear()
        self._locks.clear()
        self._lock_users.clear()
        self._request_tasks.clear()
        for route in self._handlers.values():
            try:
                await awaitable(route._onclose)()
            except Exception:
                log.exception("RPC route cleanup failed")
        self._handlers.clear()

    async def handle_binary(self, message: bytes):
        """Override to handle binary WebSocket messages."""
        log.warning("Unhandled binary message (%d bytes)", len(message))

    async def _call_method(self, call_item: CallItem):
        try:
            if (
                not isinstance(call_item.method, Nothing)
                and call_item.serial is not None
            ):
                log.debug(
                    "Acquiring lock for %r serial %r", self, call_item.serial
                )
                self._lock_users[call_item.serial] = (
                    self._lock_users.get(call_item.serial, 0) + 1
                )
                async with self._locks[call_item.serial]:
                    args, kwargs = self.prepare_args(call_item.params)

                    return await self.handle_method(
                        call_item.method, call_item.serial, args, kwargs
                    )
            elif not isinstance(call_item.result, Nothing):
                return await self.handle_result(
                    call_item.serial, call_item.result
                )
            elif not isinstance(call_item.error, Nothing):
                return await self.handle_error(
                    call_item.serial, call_item.error
                )
            else:
                return await self.handle_result(call_item.serial, None)

        except Exception as e:
            if self.closed:
                return
            log.exception(e)

            if call_item.serial is not None:
                await self._send(
                    error=self._format_error(e), id=call_item.serial
                )
        finally:
            serial = call_item.serial
            if (
                not isinstance(call_item.method, Nothing)
                and serial is not None
                and serial in self._lock_users
            ):
                self._lock_users[serial] -= 1
                if self._lock_users[serial] == 0:
                    self._lock_users.pop(serial, None)
                    self._locks.pop(serial, None)

    @staticmethod
    def _parse_message(data: dict) -> CallItem:
        message_id: int | None = data.get("id")

        if isinstance(message_id, bool) or not isinstance(message_id, int):
            raise ValueError

        message_method: str | Nothing | None = data.get("method", Nothing())

        message_result: t.Any = data.get("result", Nothing())

        message_error: dict[str, t.Any] | Nothing | None = data.get(
            "error", Nothing()
        )

        message_params: list[t.Any] | dict[t.Any, t.Any] | None = data.get(
            "params", None
        )

        return CallItem(
            serial=message_id,
            method=message_method,
            result=message_result,
            error=message_error,
            params=message_params,
        )

    async def handle_message(self, message: str):
        data = self._json_loads(message)
        if not isinstance(data, dict):
            raise ValueError("RPC message must be an object")
        if data.get("id") is None:
            return await self.handle_event(data)
        call_item = self._parse_message(data)
        await self._call_method(call_item)

    async def _on_message(self, msg: str | bytes):
        """Read replies immediately; dispatch calls concurrently."""
        if isinstance(msg, bytes):
            if len(self._request_tasks) >= getattr(
                self, "MAX_PENDING_REQUESTS", 1024
            ):
                await self.close()
                return
            task = self._create_task(self._dispatch_binary(msg))
            self._request_tasks.add(task)
            task.add_done_callback(self._request_tasks.discard)
            return
        try:
            data = self._json_loads(msg)
            if not isinstance(data, dict):
                raise ValueError("RPC message must be an object")
            if data.get("id") is None:
                await self.handle_event(data)
                return
            item = self._parse_message(data)
            if isinstance(item.method, Nothing):
                await self._call_method(item)
                return
            if not isinstance(item.method, str):
                raise ValueError("RPC method must be a string")
            limit = getattr(self, "MAX_PENDING_REQUESTS", 1024)
            if len(self._request_tasks) >= limit:
                await self._send(
                    id=item.serial,
                    error={
                        "type": "RuntimeError",
                        "message": "Too many concurrent requests",
                    },
                )
                return
            task = self._create_task(self._dispatch_call(item))
            self._request_tasks.add(task)
            task.add_done_callback(self._request_tasks.discard)
        except (ValueError, TypeError, KeyError):
            log.warning("Invalid RPC message", exc_info=True)
            await self.close()

    async def _dispatch_binary(self, message):
        async with self.semaphore:
            await self.handle_binary(message)

    async def _dispatch_call(self, item):
        async with self.semaphore:
            await self._call_method(item)

    @classmethod
    def get_routes(cls) -> dict[str, RouteType]:
        return cls._ROUTES[cls]

    @classmethod
    def get_clients(cls) -> dict[UUID, AbstractWSRPC]:
        return cls._CLIENTS[cls]

    @property
    def routes(self) -> dict[str, RouteType]:
        """Property which contains the socket routes"""
        return self.get_routes()

    @property
    def clients(self) -> dict[UUID, AbstractWSRPC]:
        """Property which contains the socket clients"""
        return self.get_clients()

    @staticmethod
    def _prepare_args(args):
        arguments: list[t.Any] = []
        kwargs: dict[str, t.Any] = {}

        if isinstance(args, type(None)):
            return arguments, kwargs

        if isinstance(args, list):
            arguments.extend(args)
        elif isinstance(args, dict):
            kwargs.update(args)
        else:
            arguments.append(args)

        return arguments, kwargs

    def prepare_args(self, args):
        return self._prepare_args(args)

    @staticmethod
    def is_route(func):
        return hasattr(func, "__self__") and isinstance(func.__self__, Route)

    async def handle_method(self, method, serial, args, kwargs):
        await self.ON_CALL_START.call(
            method=method, serial=serial, args=args, kwargs=kwargs
        )
        callee = self.resolver(method)

        if not self.is_route(callee):
            a = [self]
            a.extend(args)
            args = a

        func = partial(callee, *args, **kwargs)
        try:
            execution_timeout = getattr(self, "REQUEST_EXECUTION_TIMEOUT", None)
            result = await asyncio.wait_for(
                self._executor(func), execution_timeout
            )
        except Exception as err:
            await self.ON_CALL_FAIL.call(
                method=method, serial=serial, args=args, kwargs=kwargs, err=err
            )
            raise

        await self.ON_CALL_SUCCESS.call(
            method=method,
            serial=serial,
            args=args,
            kwargs=kwargs,
            result=result,
        )

        await self._send(result=result, id=serial)

    async def handle_result(self, serial, result):
        cb = self._futures.pop(serial, None)
        if not cb or cb.done():
            return
        cb.set_result(result)

    async def handle_error(self, serial, error):
        self._reject(serial, error)
        log.error("Client return error: \n\t%r", error)

    async def handle_event(self, event):
        for listener in self._event_listeners:
            self._loop.call_soon(listener, event)

    @abc.abstractmethod
    async def _send(self, **kwargs):
        raise NotImplementedError

    @staticmethod
    def _format_error(e):
        return {"type": str(type(e).__name__), "message": str(e)}

    def _reject(self, serial, error):
        future = self._futures.pop(serial, None)
        if future is not None and not future.done():
            future.set_exception(ClientException(error))

    def _unresolvable(self, func_name, *args, **kwargs):
        raise NotImplementedError(
            f'Callback function "{func_name!r}" not implemented'
        )

    def resolver(self, func_name):
        class_name, method = (
            func_name.split(".", 1) if "." in func_name else (func_name, "init")
        )

        callee: t.Any = self.routes.get(class_name, self._unresolvable)

        if isinstance(callee, decorators.ProxyBase):
            callee = callee.func

        condition = (
            callee == self._unresolvable
            or isinstance(getattr(callee, "__self__", None), Route)
            or (isinstance(callee, type) and issubclass(callee, Route))
        )

        if condition:
            if class_name not in self._handlers:
                self._handlers[class_name] = callee(self)

            return self._handlers[class_name](method)

        callee = self.routes.get(func_name, self._unresolvable)
        if hasattr(callee, "__call__"):
            return callee
        else:
            raise NotImplementedError(
                f"Method call of {repr(callee)} is not implemented"
            )

    def _get_serial(self):
        self._serial += 2
        return self._serial

    async def call(self, func: str, timeout=None, **kwargs):
        """Method for call remote function

        Remote methods allows only kwargs as arguments.

        You might use functions as route or classes

        .. code-block:: python

            async def remote_function(socket: WSRPCBase, *, foo, bar):
                # call function from the client-side
                await socket.proxy.ping()
                return foo + bar

            class RemoteClass(WebSocketRoute):

                # this method executes when remote side call route name
                async def init(self):
                    # call function from the client-side
                    await self.socket.proxy.ping()

                async def make_something(self, foo, bar):
                    return foo + bar

        """
        serial = self._get_serial()

        if self.closed:
            raise ConnectionError("RPC connection closed")
        future = self._loop.create_future()
        self._futures[serial] = future

        payload = dict(id=serial, method=func, params=kwargs)

        log.info(
            'Sending request #%r "%s(%r)" to the client.', serial, func, kwargs
        )

        try:
            await self._send(**payload)
            return await asyncio.wait_for(
                future, timeout=self._timeout if timeout is None else timeout
            )
        finally:
            self._futures.pop(serial, None)
            if not future.done():
                future.cancel()
            elif not future.cancelled():
                future.exception()

    async def emit(self, event):
        await self._send(**event)

    @classmethod
    def add_route(cls, route: str, handler: RouteType) -> None:
        """Expose local function through RPC

        :param route: Name which function will be aliased for this function.
                      Remote side should call function by this name.
        :param handler: Function or Route class (classes based on
                        :class:`wsrpc_asgi.WebSocketRoute`).
                        For route classes the public methods will
                        be registered automatically.

        .. note::

            Route classes might be initialized only once for the each
            socket instance.

            A route instance is created lazily for each connection.
            Calling the bare route name resolves its exposed ``init``
            method; ``init`` is not called automatically before other
            methods.

        """
        assert callable(handler) or isinstance(handler, Route)
        if callable(handler):
            handler = decorators.proxy(handler)

        cls.get_routes()[route] = handler

    def add_event_listener(self, func: EventListenerType):
        self._event_listeners.add(func)

    def remove_event_listeners(self, func):
        return self._event_listeners.remove(func)

    @classmethod
    def remove_route(cls, route: str, fail=True):
        """Removes route by name. If `fail=True` an exception
        will be raised in case the route was not found."""

        if fail:
            cls.get_routes().pop(route)
        else:
            cls.get_routes().pop(route, None)

    def __repr__(self):
        if hasattr(self, "id"):
            return f"<RPCWebSocket: ID[{self.id}]>"
        else:
            return f"<RPCWebsocket: {self.__hash__()} (waiting)>"

    @abc.abstractmethod
    async def _executor(self, func):
        raise NotImplementedError

    @property
    def proxy(self):
        """Special property which allow run the remote functions
        by `dot` notation

        .. code-block:: python

            # calls remote function with name ping
            await client.proxy.ping()

            # full equivalent of
            await client.call('ping')
        """
        return Proxy(self.call)


__all__ = ("Route", "WSRPCBase", "ClientException", "WSRPCError")
