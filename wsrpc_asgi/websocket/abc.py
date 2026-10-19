import asyncio
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Callable, Mapping
from typing import (
    Any,
    Concatenate,
    Generic,
    ParamSpec,
    TypeAlias,
    TypeVar,
)
from uuid import UUID

LocksCollectionType: TypeAlias = defaultdict[int, asyncio.Lock]
FutureCollectionType: TypeAlias = dict[int, asyncio.Future[Any]]
TimeoutType: TypeAlias = int | float
LoadsType: TypeAlias = Callable[..., Any]
DumpsType: TypeAlias = Callable[..., str]


SocketT = TypeVar("SocketT", bound="WSRPCBase")


class AbstractRoute(Generic[SocketT]):
    def __call__(self, method: str) -> Callable[..., Any]:
        raise NotImplementedError

    def _onclose(self) -> Any:
        pass

    # noinspection PyUnusedLocal
    def __init__(self, socket: SocketT):
        pass

    @property
    def socket(self) -> SocketT:
        raise NotImplementedError

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        raise NotImplementedError


class ProxyMethod:
    __slots__ = "__call", "__name"

    def __init__(self, call_method, name):
        self.__call = call_method
        self.__name = name

    def __call__(self, **kwargs):
        return self.__call(self.__name, **kwargs)

    def __getattr__(self, item: str):
        return self.__class__(self.__call, ".".join((self.__name, item)))


class Proxy:
    __slots__ = ("__call",)

    def __init__(self, call_method):
        self.__call = call_method

    def __getattr__(self, item: str):
        return ProxyMethod(self.__call, item)


EventType: TypeAlias = Mapping[str, Any]
EventListenerType: TypeAlias = Callable[[EventType], Any]


class WSRPCBase(ABC):
    @abstractmethod
    def __init__(
        self,
        loop: asyncio.AbstractEventLoop | None = None,
        timeout: TimeoutType | None = None,
    ):
        raise NotImplementedError((loop, timeout))

    @abstractmethod
    async def close(self, message: Any = None):
        """Cancel all pending tasks"""
        raise NotImplementedError

    @abstractmethod
    async def handle_binary(self, message: bytes) -> None:
        raise NotImplementedError

    @abstractmethod
    async def handle_message(self, message: str):
        raise NotImplementedError

    @abstractmethod
    async def _on_message(self, msg: str | bytes):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def get_routes(cls) -> Mapping[str, "RouteType"]:
        raise NotImplementedError

    @classmethod
    def get_clients(cls) -> dict[UUID, "AbstractWSRPC"]:
        raise NotImplementedError

    @property
    @abstractmethod
    def routes(self) -> dict[str, "RouteType"]:
        raise NotImplementedError

    @property
    def clients(self) -> dict[UUID, "AbstractWSRPC"]:
        """Property which contains the socket clients"""
        raise NotImplementedError

    @abstractmethod
    def prepare_args(self, args) -> tuple[list[Any], dict[str, Any]]:
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def is_route(func) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def handle_method(
        self,
        method: str,
        serial: int,
        args: list[Any],
        kwargs: Mapping[str, Any],
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    async def handle_result(self, serial: int, result: Any):
        raise NotImplementedError

    @abstractmethod
    async def handle_error(self, serial, error):
        raise NotImplementedError

    @abstractmethod
    async def handle_event(self, event):
        raise NotImplementedError

    @abstractmethod
    def resolver(self, func_name: str) -> Callable[..., Any]:
        raise NotImplementedError

    @abstractmethod
    async def call(
        self,
        func: str,
        timeout: TimeoutType | None = None,
        **kwargs: Any,
    ):
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
        raise NotImplementedError

    async def emit(self, event: Any) -> None:
        pass

    @abstractmethod
    def add_event_listener(self, func: EventListenerType) -> None:
        raise NotImplementedError

    def remove_event_listeners(self, func: EventListenerType) -> None:
        raise NotImplementedError

    @classmethod
    def remove_route(cls, route: str, fail: bool = True):
        """Removes route by name. If `fail=True` an exception
        will be raised in case the route was not found."""

        raise NotImplementedError

    @property
    @abstractmethod
    def proxy(self) -> Proxy:
        """Special property which allow run the remote functions
        by `dot` notation

        .. code-block:: python

            # calls remote function with name ping
            await client.proxy.ping()

            # full equivalent of
            await client.call('ping')
        """
        raise NotImplementedError


WSRPC = TypeVar("WSRPC", bound=WSRPCBase)
RouteType: TypeAlias = Callable[..., Any] | type[AbstractRoute]
P = ParamSpec("P")


class AbstractWSRPC(WSRPCBase, ABC):
    @classmethod
    @abstractmethod
    def add_route(
        cls,
        route: str,
        handler: Callable[Concatenate[WSRPC, P], Any] | type[AbstractRoute],
    ) -> None:
        """Expose local function through RPC

        :param route: Name which function will be aliased for this function.
                      Remote side should call function by this name.
        :param handler: Function or Route class (classes based on
                        :class:`wsrpc_asgi.WebSocketRoute`).
                        For route classes the public methods will
                        be registered automatically.

        .. note::

            Route classes might be initialized only once for each
            socket instance.

            A route instance is created lazily for each connection.
            Calling the bare route name resolves its exposed ``init``
            method; ``init`` is not called automatically before other
            methods.

        """
        raise NotImplementedError


RouteCollectionType: TypeAlias = defaultdict[
    type[AbstractWSRPC], dict[str, RouteType]
]
ClientCollectionType: TypeAlias = defaultdict[
    type[AbstractWSRPC], dict[UUID, AbstractWSRPC]
]
EventListenerCollectionType: TypeAlias = set[EventListenerType]
