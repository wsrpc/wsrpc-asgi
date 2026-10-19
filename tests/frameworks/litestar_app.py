from litestar import Litestar, get

from wsrpc_asgi import RPCMiddleware

from .common import create_handler


@get("/health")
async def health() -> dict[str, bool]:
    return {"ok": True}


http_app = Litestar(route_handlers=[health])
rpc = create_handler().as_asgi()
app = RPCMiddleware(http_app, rpc, path="/ws/")
