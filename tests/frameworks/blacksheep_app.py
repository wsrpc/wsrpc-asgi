from blacksheep import Application
from blacksheep.server.responses import json

from wsrpc_asgi import RPCMiddleware

from .common import create_handler

http_app = Application()


@http_app.router.get("/health")
async def health():
    return json({"ok": True})


rpc = create_handler().as_asgi()
app = RPCMiddleware(http_app, rpc, path="/ws/")
