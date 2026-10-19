from sanic import Sanic
from sanic.response import json

from wsrpc_asgi import RPCMiddleware

from .common import create_handler

http_app = Sanic("wsrpc_example")


@http_app.get("/health")
async def health(request):
    return json({"ok": True})


rpc = create_handler().as_asgi()
app = RPCMiddleware(http_app, rpc, path="/ws/")
