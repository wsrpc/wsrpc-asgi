import falcon.asgi

from wsrpc_asgi import RPCMiddleware

from .common import create_handler


class Health:
    async def on_get(self, request, response):
        response.media = {"ok": True}


http_app = falcon.asgi.App()
http_app.add_route("/health", Health())
rpc = create_handler().as_asgi()
app = RPCMiddleware(http_app, rpc, path="/ws/")
