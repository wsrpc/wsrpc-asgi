from quart import Quart

from wsrpc_asgi import RPCMiddleware

from .common import create_handler

app = Quart(__name__)


@app.get("/health")
async def health():
    return {"ok": True}


rpc = create_handler().as_asgi()
app.asgi_app = RPCMiddleware(app.asgi_app, rpc, path="/ws/")
