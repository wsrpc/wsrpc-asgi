from contextlib import asynccontextmanager

from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route, WebSocketRoute

from .common import create_handler

rpc = create_handler().as_asgi()


@asynccontextmanager
async def lifespan(app):
    yield
    await rpc.close()


async def health(request):
    return JSONResponse({"ok": True})


app = Starlette(
    routes=[Route("/health", health), WebSocketRoute("/ws/", rpc)],
    lifespan=lifespan,
)
