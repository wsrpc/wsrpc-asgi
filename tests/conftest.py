import asyncio
import json
import os
import socket
import sys
import sysconfig
from contextlib import asynccontextmanager

import pytest
import uvicorn

from wsrpc_asgi import WebSocketAsync, WSRPCClient

try:
    import orjson
except ImportError:
    orjson = None

CODECS = [
    pytest.param({}, id="default"),
    pytest.param(dict(loads=json.loads, dumps=json.dumps), id="json"),
]
if orjson is not None:
    CODECS.append(
        pytest.param(
            dict(
                loads=orjson.loads,
                dumps=lambda x, **kw: orjson.dumps(x, **kw).decode(),
            ),
            id="orjson",
        )
    )


@pytest.fixture
def handler():
    class Handler(WebSocketAsync):
        pass

    return Handler


@pytest.fixture
def socket_path():
    return "/ws/"


@asynccontextmanager
async def serve_app(app):
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    port = listener.getsockname()[1]
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            log_level="error",
            lifespan="auto",
            ws="websockets-sansio",
        )
    )
    task = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        for _ in range(500):
            if server.started:
                break
            if task.done():
                await task
                raise RuntimeError("ASGI server failed to start")
            await asyncio.sleep(0.01)
        else:
            raise TimeoutError("ASGI server startup timed out")
        yield f"ws://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        try:
            await asyncio.wait_for(task, 10)
        finally:
            listener.close()


@pytest.fixture(params=CODECS)
def application(request, handler, socket_path):
    handler.configure(**request.param)

    # Bind the class directly so legacy class-level registration is visible.
    async def app(scope, receive, send):
        if scope["type"] == "websocket":
            from wsrpc_asgi.websocket.asgi import ASGIWebSocket

            await handler.serve(ASGIWebSocket(scope, receive, send))
        elif scope["type"] == "lifespan":
            while True:
                message = await receive()
                if message["type"] == "lifespan.startup":
                    await send({"type": "lifespan.startup.complete"})
                else:
                    await send({"type": "lifespan.shutdown.complete"})
                    return

    return app


@pytest.fixture
async def server_url(application):
    async with serve_app(application) as url:
        yield url


@pytest.fixture(params=CODECS)
async def client(request, server_url, socket_path):
    client = WSRPCClient(server_url + socket_path, **request.param)
    try:
        yield client
    finally:
        await client.close()


@pytest.fixture(scope="session", autouse=True)
def verify_free_threaded_runtime():
    if os.environ.get("WSRPC_EXPECT_FREE_THREADED") != "1":
        yield
        return
    assert sysconfig.get_config_var("Py_GIL_DISABLED") == 1
    assert not sys._is_gil_enabled()
    yield
    assert not sys._is_gil_enabled()
