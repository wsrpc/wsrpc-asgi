from aiohttp import web
from aiohttp.test_utils import TestServer
from wsrpc_aiohttp import (
    WebSocketAsync as AiohttpServer,
    WSRPCClient as AiohttpClient,
)

from tests.conftest import serve_app
from wsrpc_asgi import WebSocketAsync, WSRPCClient


async def reverse(socket, value):
    return await socket.proxy.double(value=value)


async def test_aiohttp_client_asgi_server():
    class Server(WebSocketAsync):
        pass

    class Client(AiohttpClient):
        pass

    Server.add_route("reverse", reverse)
    Client.add_route("double", lambda socket, value: value * 2)
    async with serve_app(Server.as_asgi()) as url:
        async with Client(url, timeout=3) as client:
            assert await client.proxy.reverse(value=21) == 42
            assert await client.proxy.ping(text="Привет") == {"text": "Привет"}


async def test_asgi_client_aiohttp_server():
    class Server(AiohttpServer):
        pass

    class Client(WSRPCClient):
        pass

    Server.add_route("reverse", reverse)
    Client.add_route("double", lambda socket, value: value * 2)
    app = web.Application()
    app.router.add_get("/ws/", Server)
    async with TestServer(app) as server:
        async with Client(str(server.make_url("/ws/")), timeout=3) as client:
            assert await client.proxy.reverse(value=21) == 42
            assert await client.proxy.ping(text="Привет") == {"text": "Привет"}
