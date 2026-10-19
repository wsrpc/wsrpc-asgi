import asyncio
import json
import shutil

import pytest
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidStatus

from tests.conftest import serve_app
from wsrpc_asgi import (
    STATIC_DIR,
    ClientException,
    RPCMiddleware,
    WebSocketAsync,
    WebSocketRoute,
    WSRPCClient,
)
from wsrpc_asgi.websocket.asgi import ASGIWebSocket


async def test_reverse_calls_with_saturated_semaphore():
    class RPC(WebSocketAsync):
        MAX_CONCURRENT_REQUESTS = 1

    async def reverse(socket, value):
        return await socket.proxy.double(value=value)

    RPC.add_route("reverse", reverse)
    async with serve_app(RPC.as_asgi()) as url:
        async with WSRPCClient(url) as client:
            client.add_route("double", lambda socket, value: value * 2)
            assert await client.call("reverse", value=21, timeout=2) == 42


async def test_timeout_cancellation_and_late_reply():
    class RPC(WebSocketAsync):
        pass

    async def sleep(socket):
        await asyncio.sleep(0.08)
        return True

    RPC.add_route("sleep", sleep)
    async with serve_app(RPC.as_asgi()) as url:
        async with WSRPCClient(url) as client:
            with pytest.raises(asyncio.TimeoutError):
                await client.call("sleep", timeout=0.01)
            assert not client._futures
            task = asyncio.create_task(client.call("sleep"))
            await asyncio.sleep(0.01)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert not client._futures
            await asyncio.sleep(0.1)
            assert await client.proxy.ping(value=1) == {"value": 1}


async def test_disconnect_cancels_handler_and_awaits_route_cleanup():
    started = asyncio.Event()
    stopped = asyncio.Event()
    cleaned = asyncio.Event()

    class RPC(WebSocketAsync):
        pass

    class Slow(WebSocketRoute):
        async def wait(self):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()

        async def _onclose(self):
            await asyncio.sleep(0)
            cleaned.set()

    RPC.add_route("slow", Slow)
    app = RPC.as_asgi()
    async with serve_app(app) as url:
        async with WSRPCClient(url) as client:
            task = asyncio.create_task(client.proxy.slow.wait())
            await asyncio.wait_for(started.wait(), 2)
            await app.close()
            with pytest.raises(ConnectionError):
                await asyncio.wait_for(task, 2)
            await asyncio.wait_for(stopped.wait(), 2)
            await asyncio.wait_for(cleaned.wait(), 2)
            assert not app.get_clients()


async def test_queue_limit_keeps_replies_and_connection_alive():
    entered = asyncio.Event()
    release = asyncio.Event()

    class RPC(WebSocketAsync):
        MAX_CONCURRENT_REQUESTS = 1
        MAX_PENDING_REQUESTS = 1

    async def wait(socket):
        entered.set()
        await release.wait()
        return True

    RPC.add_route("wait", wait)
    async with serve_app(RPC.as_asgi()) as url:
        async with WSRPCClient(url) as client:
            task = asyncio.create_task(client.proxy.wait())
            await asyncio.wait_for(entered.wait(), 2)
            with pytest.raises(ClientException, match="Too many"):
                await client.call("wait", timeout=2)
            release.set()
            assert await asyncio.wait_for(task, 2)
            assert await client.proxy.ping(ok=True) == {"ok": True}


@pytest.mark.parametrize(
    "payload", ["{", "[]", '{"id":true}', '{"id":1,"method":42}']
)
async def test_malformed_messages_close_connection(payload):
    async with serve_app(WebSocketAsync.as_asgi()) as url:
        async with connect(url) as socket:
            await socket.send(payload)
            with pytest.raises(ConnectionClosed):
                await asyncio.wait_for(socket.recv(), 2)


async def test_binary_and_zero_id():
    class RPC(WebSocketAsync):
        async def handle_binary(self, message: bytes):
            await self.socket.send_bytes(message[::-1])

    async with serve_app(RPC.as_asgi()) as url:
        async with connect(url) as socket:
            await socket.send(b"abc")
            assert await socket.recv() == b"cba"
            await socket.send(
                json.dumps({"id": 0, "method": "ping", "params": [1]})
            )
            # Positional input is supported; builtin ping accepts only kwargs.
            response = json.loads(await socket.recv())
            assert response["id"] == 0
            assert "error" in response
            await socket.send(
                json.dumps({"id": 0, "method": "ping", "params": {"ok": True}})
            )
            assert json.loads(await socket.recv()) == {
                "id": 0,
                "result": {"ok": True},
            }


async def test_authorization_scope_and_handshake_rejection():
    class RPC(WebSocketAsync):
        async def authorize(self):
            return self.request.cookies.get("access") == "yes"

    async with serve_app(RPC.as_asgi(path="/ws/")) as url:
        with pytest.raises(InvalidStatus) as error:
            async with connect(url + "/ws/"):
                pass
        assert error.value.response.status_code == 403
        async with WSRPCClient(
            url + "/ws/", additional_headers={"cookie": "access=yes"}
        ) as client:
            assert await client.proxy.ping() == {}
        with pytest.raises(InvalidStatus):
            async with connect(url + "/wrong"):
                pass


async def test_app_registry_isolation():
    class RPC(WebSocketAsync):
        pass

    first = RPC.as_asgi()
    second = RPC.as_asgi()
    async with serve_app(first) as one, serve_app(second) as two:
        async with (
            WSRPCClient(one) as client_one,
            WSRPCClient(two),
        ):
            client_one.add_route("identity", lambda socket: "one")

            # Client registrations are class-scoped; use distinct subclasses.
            class SecondClient(WSRPCClient):
                pass

            second_client = SecondClient(two)
            second_client.add_route("identity", lambda socket: "two")
            async with second_client:
                assert len(first.get_clients()) == 1
                assert len(second.get_clients()) == 2
                assert await first.broadcast("identity") == ["one"]
                await first.close()
                assert await second_client.proxy.ping() == {}


async def test_asgi_20_close_omits_reason_and_lifespan_cleanup():
    messages = []

    async def receive():
        return {"type": "websocket.connect"}

    async def send(message):
        messages.append(message)

    socket = ASGIWebSocket({"type": "websocket"}, receive, send)
    await socket.close(reason="denied")
    await socket.close()
    assert messages == [{"type": "websocket.close", "code": 1000}]

    events = iter([{"type": "lifespan.startup"}, {"type": "lifespan.shutdown"}])

    async def lifespan_receive():
        return next(events)

    app = WebSocketAsync.as_asgi()
    await app({"type": "lifespan"}, lifespan_receive, send)
    assert messages[-2:] == [
        {"type": "lifespan.startup.complete"},
        {"type": "lifespan.shutdown.complete"},
    ]


async def test_middleware_preserves_scopes_and_parent_lifecycle():
    scopes = []
    events = iter([{"type": "lifespan.startup"}, {"type": "lifespan.shutdown"}])
    messages = []

    async def parent(scope, receive, send):
        scopes.append(scope)
        if scope["type"] == "lifespan":
            while True:
                event = await receive()
                await send({"type": event["type"] + ".complete"})
                if event["type"] == "lifespan.shutdown":
                    break

    async def receive():
        return next(events)

    async def send(message):
        messages.append(message)

    wrapped = RPCMiddleware(parent, WebSocketAsync.as_asgi())
    scope = {"type": "http", "path": "/health", "state": {"value": 42}}
    await wrapped(scope, receive, send)
    assert scopes[0] is scope
    await wrapped({"type": "lifespan"}, receive, send)
    assert messages[-1] == {"type": "lifespan.shutdown.complete"}


async def test_bundled_javascript_client(tmp_path):
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node 22+ required for the browser WebSocket API")
    from pathlib import Path

    module = tmp_path / "wsrpc.mjs"
    module.write_text((Path(STATIC_DIR) / "wsrpc.es6.js").read_text())
    script = """
import WSRPC from './wsrpc.mjs';
const rpc = new WSRPC(process.argv[2]);
rpc.addRoute('double', ({value}) => value * 2);
rpc.connect();
await rpc.onEvent('onconnect');
const result = await rpc.call('echo', {text: 'Привет', value: 42});
if (result.value !== 42 || result.text !== 'Привет') process.exit(1);
if (await rpc.call('callback', {value: 21}) !== 42) process.exit(2);
rpc.destroy();
process.exit(0);
"""
    from tests.frameworks.common import create_handler

    async with serve_app(create_handler().as_asgi()) as url:
        process = await asyncio.create_subprocess_exec(
            node,
            "--input-type=module",
            "-",
            url,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=tmp_path,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(script.encode()), 10
            )
            assert process.returncode == 0, stderr.decode()
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()


@pytest.mark.skipif(
    __import__("sys").platform == "win32", reason="fd socket binding is POSIX"
)
async def test_hypercorn_asyncio():
    import socket

    from hypercorn.asyncio import serve
    from hypercorn.config import Config

    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    port = listener.getsockname()[1]
    config = Config()
    config.bind = [f"fd://{listener.detach()}"]
    config.loglevel = "ERROR"
    stop = asyncio.Event()
    task = asyncio.create_task(
        serve(
            WebSocketAsync.as_asgi(),
            config,
            shutdown_trigger=stop.wait,
        )
    )
    try:
        async with WSRPCClient(f"ws://127.0.0.1:{port}/", timeout=3) as client:
            assert await client.proxy.ping(value="hypercorn") == {
                "value": "hypercorn"
            }
    finally:
        stop.set()
        await asyncio.wait_for(task, 5)


async def test_rpc_keepalive_timeout_closes_unresponsive_peer():
    class RPC(WebSocketAsync):
        KEEPALIVE_PING_TIMEOUT = 0.01
        CLIENT_TIMEOUT = 0.02

    async with serve_app(RPC.as_asgi()) as url:
        async with connect(url) as socket:
            request = json.loads(await asyncio.wait_for(socket.recv(), 2))
            assert request["method"] == "ping"
            with pytest.raises(ConnectionClosed):
                await asyncio.wait_for(socket.recv(), 2)


async def test_handler_execution_timeout():
    class RPC(WebSocketAsync):
        REQUEST_EXECUTION_TIMEOUT = 0.01

    async def slow(socket):
        await asyncio.sleep(5)

    RPC.add_route("slow", slow)
    async with serve_app(RPC.as_asgi()) as url:
        async with WSRPCClient(url, timeout=2) as client:
            with pytest.raises(ClientException) as error:
                await client.proxy.slow()
            assert error.value.type == "TimeoutError"
            assert await client.proxy.ping() == {}


async def test_asgi_send_failure_cleans_up_session():
    from tests.frameworks.common import create_handler

    incoming = asyncio.Queue()
    incoming.put_nowait({"type": "websocket.connect"})
    incoming.put_nowait(
        {
            "type": "websocket.receive",
            "text": json.dumps({"id": 1, "method": "echo", "params": {}}),
        }
    )
    app = create_handler().as_asgi()

    async def send(message):
        if message["type"] == "websocket.send":
            raise OSError("Connection lost")
        if message["type"] == "websocket.close":
            incoming.put_nowait({"type": "websocket.disconnect", "code": 1006})

    await asyncio.wait_for(app({"type": "websocket"}, incoming.get, send), 2)
    assert not app.get_clients()
