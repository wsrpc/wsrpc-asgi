# wsrpc-asgi

Bidirectional WebSocket RPC for asyncio ASGI applications. A fork of
[wsrpc-aiohttp](https://github.com/wsrpc/wsrpc-aiohttp) with an ASGI server
transport and an optional `websockets` Python client. Existing WSRPC Python
and JavaScript peers use the same wire protocol.

Supports functions, per-connection route objects, reverse calls, events,
broadcast, lifecycle signals, custom JSON codecs and threaded handlers.
The base package has no runtime dependencies. Python 3.10+ is supported.

## Install

```shell
pip install 'wsrpc-asgi[fastapi,client]' uvicorn
```

For a local checkout use `uv sync` and `uv run` for the commands below.

## FastAPI server

Save as `app.py`:

<!-- name: test_readme_server -->
```python
from fastapi import FastAPI, WebSocket
from wsrpc_asgi import WebSocketAsync


class RPC(WebSocketAsync):
    pass


async def add(socket, *, a: int, b: int) -> int:
    return a + b


RPC.add_route("add", add)
app = FastAPI()


@app.websocket("/ws/")
async def rpc(websocket: WebSocket):
    await RPC.serve(websocket)
```

Run `uvicorn app:app`, then connect from Python:

```python
import asyncio
from wsrpc_asgi import WSRPCClient


async def main():
    async with WSRPCClient("ws://127.0.0.1:8000/ws/") as client:
        print(await client.proxy.add(a=20, b=22))


if __name__ == "__main__":
    asyncio.run(main())
```

## Verify a round trip

This self-contained example runs the same RPC endpoint through FastAPI's
in-process test client:

<!-- name: test_readme_round_trip -->
```python
from fastapi import FastAPI, WebSocket
from fastapi.testclient import TestClient
from wsrpc_asgi import WebSocketAsync


class RPC(WebSocketAsync):
    pass


RPC.add_route("add", lambda socket, a, b: a + b)
app = FastAPI()


@app.websocket("/ws/")
async def endpoint(websocket: WebSocket):
    await RPC.serve(websocket)


with TestClient(app) as http:
    with http.websocket_connect("/ws/") as socket:
        socket.send_json(
            {"id": 1, "method": "add", "params": {"a": 20, "b": 22}}
        )
        assert socket.receive_json() == {"id": 1, "result": 42}
```

## Other ASGI frameworks

Use `RPC.as_asgi()` as a standalone ASGI application or framework route.
`RPCMiddleware(http_app, rpc, path="/ws/")` adds a WebSocket endpoint to an
existing ASGI application while forwarding its other traffic.

The [framework guide](https://docs.wsrpc.org/quick-start.html) includes runnable examples
for **FastAPI, Starlette, Django, Channels, Quart, Litestar, Falcon, Sanic,
BlackSheep and bare ASGI**, plus Uvicorn and Hypercorn commands. Each
framework example is exercised over real HTTP and WebSocket connections.

The [quick start](https://docs.wsrpc.org/quick-start.html) includes a browser demo.
`wsrpc_asgi.STATIC_DIR` contains the unchanged JavaScript clients and
TypeScript declarations; they can also be obtained from `@wsrpc/client`.

Documentation and its runnable examples are maintained in [wsrpc/docs](https://github.com/wsrpc/docs).

## Development

```shell
uv sync --locked
uv run --locked pytask
uv run --locked --group integrations pytest tests/test_examples.py
```

See [development](https://docs.wsrpc.org/asgi/development.html) for the verification matrix,
free-threaded Python and release workflow. Framework integration dependencies
are test-only; applications install the framework they use.

Licensed under Apache-2.0. The bundled JavaScript client retains its MIT
license. Upstream attribution and license notices are preserved.
