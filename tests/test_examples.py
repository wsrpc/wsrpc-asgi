import importlib

import httpx
import pytest

from tests.conftest import serve_app
from wsrpc_asgi import WSRPCClient

FRAMEWORKS = [
    ("bare", None),
    ("fastapi", "fastapi"),
    ("starlette", "starlette"),
    ("django", "django"),
    ("channels", "channels"),
    ("quart", "quart"),
    ("litestar", "litestar"),
    ("falcon", "falcon"),
    ("sanic", "sanic"),
    ("blacksheep", "blacksheep"),
]


@pytest.mark.parametrize("name,dependency", FRAMEWORKS)
async def test_framework_integrations(name, dependency):
    if dependency:
        pytest.importorskip(dependency)
    module = importlib.import_module(f"tests.frameworks.{name}_app")
    async with serve_app(module.app) as url:
        async with WSRPCClient(url + "/ws/", timeout=3) as client:
            client.add_route("double", lambda socket, value: value * 2)
            assert await client.proxy.echo(text="Привет", value=42) == {
                "text": "Привет",
                "value": 42,
            }
            assert await client.proxy.callback(value=21) == 42
        async with httpx.AsyncClient(
            base_url=url.replace("ws:", "http:")
        ) as http:
            response = await http.get("/health")
            if name == "bare":
                assert response.status_code == 404
            else:
                assert response.status_code == 200
                assert response.json() == {"ok": True}
