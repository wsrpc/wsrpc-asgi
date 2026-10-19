from http import HTTPStatus

import pytest
from websockets.exceptions import InvalidStatus

from wsrpc_asgi import WebSocketAsync, WSRPCClient


class WebSocketHandler(WebSocketAsync):
    AUTHORIZE = False

    async def authorize(self):
        return self.AUTHORIZE


@pytest.fixture
def handler():
    return WebSocketHandler


async def test_auth_fail(client: WSRPCClient, handler):
    handler.AUTHORIZE = False

    with pytest.raises(InvalidStatus) as e:
        await client.connect()

    assert e.value.response.status_code == HTTPStatus.FORBIDDEN


async def test_auth_ok(client: WSRPCClient, handler):
    handler.AUTHORIZE = True
    await client.connect()
    await client.close()
