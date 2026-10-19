from wsrpc_asgi import WebSocketAsync


def create_handler():
    class RPC(WebSocketAsync):
        pass

    async def echo(socket, **params):
        return params

    async def callback(socket, *, value):
        return await socket.proxy.double(value=value)

    RPC.add_route("echo", echo)
    RPC.add_route("callback", callback)
    return RPC
