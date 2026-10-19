from .common import create_handler

rpc = create_handler().as_asgi(path="/ws/")
app = rpc
