from channels.routing import ProtocolTypeRouter, URLRouter
from django.urls import path

from .common import create_handler
from .django_app import http_app

rpc = create_handler().as_asgi()
app = ProtocolTypeRouter(
    {
        "http": http_app,
        "websocket": URLRouter([path("ws/", rpc)]),
    }
)
