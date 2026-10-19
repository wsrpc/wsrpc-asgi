import django
from django.conf import settings
from django.core.asgi import get_asgi_application
from django.http import JsonResponse
from django.urls import path

from wsrpc_asgi import RPCMiddleware

from .common import create_handler

# In an existing project keep DJANGO_SETTINGS_MODULE and its settings.
if not settings.configured:
    settings.configure(
        DEBUG=True,
        SECRET_KEY="local-example-only",
        ROOT_URLCONF=__name__,
        ALLOWED_HOSTS=["*"],
    )
django.setup()


def health(request):
    return JsonResponse({"ok": True})


urlpatterns = [path("health", health)]
http_app = get_asgi_application()
rpc = create_handler().as_asgi()
app = RPCMiddleware(http_app, rpc, path="/ws/")
