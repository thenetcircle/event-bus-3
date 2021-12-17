import os

from starlette.applications import Starlette
from starlette.responses import JSONResponse, PlainTextResponse
from starlette.routing import Route

from eventbus import config, config_watcher

config_file = (
    os.environ["EVENTBUS_CONFIG"] if "EVENTBUS_CONFIG" in os.environ else "config.yml"
)
config_watcher.start_watching(config_file)


def home(request):
    return PlainTextResponse("running")


def show_config(request):
    """Print current running config"""
    return JSONResponse(config.get().dict())


async def receive_events(request):
    namespace = request.path_params["namespace"]
    if namespace == "":
        raise ValueError("Namespace is required.")
    allowed_namespaces = config.get().allowed_namespaces
    if allowed_namespaces is not None and namespace not in allowed_namespaces:
        raise ValueError(
            f"Namespace '{namespace}' is not in allowed_namespaces list {allowed_namespaces}."
        )

    return "welcome"


def startup():
    print("startup")


routes = [
    Route("/", home),
    Route("/config", show_config),
    Route("/new_event/{namespace}", receive_events),
]

app = Starlette(debug=config.get().debug, routes=routes, on_startup=[startup])
