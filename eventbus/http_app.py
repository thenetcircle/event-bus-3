import os

import anyio
from loguru import logger
from starlette.applications import Starlette
from starlette.responses import JSONResponse, PlainTextResponse
from starlette.routing import Route

from eventbus import config, config_watcher
from eventbus.errors import EventValidationError, NamespaceValidationError
from eventbus.event import parse_request_body
from eventbus.event_handler import EventHandler

config_file = (
    os.environ["EVENTBUS_CONFIG"] if "EVENTBUS_CONFIG" in os.environ else "config.yml"
)
config_watcher.watch_file(config_file)
event_handler = EventHandler()


def startup():
    logger.info("startup")
    event_handler.init()


def home(request):
    return PlainTextResponse("running")


def show_config(request):
    """Print current running config"""
    return JSONResponse(config.get().dict())


async def receive_events(request):
    namespace = request.path_params["namespace"]
    validate_namespace(namespace)

    request_body = await request.body()

    if "gzip" in request.query_params:
        request_body = ungzip_request_body(request_body)

    events = parse_request_body(request_body, namespace=namespace)

    if len(events) > 1:
        async with anyio.create_task_group() as tg:
            for _event in events:
                tg.start_soon(event_handler.handler_event, _event)
    elif len(events) == 1:
        await event_handler.handler_event(events[0])
    else:
        raise EventValidationError("Invalid format of request body.")

    return PlainTextResponse("ok")


def ungzip_request_body(request_body: str) -> str:
    raise NotImplementedError


def validate_namespace(namespace) -> None:
    if namespace == "":
        raise NamespaceValidationError("Namespace is required.")
    allowed_namespaces = config.get().allowed_namespaces
    if allowed_namespaces is not None and namespace not in allowed_namespaces:
        raise NamespaceValidationError(
            f"Namespace '{namespace}' is not in allowed_namespaces list {allowed_namespaces}."
        )


routes = [
    Route("/", home),
    Route("/config", show_config),
    Route("/new_events/{namespace}", receive_events, methods=["POST"]),
]

app = Starlette(debug=config.get().debug, routes=routes, on_startup=[startup])
