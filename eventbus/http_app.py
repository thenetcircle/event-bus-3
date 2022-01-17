import anyio
from loguru import logger
from starlette.applications import Starlette
from starlette.responses import JSONResponse, PlainTextResponse
from starlette.routing import Route

from eventbus import config, config_watcher
from eventbus.errors import EventValidationError
from eventbus.event import parse_request_body
from eventbus.event_handler import EventHandler

config_watcher.watch_file_from_environ()
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
    request_body = await request.body()

    if "gzip" in request.query_params:
        request_body = ungzip_request_body(request_body)

    events = parse_request_body(request_body)

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


routes = [
    Route("/", home),
    Route("/config", show_config),
    Route("/new_events", receive_events, methods=["POST"]),
]

app = Starlette(debug=config.get().debug, routes=routes, on_startup=[startup])
