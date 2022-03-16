import anyio
from loguru import logger
from producer import EventProducer
from starlette.applications import Starlette
from starlette.responses import JSONResponse, PlainTextResponse
from starlette.routing import Route
from topic_resolver import TopicResolver

from eventbus import config, config_watcher
from eventbus.errors import EventValidationError, InitError
from eventbus.event import Event, parse_request_body

config_watcher.watch_file_from_environ()
topic_resolver = TopicResolver()
producer = EventProducer()


def startup():
    logger.info("startup")
    topic_resolver.init()
    producer.init()


def shutdown():
    logger.info("startup")
    producer.close()


# TODO teardown


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
                tg.start_soon(handler_event, _event)
    elif len(events) == 1:
        await handler_event(events[0])
    else:
        raise EventValidationError("Invalid format of request body.")

    return PlainTextResponse("ok")


async def handler_event(self, event: Event) -> None:
    if not self.topic_resolver or not self.producer:
        raise InitError("EventHandler must to be inited before use.")

    if event_topic := self.topic_resolver.resolve(event.title):
        msg = await self.producer.produce(event_topic, event)
        print(msg)


def ungzip_request_body(request_body: str) -> str:
    raise NotImplementedError


routes = [
    Route("/", home),
    Route("/config", show_config),
    Route("/new_events", receive_events, methods=["POST"]),
]

app = Starlette(
    debug=config.get().debug,
    routes=routes,
    on_startup=[startup],
    on_shutdown=[shutdown],
)
