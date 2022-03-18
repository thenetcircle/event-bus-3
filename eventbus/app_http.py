import asyncio
from typing import List

from loguru import logger
from starlette.applications import Starlette
from starlette.responses import JSONResponse, PlainTextResponse, Response
from starlette.routing import Route

from eventbus import config, config_watcher
from eventbus.errors import EventValidationError, NoMatchedKafkaTopicError
from eventbus.event import Event, parse_request_body
from eventbus.producer import EventProducer
from eventbus.topic_resolver import TopicResolver

config_watcher.watch_file_from_environ()
topic_resolver = TopicResolver()
producer = EventProducer("app_http", config.get().http_app.producers)


def startup():
    logger.info("The app is starting up")
    topic_resolver.init()
    producer.init()


async def shutdown():
    logger.info("The app is shutting down")
    await producer.close()


def home(request):
    return PlainTextResponse("running")


def show_config(request):
    """Print current running config"""
    return JSONResponse(config.get().dict())


async def receive_events(request):
    request_body = await request.body()

    if "gzip" in request.query_params:
        request_body = _ungzip_request_body(request_body)

    resp_format = 3
    if "resp_format" in request.query_params:
        resp_format = int(request.query_params["resp_format"])

    events = parse_request_body(request_body)
    if not events:
        raise EventValidationError("Invalid format of request body.")
    results = await handler_event(*events)
    return _create_response(events, results, resp_format)


# TODO test timeout
async def handler_event(*events: Event) -> List[bool]:
    tasks = []
    for event in events:
        if event_topic := topic_resolver.resolve(event):
            task = asyncio.create_task(producer.produce(event_topic, event))
            tasks.append(task)
        else:
            feature = asyncio.get_running_loop().create_future()
            feature.set_exception(NoMatchedKafkaTopicError)
    return await asyncio.gather(*tasks, return_exceptions=True)


def _create_response(
    events: List[Event], results: List[bool], resp_format: int
) -> Response:
    succ_events_len = len(list(filter(lambda r: r == True, results)))

    if resp_format == 2:
        if succ_events_len == len(events):
            resp = "ok"
        elif succ_events_len > 0:
            resp = "part_ok"
        else:
            resp = "fail"

        return PlainTextResponse(resp)

    else:
        resp = {}

        if succ_events_len == len(events):
            resp["status"] = "all_succ"
        elif succ_events_len > 0:
            resp["status"] = "part_succ"
        else:
            resp["status"] = "all_fail"

        details = {}
        for i, event in enumerate(events):
            result = results[i]
            if result != True:
                details[event.id] = f"<{type(result).__name__}> {result}"
        if details:
            resp["details"] = details

        return JSONResponse(resp)


def _ungzip_request_body(request_body: str) -> str:
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
