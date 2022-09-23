import asyncio
import socket
from typing import List, Union

from confluent_kafka import Message
from loguru import logger
from starlette.applications import Starlette
from starlette.responses import JSONResponse, PlainTextResponse, Response
from starlette.routing import Route

from eventbus import config, config_watcher
from eventbus.errors import EventValidationError, NoMatchedKafkaTopicError
from eventbus.event import Event, parse_request_body
from eventbus.metrics import stats_client
from eventbus.producer import EventProducer
from eventbus.topic_resolver import TopicResolver
from eventbus.utils import setup_logger

config.load_from_environ()
setup_logger()
stats_client.init(config.get())
topic_resolver = TopicResolver()
producer = EventProducer(
    f"app_http_{socket.gethostname()}", config.get().app.producer.use_producers
)


async def startup():
    logger.info("The app is starting up")
    await config_watcher.async_watch_config_file(
        config.get().config_file_path, checking_interval=3
    )
    await topic_resolver.init()
    await producer.init()


async def shutdown():
    logger.info("The app is shutting down")
    await producer.close()


def home(request):
    return PlainTextResponse("running")


def show_config(request):
    """Print current running config"""
    return JSONResponse(config.get().dict())


async def receive_events(request):
    stats_client.incr("producer.request.new")
    request_body = await request.body()

    resp_format = 3
    if "resp_format" in request.query_params:
        resp_format = int(request.query_params["resp_format"])

    max_resp_time = config.get().app.producer.max_response_time  # seconds
    if "max_resp_time" in request.query_params:
        max_resp_time = int(request.query_params["max_resp_time"])

    events = None
    try:
        if "gzip" in request.query_params:
            request_body = _ungzip_request_body(request_body)

        events = parse_request_body(request_body)
        if not events:
            raise EventValidationError("Invalid format of request body.")

        results = await asyncio.wait_for(handler_event(*events), max_resp_time)

        stats_client.incr("producer.request.succ")
        return _create_response([e.id for e in events], results, resp_format)

    except EventValidationError as ex:
        event_ids = [e.id for e in events] if events else ["root"]
        stats_client.incr("producer.request.fail")
        return _create_response(event_ids, [ex for _ in event_ids], resp_format)

    except asyncio.TimeoutError as ex:
        stats_client.incr("producer.request.fail")
        return _create_response(
            [e.id for e in events], [ex for _ in events], resp_format
        )


async def handler_event(*events: Event) -> List[Union[Message, Exception]]:
    tasks = []
    for event in events:
        stats_client.incr("producer.event.new")

        if event_topic := topic_resolver.resolve(event):
            task = asyncio.create_task(producer.produce(event_topic, event))
            tasks.append(task)
        else:
            feature = asyncio.get_running_loop().create_future()
            feature.set_exception(NoMatchedKafkaTopicError)
    return await asyncio.gather(*tasks, return_exceptions=True)


def _create_response(
    event_ids: List[str], results: List[Union[Message, Exception]], resp_format: int
) -> Response:
    succ_events_len = len(list(filter(lambda r: isinstance(r, Message), results)))

    if resp_format == 2:
        if succ_events_len == len(event_ids):
            resp = "ok"
        elif succ_events_len > 0:
            resp = "part_ok"
        else:
            resp = "fail"

        return PlainTextResponse(resp)

    else:
        resp = {}

        if succ_events_len == len(event_ids):
            resp["status"] = "all_succ"
        elif succ_events_len > 0:
            resp["status"] = "part_succ"
        else:
            resp["status"] = "all_fail"

        details = {}
        for i, event_id in enumerate(event_ids):
            result = results[i]
            if not isinstance(result, Message):
                details[event_id] = f"<{type(result).__name__}> {result}"
        if details:
            resp["details"] = details

        return JSONResponse(resp)


def _ungzip_request_body(request_body: str) -> str:
    raise NotImplementedError


routes = [
    Route("/", home),
    Route("/config", show_config),
    Route("/events", receive_events, methods=["POST"]),
]

app = Starlette(
    debug=config.get().app.debug,
    routes=routes,
    on_startup=[startup],
    on_shutdown=[shutdown],
)
