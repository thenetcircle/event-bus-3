import asyncio
import contextlib
from dataclasses import dataclass
from typing import Any, List

from loguru import logger
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse, Response
from starlette.routing import Route

from eventbus import config
from eventbus.aio_zoo_client import AioZooClient
from eventbus.errors import (
    EventValidationError,
    InvalidArgumentError,
    NoMatchedKafkaTopicError,
)
from eventbus.event import Event, EventStatus, parse_request_body
from eventbus.kafka_producer import KafkaProducer, KafkaProducerParams
from eventbus.metrics import stats_client
from eventbus.topic_resolver import TopicResolver
from eventbus.sink_pool import SinkPool
from eventbus.utils import setup_logger
from eventbus.model import SinkResult

config.load_from_environ()
setup_logger()
stats_client.init(config.get())
topic_resolver = TopicResolver()
zoo_client = AioZooClient(
    hosts=config.get().zookeeper.hosts, timeout=config.get().zookeeper.timeout
)
producer = KafkaProducer(
    KafkaProducerParams(client_args=config.get().default_kafka_params.producer)
)
sink_pool = SinkPool()


@contextlib.asynccontextmanager
async def lifespan(app):
    try:
        logger.info("The app is starting up")

        await zoo_client.init()
        await zoo_client.watch_data(
            config.get().zookeeper.topic_mapping_path, _set_topic_mapping
        )
        await producer.init()
        await sink_pool.init()

        yield

    finally:
        logger.info("The app is shutting down")
        await asyncio.gather(producer.close(), zoo_client.close(), sink_pool.close())


def home(request):
    return PlainTextResponse("running")


def show_config(request):
    """Print current running config"""
    return JSONResponse(config.get().model_dump())


@dataclass
class RequestContext:
    events: List[Event]
    request: Request
    resp_format: str = "plain"
    max_resp_time: float = config.get().app.max_response_time  # seconds


async def main(request):
    stats_client.incr("producer.request.new")

    request_context = RequestContext(request=request, events=[])

    if "resp_format" in request.query_params:
        request_context.resp_format = request.query_params["resp_format"]

    if "max_resp_time" in request.query_params:
        request_context.max_resp_time = float(request.query_params["max_resp_time"])

    request_body = await request.body()
    events = None
    try:
        events = parse_request_body(request_body)
        if not events:
            raise EventValidationError("Invalid format of request body.")
    except EventValidationError as ex:
        event_ids = [e.id for e in events] if events else ["root"]
        stats_client.incr("producer.request.fail")
        return _create_response(
            event_ids, [ex for _ in event_ids], request_context.resp_format
        )

    request_context.events = events

    if "sink" in request.query_params:
        return await send_to_sink(request.query_params["sink"], request_context)
    else:
        return await send_to_kafka(request_context)


async def send_to_sink(sink_id: str, context: RequestContext):
    if sink := sink_pool.get_sink(sink_id):
        events = context.events
        try:
            tasks = asyncio.gather(
                *[sink.send_event(event) for event in events], return_exceptions=True
            )
            results = await asyncio.wait_for(tasks, context.max_resp_time)

            def convert_results(r):
                if isinstance(r, SinkResult):
                    if r.error:
                        return r.error
                    else:
                        return r.event
                else:
                    return r

            results = list(map(convert_results, results))
            return _create_response(
                [e.id for e in events], results, context.resp_format
            )
        except Exception as ex:
            stats_client.incr("producer.request.fail")
            return _create_response(
                [e.id for e in events], [ex for _ in events], context.resp_format
            )
    else:
        return _create_response(
            ["root"],
            [InvalidArgumentError(f"Sink '{sink_id}' not found.")],
            context.resp_format,
        )


async def send_to_kafka(context: RequestContext):
    events = context.events
    try:
        results = await asyncio.wait_for(handler_event(*events), context.max_resp_time)

        stats_client.incr("producer.request.succ")
        return _create_response([e.id for e in events], results, context.resp_format)

    except Exception as ex:
        stats_client.incr("producer.request.fail")
        return _create_response(
            [e.id for e in events], [ex for _ in events], context.resp_format
        )


async def handler_event(*events: Event) -> List[Any]:
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
    event_ids: List[str], results: List[Any], resp_format: str
) -> Response:
    # succ_events_len = len(list(filter(lambda r: isinstance(r, Message), results)))
    succ_events_len = sum(1 for item in results if not isinstance(item, Exception))

    def _get_details():
        details = {}
        for i, event_id in enumerate(event_ids):
            result = results[i]
            if isinstance(result, Exception):
                details[event_id] = f"<{type(result).__name__}> {result}"
        return (
            details
            if resp_format == "json"
            else "\n".join(f"{k}: {v}" for k, v in details.items())
        )

    if resp_format == "json":
        resp = {}
        if succ_events_len == len(event_ids):
            resp["status"] = "all_succ"
        elif succ_events_len > 0:
            resp["status"] = "part_succ"
        else:
            resp["status"] = "all_fail"

        if d := _get_details():
            resp["details"] = d

        return JSONResponse(resp)

    else:
        if succ_events_len == len(event_ids):
            resp = "ok"
        elif succ_events_len > 0:
            resp = "part_ok" + "\n" + _get_details()
        else:
            resp = "fail" + "\n" + _get_details()

        return PlainTextResponse(resp)


async def _set_topic_mapping(data, stats):
    try:
        data = data.decode("utf-8")
        if data == "":
            logger.warning("topic mapping is empty")
            return
        logger.info("get new topic mapping data from zookeeper: {}", data)
        topic_mapping = TopicResolver.convert_str_to_topic_mapping(data)
        await topic_resolver.set_topic_mapping(topic_mapping)
    except Exception as ex:
        logger.error("update topic mapping error: {}", ex)


routes = [
    Route("/", home),
    Route("/config", show_config),
    Route("/main", main, methods=["POST"]),
]

app = Starlette(debug=config.get().app.debug, routes=routes, lifespan=lifespan)
