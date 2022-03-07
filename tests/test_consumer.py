import asyncio

import janus
import pytest
from aiohttp import web
from loguru import logger
from pytest_mock import MockFixture

from eventbus.config import ConsumerConfig, HttpSinkConfig, HttpSinkMethod
from eventbus.consumer import KafkaConsumer, SendEventResult
from tests.utils import create_kafka_event_from_dict


@pytest.fixture
def consumer():
    consumer_conf = ConsumerConfig(
        id="test_consumer",
        subscribe_events=["test_event1"],
        kafka_config={},
        sink=HttpSinkConfig(url="/", method=HttpSinkMethod.POST),
    )
    yield KafkaConsumer(config=consumer_conf, topics=["test_topic1"])
    # mocker.patch.object(consumer, "_fetch_events", autospec=True)


@pytest.mark.asyncio
async def test_send_one_event(
    aiohttp_client, mocker: MockFixture, consumer: KafkaConsumer
):
    async def mock_server(request):
        try:
            request_body = await request.text()
            return web.Response(text=request_body)
        except Exception as ex:
            logger.error(ex)

    app = web.Application()
    app.router.add_post("/", mock_server)
    client = await aiohttp_client(app)

    test_event = create_kafka_event_from_dict({"payload": b"normal"})
    assert (
        await consumer._send_one_event(client, test_event)
    ) == SendEventResult.RETRY_LATER


@pytest.mark.asyncio
async def test_send_events(mocker: MockFixture, consumer):

    test_event = create_kafka_event_from_dict({})

    send_queue = janus.Queue(maxsize=100)
    commit_queue = janus.Queue(maxsize=100)

    # test logic:
    # put bunch of events into send_queue,

    # spy_send_one_event = mocker.spy(consumer, "_send_one_event")
    _send_one_event = mocker.patch.object(consumer, "_send_one_event")

    send_queue.sync_q.put(test_event)
    send_queue.sync_q.put(test_event)
    send_queue.sync_q.put(test_event)

    try:
        await asyncio.wait_for(
            consumer._wait_and_deliver_events(send_queue.async_q, commit_queue.async_q),
            1,
        )
    except asyncio.TimeoutError:
        pass

    # spy_send_one_event.assert_called_once()
    assert _send_one_event.call_count == 3
