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
        sink=HttpSinkConfig(
            url="/", method=HttpSinkMethod.POST, timeout=0.2, max_retry_times=3
        ),
    )
    yield KafkaConsumer(config=consumer_conf, topics=["test_topic1"])
    # mocker.patch.object(consumer, "_fetch_events", autospec=True)


@pytest.mark.asyncio
async def test_send_one_event(aiohttp_client, consumer: KafkaConsumer):
    retry2_req_times = 0
    timeout_req_times = 0
    ce_req_times = 0

    async def mock_server(request):
        try:
            req_body = await request.text()
            if req_body == "ok":
                return web.Response(text="ok")
            elif req_body == "retry":
                return web.Response(text="retry")
            elif req_body == "retry2":
                nonlocal retry2_req_times
                retry2_req_times += 1
                if retry2_req_times < 3:
                    return web.Response(text="retry")
                else:
                    return web.Response(text="ok")
            elif req_body == "unexpected_resp":
                return web.Response(text="something else")
            elif req_body == "timeout":
                nonlocal timeout_req_times
                timeout_req_times += 1
                if timeout_req_times < 3:
                    await asyncio.sleep(0.2)
                return web.Response(text="ok")
            elif req_body == "non-200":
                return web.Response(text="non-200", status=500)
            elif req_body == "connection-error":
                nonlocal ce_req_times
                ce_req_times += 1
                if ce_req_times < 3:
                    return
                else:
                    return web.Response(text="ok")
        except Exception as ex:
            logger.error(ex)

    app = web.Application()
    app.router.add_post("/", mock_server)
    client = await aiohttp_client(app)

    ok_event = create_kafka_event_from_dict({"payload": b"ok"})
    assert (await consumer._send_one_event(client, ok_event)) == SendEventResult.DONE

    retry_event = create_kafka_event_from_dict({"payload": b"retry"})
    assert (
        await consumer._send_one_event(client, retry_event)
    ) == SendEventResult.RETRY_LATER

    ok_event = create_kafka_event_from_dict({"payload": b"retry2"})
    assert (await consumer._send_one_event(client, ok_event)) == SendEventResult.DONE

    retry_event = create_kafka_event_from_dict({"payload": b"unexpected_resp"})
    assert (
        await consumer._send_one_event(client, retry_event)
    ) == SendEventResult.RETRY_LATER

    retry_event = create_kafka_event_from_dict({"payload": b"timeout"})
    assert (await consumer._send_one_event(client, retry_event)) == SendEventResult.DONE

    retry_event = create_kafka_event_from_dict({"payload": b"non-200"})
    assert (
        await consumer._send_one_event(client, retry_event)
    ) == SendEventResult.RETRY_LATER

    retry_event = create_kafka_event_from_dict({"payload": b"connection-error"})
    assert (await consumer._send_one_event(client, retry_event)) == SendEventResult.DONE


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
