import asyncio

import pytest
from aiohttp import web
from loguru import logger

from eventbus.config import ConsumerConfig, HttpSinkConfig, HttpSinkMethod
from eventbus.consumer import ProcessStatus
from eventbus.sink import HttpSink
from tests.utils import create_kafka_event_from_dict


@pytest.mark.asyncio
async def test_httpsink_send_event(aiohttp_client):
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

    sink = HttpSink(
        ConsumerConfig(
            id="test_consumer",
            subscribe_events=["test_event1"],
            kafka_config={},
            sink=HttpSinkConfig(
                url="/", method=HttpSinkMethod.POST, timeout=0.2, max_retry_times=3
            ),
        )
    )
    sink._client = client

    ok_event = create_kafka_event_from_dict({"payload": b"ok"})
    assert (await sink.send_event(ok_event))[1] == ProcessStatus.DONE

    retry_event = create_kafka_event_from_dict({"payload": b"retry"})
    assert (await sink.send_event(retry_event))[1] == ProcessStatus.RETRY_LATER

    ok_event = create_kafka_event_from_dict({"payload": b"retry2"})
    assert (await sink.send_event(ok_event))[1] == ProcessStatus.DONE

    retry_event = create_kafka_event_from_dict({"payload": b"unexpected_resp"})
    assert (await sink.send_event(retry_event))[1] == ProcessStatus.RETRY_LATER

    retry_event = create_kafka_event_from_dict({"payload": b"timeout"})
    assert (await sink.send_event(retry_event))[1] == ProcessStatus.DONE

    retry_event = create_kafka_event_from_dict({"payload": b"non-200"})
    assert (await sink.send_event(retry_event))[1] == ProcessStatus.RETRY_LATER

    retry_event = create_kafka_event_from_dict({"payload": b"connection-error"})
    assert (await sink.send_event(retry_event))[1] == ProcessStatus.DONE

    sink2 = HttpSink(
        ConsumerConfig(
            id="test_consumer2",
            subscribe_events=["test_event1"],
            kafka_config={},
            sink=HttpSinkConfig(
                url="/unknown",
                method=HttpSinkMethod.POST,
                timeout=0.2,
                max_retry_times=3,
            ),
        )
    )
    sink2._client = client
    assert (await sink2.send_event(ok_event))[1] == ProcessStatus.RETRY_LATER
