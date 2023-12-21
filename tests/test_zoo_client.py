import asyncio
import time
from unittest.mock import AsyncMock, Mock

import pytest

from eventbus import config
from eventbus.zoo_client import ZooClient
from eventbus.aio_zoo_client import AioZooClient


@pytest.fixture
def zoo_client():
    zoo_client = ZooClient(
        hosts=config.get().zookeeper.hosts,
        timeout=config.get().zookeeper.timeout,
    )
    zoo_client.init()
    yield zoo_client
    zoo_client.close()


@pytest.fixture
async def aio_zoo_client(zoo_client: ZooClient):
    aio_zoo_client = AioZooClient(
        hosts=config.get().zookeeper.hosts,
        timeout=config.get().zookeeper.timeout,
    )
    await aio_zoo_client.init()
    yield aio_zoo_client
    await aio_zoo_client.close()


@pytest.mark.asyncio
async def test_aio_zoo_client(aio_zoo_client: AioZooClient):
    test_path = "/event-bus-3/test/client/aio_zoo_client"
    test_data = b"test_data"

    if await aio_zoo_client.exists(test_path):
        await aio_zoo_client.delete(test_path, recursive=True)
    await aio_zoo_client.create(test_path, test_data, makepath=True)

    data, stats = await aio_zoo_client.get(test_path)
    assert data == test_data

    data_change_callback = AsyncMock()
    await aio_zoo_client.watch_data(test_path, data_change_callback)
    await _aio_wait_and_check(data_change_callback, 1, test_data)

    await aio_zoo_client.set(test_path, b"test_data_new")
    await _aio_wait_and_check(data_change_callback, 2, b"test_data_new")

    children_change_callback = AsyncMock()
    await aio_zoo_client.watch_children(
        test_path, children_change_callback, send_event=True
    )
    await _aio_wait_and_check(children_change_callback, 1, [])

    await aio_zoo_client.create(f"{test_path}/child1", b"test_child_value")
    await _aio_wait_and_check(children_change_callback, 2, ["child1"])

    await aio_zoo_client.create(f"{test_path}/child2", b"test_child_value")
    await _aio_wait_and_check(children_change_callback, 3, ["child2", "child1"])

    await aio_zoo_client.set(f"{test_path}/child2", b"test_child_value2")
    await _aio_wait_and_check(children_change_callback, 3, ["child2", "child1"])

    await aio_zoo_client.create(f"{test_path}/child2/subchild", b"test_child_value")
    await _aio_wait_and_check(children_change_callback, 3, ["child2", "child1"])

    await aio_zoo_client.set(f"{test_path}/child2/subchild", b"test_child_value2")
    await _aio_wait_and_check(children_change_callback, 3, ["child2", "child1"])

    await aio_zoo_client.delete(f"{test_path}/child2", recursive=True)
    await _aio_wait_and_check(children_change_callback, 4, ["child1"])


def test_zoo_client(zoo_client: ZooClient):
    test_path = "/event-bus-3/test/client/zoo_client"
    test_data = b"test_data"

    if zoo_client.exists(test_path):
        zoo_client.delete(test_path, recursive=True)

    zoo_client.create(test_path, test_data, makepath=True)

    data, stats = zoo_client.get(test_path)
    assert data == test_data

    data_change_callback = Mock()
    zoo_client.watch_data(test_path, data_change_callback)
    _wait_and_check(data_change_callback, 1, test_data)

    zoo_client.set(test_path, b"test_data_new")
    _wait_and_check(data_change_callback, 2, b"test_data_new")


async def _aio_wait_and_check(mock_callback, call_count, call_args):
    await asyncio.sleep(0.2)  # waiting for callback
    assert mock_callback.call_count == call_count
    assert mock_callback.call_args[0][0] == call_args


def _wait_and_check(mock_callback, call_count, call_args):
    time.sleep(0.5)
    assert mock_callback.call_count == call_count
    assert mock_callback.call_args[0][0] == call_args
    print(mock_callback.call_args[0][1])
    print(mock_callback.call_args[0][2])
