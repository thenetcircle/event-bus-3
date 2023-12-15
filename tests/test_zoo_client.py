import asyncio
from unittest.mock import AsyncMock

import pytest

from eventbus import config
from eventbus.aio_zoo_client import AioZooClient


@pytest.mark.asyncio
async def test_aio_zoo_client():
    zoo_client = AioZooClient(
        hosts=config.get().zookeeper.hosts,
        timeout=config.get().zookeeper.timeout,
    )
    await zoo_client.init()

    test_path = "/event-bus-3/test_path"
    test_data = b"test_data"

    if await zoo_client.exists(test_path):
        await zoo_client.delete(test_path, recursive=True)
    await zoo_client.create(test_path, test_data, makepath=True)

    data, stats = await zoo_client.get(test_path)
    assert data == test_data

    data_change_callback = AsyncMock()
    await zoo_client.watch_data(test_path, data_change_callback)
    await _wait_and_check(data_change_callback, 1, test_data)

    await zoo_client.set(test_path, b"test_data_new")
    await _wait_and_check(data_change_callback, 2, b"test_data_new")

    children_change_callback = AsyncMock()
    await zoo_client.watch_children(test_path, children_change_callback)
    await _wait_and_check(children_change_callback, 1, [])

    await zoo_client.create(f"{test_path}/child1", b"test_child_value")
    await _wait_and_check(children_change_callback, 2, ["child1"])

    await zoo_client.create(f"{test_path}/child2", b"test_child_value")
    await _wait_and_check(children_change_callback, 3, ["child2", "child1"])

    await zoo_client.close()


async def _wait_and_check(mock_callback, call_count, call_args):
    await asyncio.sleep(0.1)  # waiting for callback
    assert mock_callback.call_count == call_count
    assert mock_callback.call_args[0][0] == call_args
