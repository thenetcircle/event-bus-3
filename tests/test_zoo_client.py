import pytest
from loguru import logger

from eventbus import config
from eventbus.zoo_client import AioZooClient


@pytest.mark.asyncio
async def test_zoo_get():
    zoo_client = AioZooClient()
    await zoo_client.init(**config.get().app.zookeeper.dict())

    test_path = "/event-bus-3/test_path"

    if not await zoo_client.exists(test_path):
        await zoo_client.create(test_path, b"test_path", makepath=True)

    data, stats = await zoo_client.get(test_path)
    logger.info("init data {}", data.decode("utf-8"))

    async def callback(data, stats):
        logger.info("watched data: {}", data)

    async def callback2(children):
        logger.info("watched children: {}", children)

    await zoo_client.watch_data(test_path, callback)
    await zoo_client.watch_children(test_path, callback2)

    await zoo_client.set(test_path, b"test_path2")
    await zoo_client.create(f"{test_path}/subpath", b"test_value")

    await zoo_client.delete(test_path, recursive=True)
    await zoo_client.close()
