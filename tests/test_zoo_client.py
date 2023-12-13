import pytest
from loguru import logger

from eventbus import config
from eventbus.aio_zoo_client import AioZooClient


@pytest.mark.asyncio
async def test_aio_zoo_client():
    zoo_client = AioZooClient(
        hosts=config.get().app.zookeeper.hosts,
        timeout=config.get().app.zookeeper.timeout,
    )
    await zoo_client.init()

    test_zoo_path = "/event-bus-3/test_path"

    if not await zoo_client.exists(test_zoo_path):
        await zoo_client.create(test_zoo_path, b"test_path", makepath=True)

    data, stats = await zoo_client.get(test_zoo_path)
    logger.info("init data {}", data.decode("utf-8"))

    async def callback(data, stats):
        logger.info("watched data: {}", data)

    async def callback2(children):
        logger.info("watched children: {}", children)

    await zoo_client.watch_data(test_zoo_path, callback)
    await zoo_client.watch_children(test_zoo_path, callback2)

    await zoo_client.set(test_zoo_path, b"test_path2")
    await zoo_client.create(f"{test_zoo_path}/subpath", b"test_value")

    await zoo_client.delete(test_zoo_path, recursive=True)
    await zoo_client.close()
