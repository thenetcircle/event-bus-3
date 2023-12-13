import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from loguru import logger

from eventbus.zoo_client import ZooClient


class AioZooClient:
    def __init__(self, **kwargs):
        self._loop = None
        self._client: Optional[ZooClient] = None
        self._client_params = kwargs
        self._executor: Optional[ThreadPoolExecutor] = None

    @property
    def sync_client(self):
        return self._client

    async def init(self):
        logger.info("init aio zoo client")
        self._loop = asyncio.get_running_loop()
        self._executor = ThreadPoolExecutor(
            max_workers=3, thread_name_prefix="ZooClientExecutor"
        )
        self._client = ZooClient(
            **self._client_params, logger=logging.getLogger("AioZooClient")
        )
        self._client.init()

    async def close(self):
        logger.info("stop zoo client")
        self._client.close()

    async def create(
        self,
        path,
        value: bytes = b"",
        ephemeral: bool = False,
        sequence: bool = False,
        makepath: bool = False,
        include_data: bool = False,
    ):
        return await self._run_in_executor(
            self._client.create,
            path,
            value,
            ephemeral,
            sequence,
            makepath,
            include_data,
        )

    async def ensure_path(self, path):
        return await self._run_in_executor(self._client.ensure_path, path)

    async def exists(self, path):
        return await self._run_in_executor(self._client.exists, path)

    async def get(self, path):
        return await self._run_in_executor(self._client.get, path)

    async def get_children(self, path, include_data: bool = False):
        return await self._run_in_executor(
            self._client.get_children, path, None, include_data
        )

    async def set(self, path, value, version: int = -1):
        return await self._run_in_executor(self._client.set, path, value, version)

    async def delete(self, path, version: int = -1, recursive: bool = False):
        return await self._run_in_executor(
            self._client.delete, path, version, recursive
        )

    async def watch_data(self, path, callback):
        def sync_callback(*args, **kwargs):
            logger.debug("watched new data from zookeeper path: {}", path)
            asyncio.run_coroutine_threadsafe(callback(*args, **kwargs), self._loop)

        await self._run_in_executor(self._client.watch_data, path, sync_callback)

    async def watch_children(self, path, callback):
        def sync_callback(*args, **kwargs):
            logger.info("watched new children from zookeeper path: {}", path)
            asyncio.run_coroutine_threadsafe(callback(*args, **kwargs), self._loop)

        await self._run_in_executor(self._client.watch_children, path, sync_callback)

    async def _run_in_executor(self, func, *args):
        return await self._loop.run_in_executor(self._executor, func, *args)
