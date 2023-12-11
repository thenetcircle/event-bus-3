import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Optional

from kazoo.client import KazooClient
from loguru import logger

from eventbus import config


class ZooClient:
    def __init__(self):
        self._loop = None
        self._zk: Optional[KazooClient] = None
        self._executor: Optional[ThreadPoolExecutor] = None

    async def init(self):
        logger.info("init zoo client")
        self._loop = asyncio.get_running_loop()
        self._zk = KazooClient(
            hosts=config.get().app.zookeeper.hosts,
            timeout=config.get().app.zookeeper.timeout,
            logger=logging.getLogger("KazooClient"),
        )
        self._zk.start()
        self._executor = ThreadPoolExecutor(
            max_workers=3, thread_name_prefix="ZooClientExecutor"
        )

    async def close(self):
        logger.info("stop zoo client")
        self._zk.stop()

    async def create(
        self,
        path,
        value: bytes = b"",
        acl: Any = None,
        ephemeral: bool = False,
        sequence: bool = False,
        makepath: bool = False,
        include_data: bool = False,
    ):
        return await self._run_in_executor(
            self._zk.create,
            path,
            value,
            acl,
            ephemeral,
            sequence,
            makepath,
            include_data,
        )

    async def ensure_path(self, path):
        return await self._run_in_executor(self._zk.ensure_path, path)

    async def exists(self, path):
        return await self._run_in_executor(self._zk.exists, path)

    async def get(self, path):
        return await self._run_in_executor(self._zk.get, path)

    async def get_children(self, path, include_data=False):
        return await self._run_in_executor(
            self._zk.get_children, path, None, include_data
        )

    async def set(self, path, value):
        return await self._run_in_executor(self._zk.set, path, value)

    async def delete(self, path, version=-1, recursive=False):
        return await self._run_in_executor(self._zk.delete, path, version, recursive)

    async def watch_data(self, path, callback):
        logger.info("start watching zookeeper's data from path: {}", path)

        def sync_callback(*args, **kwargs):
            logger.info("watched new data from zookeeper path: {}", path)
            asyncio.run_coroutine_threadsafe(callback(*args, **kwargs), self._loop)

        await self._run_in_executor(self._zk.DataWatch, path, sync_callback)

    async def watch_children(self, path, callback):
        logger.info("start watching zookeeper's children from path: {}", path)

        def sync_callback(*args, **kwargs):
            logger.info("watched new children from zookeeper path: {}", path)
            asyncio.run_coroutine_threadsafe(callback(*args, **kwargs), self._loop)

        await self._run_in_executor(self._zk.ChildrenWatch, path, sync_callback)

    async def _run_in_executor(self, func, *args):
        return await self._loop.run_in_executor(self._executor, func, *args)
