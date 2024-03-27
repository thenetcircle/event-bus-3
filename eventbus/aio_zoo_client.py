import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from loguru import logger

from eventbus.zoo_client import ZooClient


class AioZooClient:
    def __init__(self, **kwargs):
        """Asyncio wrapper for KazooClient.

        :param chroot: The chroot path to use for this client.

        :param hosts: Comma-separated list of hosts to connect to
                      (e.g. 127.0.0.1:2181,127.0.0.1:2182,[::1]:2183).
        :param timeout: The longest to wait for a Zookeeper connection.
        :param client_id: A Zookeeper client id, used when
                          re-establishing a prior session connection.
        :param handler: An instance of a class implementing the
                        :class:`~kazoo.interfaces.IHandler` interface
                        for callback handling.
        :param default_acl: A default ACL used on node creation.
        :param auth_data:
            A list of authentication credentials to use for the
            connection. Should be a list of (scheme, credential)
            tuples as :meth:`add_auth` takes.
        :param sasl_options:
            SASL options for the connection, if SASL support is to be used.
            Should be a dict of SASL options passed to the underlying
            `pure-sasl <https://pypi.org/project/pure-sasl>`_ library.

            For example using the DIGEST-MD5 mechnism:

            .. code-block:: python

                sasl_options = {
                    'mechanism': 'DIGEST-MD5',
                    'username': 'myusername',
                    'password': 'mypassword'
                }

            For GSSAPI, using the running process' ticket cache:

            .. code-block:: python

                sasl_options = {
                    'mechanism': 'GSSAPI',
                    'service': 'myzk',                  # optional
                    'principal': 'client@EXAMPLE.COM'   # optional
                }

        :param read_only: Allow connections to read only servers.
        :param randomize_hosts: By default randomize host selection.
        :param connection_retry:
            A :class:`kazoo.retry.KazooRetry` object to use for
            retrying the connection to Zookeeper. Also can be a dict of
            options which will be used for creating one.
        :param command_retry:
            A :class:`kazoo.retry.KazooRetry` object to use for
            the :meth:`KazooClient.retry` method. Also can be a dict of
            options which will be used for creating one.
        :param logger: A custom logger to use instead of the module
            global `log` instance.
        :param keyfile: SSL keyfile to use for authentication
        :param keyfile_password: SSL keyfile password
        :param certfile: SSL certfile to use for authentication
        :param ca: SSL CA file to use for authentication
        :param use_ssl: argument to control whether SSL is used or not
        :param verify_certs: when using SSL, argument to bypass
            certs verification
        """
        self._loop = None
        self._client: Optional[ZooClient] = None
        self._client_params = kwargs
        self._executor: Optional[ThreadPoolExecutor] = None

    @property
    def sync_client(self):
        return self._client

    async def init(self):
        logger.info("Initializing aio zoo client")
        self._loop = asyncio.get_running_loop()
        self._executor = ThreadPoolExecutor(
            max_workers=3, thread_name_prefix="ZooClientExecutor"
        )
        self._client = ZooClient(
            **self._client_params, logger=logging.getLogger("AioZooClient")
        )
        self._client.init()

    async def close(self):
        logger.info("Stopping aio zoo client")
        self._executor.shutdown()
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

    async def watch_children(
        self, path, callback, allow_session_lost: bool = True, send_event: bool = False
    ):
        def sync_callback(*args, **kwargs):
            logger.debug("watched new children from zookeeper path: {}", path)
            asyncio.run_coroutine_threadsafe(callback(*args, **kwargs), self._loop)

        await self._run_in_executor(
            self._client.watch_children,
            path,
            sync_callback,
            allow_session_lost,
            send_event,
        )

    async def _run_in_executor(self, func, *args):
        return await self._loop.run_in_executor(self._executor, func, *args)
