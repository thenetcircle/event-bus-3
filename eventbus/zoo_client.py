import logging
from typing import Any, Optional

from kazoo.client import KazooClient
from loguru import logger


class ZooClient:
    def __init__(self, **kwargs):
        """Wrapper for KazooClient.

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
        self._kazoo_client: Optional[KazooClient] = None
        self._client_params = kwargs

    def init(self):
        logger.info("Initializing zoo client")
        if "logger" not in self._client_params:
            self._client_params["logger"] = logging.getLogger("ZooClient")
        self._kazoo_client = KazooClient(**self._client_params)
        self._kazoo_client.start()

    def close(self):
        logger.info("Stopping zoo client")
        self._kazoo_client.stop()

    def create(
        self,
        path,
        value: bytes = b"",
        ephemeral: bool = False,
        sequence: bool = False,
        makepath: bool = False,
        include_data: bool = False,
    ):
        return self._kazoo_client.create(
            path,
            value,
            ephemeral=ephemeral,
            sequence=sequence,
            makepath=makepath,
            include_data=include_data,
        )

    def ensure_path(self, path):
        return self._kazoo_client.ensure_path(path)

    def exists(self, path, watch: Any = None):
        return self._kazoo_client.exists(path, watch)

    def get(self, path, watch: Any = None):
        return self._kazoo_client.get(path, watch)

    def get_children(self, path, watch: Any = None, include_data: bool = False):
        return self._kazoo_client.get_children(path, watch, include_data)

    def set(self, path, value, version: int = -1):
        return self._kazoo_client.set(path, value, version)

    def delete(self, path, version: int = -1, recursive: bool = False):
        return self._kazoo_client.delete(path, version, recursive)

    def watch_data(self, path, callback, *args, **kwargs):
        logger.info("Start watching zookeeper's data from path: {}", path)
        return self._kazoo_client.DataWatch(path, callback, *args, **kwargs)

    def watch_children(
        self, path, callback, allow_session_lost: bool = True, send_event: bool = False
    ):
        logger.info("Start watching zookeeper's children from path: {}", path)
        return self._kazoo_client.ChildrenWatch(
            path, callback, allow_session_lost=allow_session_lost, send_event=send_event
        )
