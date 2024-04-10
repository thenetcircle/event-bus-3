import asyncio
import json
from typing import Dict, Optional

from loguru import logger

from eventbus import config
from eventbus.factories import SinkFactory
from eventbus.metrics import stats_client
from eventbus.model import AbsSink
from eventbus.zoo_data_parser import ZooDataParser


class SinkPool:
    def __init__(self):
        self._sinks: Dict[str, AbsSink] = {}

    async def init_from_zoo(self, zoo_client):
        logger.info("Initializing SinkPool from Zookeeper")
        await zoo_client.watch_data(
            ZooDataParser.get_sinks_path(), self._update_from_zoo
        )
        logger.info("SinkPool has been initialized")

    async def init_from_config(self):
        logger.info("Initializing SinkPool from Config")
        for sink_id, sink_config in config.get().predefined_sinks.items():
            sink = SinkFactory.create_sink(sink_config.type, sink_config.params)
            self._sinks[sink_id] = sink

        for sink_id in list(self._sinks.keys()):
            try:
                logger.info("Initializing sink {}", sink_id)
                await self._sinks[sink_id].init()
            except Exception as ex:
                del self._sinks[sink_id]
                logger.error("Failed to init sink {}, caused by {}", sink_id, ex)
        logger.info("SinkPool has been initialized")

    async def close(self):
        logger.info("Closing SinkPool")
        await asyncio.gather(
            *[sink.close() for _, sink in self._sinks.items()],
        )
        logger.info("SinkPool has been closed")

    def get_sink(self, sink_id: str) -> Optional[AbsSink]:
        return self._sinks.get(sink_id)

    async def _update_from_zoo(self, data, stats):
        try:
            stats_client.incr("producer.sinks.update")
            if data is None:
                raise ValueError("Get none new sinks config from Zookeeper")

            data = data.decode("utf-8")
            if data == "":
                logger.warning("Get empty new sinks config from Zookeeper")
                return

            logger.info("Get new sinks config from zookeeper: {}", data)
            data = json.loads(data)
            for sink_id, sink_config in data.items():
                sink_type, sink_params = ZooDataParser.parse_sink_params(sink_config)
                if sink_id in self._sinks:
                    SinkFactory.update_sink_params(self._sinks[sink_id], sink_params)
                else:
                    try:
                        logger.info('Initializing a new sink: "{}"', sink_id)
                        sink = SinkFactory.create_sink(sink_type, sink_params)
                        await sink.init()
                        self._sinks[sink_id] = sink
                    except Exception as ex:
                        logger.error(
                            "Failed to initialize sink {}, caused by {}", sink_id, ex
                        )
        except Exception as ex:
            logger.error("Update new sinks config failed with error: {}", ex)
