from eventbus import config


from typing import Dict, Optional
from eventbus.model import AbsSink
import asyncio
from eventbus.factories import SinkFactory
from loguru import logger


class SinkPool:
    def __init__(self):
        self._sinks: Dict[str, AbsSink] = {}
        for sink_id, sink_config in config.get().predefined_sinks.items():
            sink = SinkFactory.create_sink(sink_config.type, sink_config.params)
            self._sinks[sink_id] = sink

    async def init(self):
        logger.info("Initing SinkPool")
        for sink_id in list(self._sinks.keys()):
            try:
                logger.info("Initing sink {}", sink_id)
                await self._sinks[sink_id].init()
            except Exception as ex:
                del self._sinks[sink_id]
                logger.error("Failed to init sink {}, caused by {}", sink_id, ex)
        logger.info("Inited SinkPool")

    async def close(self):
        logger.info("Closing SinkPool")
        await asyncio.gather(
            *[sink.close() for _, sink in self._sinks.items()],
        )
        logger.info("Closed SinkPool")

    def get_sink(self, sink_id: str) -> Optional[AbsSink]:
        return self._sinks.get(sink_id)
