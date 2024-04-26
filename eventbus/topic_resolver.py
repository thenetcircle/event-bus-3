import re
from eventbus.zoo_data_parser import ZooDataParser
import json
from pydantic import StrictStr
from eventbus.model import EventBusBaseModel
from typing import List, Optional

from loguru import logger

from eventbus.errors import InitError
from eventbus.event import Event
from eventbus.metrics import stats_client


class TopicMappingEntry(EventBusBaseModel):
    topic: StrictStr
    patterns: List[StrictStr]


class TopicResolver:
    def __init__(self):
        self._index = {}
        self._topic_mappings = None
        self._cache = {}

    async def init_from_zoo(self, zoo_client):
        logger.info("Initializing TopicResolver from Zookeeper")
        await zoo_client.watch_data(
            ZooDataParser.get_topics_path(), self._update_from_zoo
        )
        logger.info("TopicResolver has been initialized")

    async def set_topic_mappings(self, topic_mappings: List[TopicMappingEntry]) -> None:
        logger.info("Updating topic mappings")
        self._topic_mappings = topic_mappings
        self._cache = {}
        self.reindex()
        logger.info("Topic mappings have been updated")

    def resolve(self, event: Event) -> Optional[str]:
        """Resolve event topic by event title according to the topic mapping"""
        if self._topic_mappings is None:
            raise InitError("Topic mappings is empty")
        if event.title in self._cache:
            return self._cache[event.title]
        for _, (pattern, topic) in self._index.items():
            if re.match(pattern, event.title):
                self._cache[event.title] = topic
                return topic
        return None

    def reindex(self) -> None:
        """Index the topic mapping with structure:
        pattern: { (compiled_pattern, topic) ... }"""
        new_index = {}
        for mp in self._topic_mappings:
            for patn in mp.patterns:
                if (
                    patn not in new_index
                ):  # if there are repetitive patterns, they will be abandoned
                    new_index[patn] = (re.compile(patn, re.I), mp.topic)
        self._index = new_index

    async def _update_from_zoo(self, data, stats):
        try:
            stats_client.incr("topic_mapping.update")
            data = data.decode("utf-8")
            if data == "":
                logger.warning("Get empty new topic mappings from Zookeeper")
                return
            logger.info("Get new topic mappings from Zookeeper: {}", data)
            topic_mappings = self.convert_str_to_topic_mapping(data)
            await self.set_topic_mappings(topic_mappings)
        except Exception as ex:
            logger.exception("Update topic mappings failed")

    @staticmethod
    def convert_str_to_topic_mapping(data: str) -> List[TopicMappingEntry]:
        data = json.loads(data)
        return [TopicMappingEntry(**entry) for entry in data]
