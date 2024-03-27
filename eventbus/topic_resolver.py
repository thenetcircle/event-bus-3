import re
import json
from pydantic import StrictStr
from eventbus.model import EventBusBaseModel
from typing import List, Optional

from loguru import logger

from eventbus.errors import InitError
from eventbus.event import Event


class TopicMappingEntry(EventBusBaseModel):
    topic: StrictStr
    patterns: List[StrictStr]


class TopicResolver:
    def __init__(self):
        self._index = {}
        self._topic_mapping = None
        self._cache = {}

    async def set_topic_mapping(self, topic_mapping: List[TopicMappingEntry]) -> None:
        logger.info("Updating topic mappings")
        self._topic_mapping = topic_mapping
        self._cache = {}
        self.reindex()
        logger.info("Topic mappings have been updated")

    def resolve(self, event: Event) -> Optional[str]:
        """Resolve event topic by event title according to the topic mapping"""
        if self._topic_mapping is None:
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
        for mp in self._topic_mapping:
            for patn in mp.patterns:
                if (
                    patn not in new_index
                ):  # if there are repetitive patterns, they will be abandoned
                    new_index[patn] = (re.compile(patn, re.I), mp.topic)
        self._index = new_index

    @staticmethod
    def convert_str_to_topic_mapping(json_data: str) -> List[TopicMappingEntry]:
        json_list = json.loads(json_data)
        return [TopicMappingEntry(**m) for m in json_list]
