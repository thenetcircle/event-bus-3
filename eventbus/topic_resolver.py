import re
from typing import List, Optional

from loguru import logger

from eventbus.event import Event
from eventbus.model import TopicMapping


class TopicResolver:
    def __init__(self):
        self._index = {}
        self._topic_mappings = None

    # TODO add cache
    def resolve(self, event: Event) -> Optional[str]:
        """Resolve event topic by event title according to the topic mapping"""
        for _, (pattern, topic) in self._index.items():
            if re.match(pattern, event.title):
                return topic
        return None

    async def set_topic_mappings(self, topic_mappings: List[TopicMapping]) -> None:
        logger.info("Updating topic mappings")
        self._topic_mappings = topic_mappings
        self.reindex()
        logger.info("Updated topic mappings")

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
