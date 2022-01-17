import re
from typing import Optional

from eventbus import config


class TopicResolver:
    def __init__(self):
        self._current_topic_mapping = config.get().topic_mapping
        self._index = {}
        self.reindex()
        config.add_subscriber(self.topic_mapping_subscriber)

    def topic_mapping_subscriber(self) -> None:
        """Subscribing the topic mapping changes, and update related index accordingly."""
        new_topic_mapping = config.get().topic_mapping
        if new_topic_mapping != self._current_topic_mapping:
            self.reindex()

    def reindex(self) -> None:
        """Index the topic mapping with structure:
        pattern: { (compiled_pattern, topic) ... }"""
        new_index = {}
        for mp in self._current_topic_mapping:
            for patn in mp.patterns:
                if (
                    patn not in new_index
                ):  # if there are repetitive patterns, they will be abandoned
                    new_index[patn] = (re.compile(patn, re.I), mp.topic)
        self._index = new_index

    # TODO add cache
    def resolve(self, event_title: str) -> Optional[str]:
        """Resolve event topic by event title according to the topic mapping"""
        for _, (pattern, topic) in self._index.items():
            if re.match(pattern, event_title):
                return topic
        return None
