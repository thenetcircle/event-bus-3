import re
from typing import Optional, Set

from eventbus import config, signals
from eventbus.event import Event


class TopicResolver:
    def __init__(self):
        self._index = {}
        self._current_topic_mapping = config.get().topic_mapping
        self.reindex()
        signals.CONFIG_TOPIC_MAPPING_CHANGED.connect(self._handle_topic_mapping_signal)

    async def init(self) -> None:
        pass

    # TODO add cache
    def resolve(self, event: Event) -> Optional[str]:
        """Resolve event topic by event title according to the topic mapping"""
        for _, (pattern, topic) in self._index.items():
            if re.match(pattern, event.title):
                return topic
        return None

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

    def _handle_topic_mapping_signal(self, sender) -> None:
        """Subscribing the topic mapping changes signal, and update related index accordingly."""
        self._current_topic_mapping = config.get().topic_mapping
        self.reindex()
