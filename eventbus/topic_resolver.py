import re

from eventbus import config
from eventbus.event import Event


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
        namespace: { pattern: { (compiled_pattern, topic) ... }, ... }"""
        new_index = {}
        for mp in self._current_topic_mapping:
            for ns in mp.namespaces:
                if ns not in new_index:
                    new_index[ns] = {}
                for patn in mp.patterns:
                    if (
                        patn not in new_index[ns]
                    ):  # if there are repetitive patterns in one namespace, they will be abandoned
                        new_index[ns][patn] = (re.compile(patn, re.I), mp.topic)
        self._index = new_index

    def resolve(self, event: Event) -> None:
        """Resolve event topic by event title according to the topic mapping"""
        if event.namespace not in self._index:
            return
        for _, (pattern, topic) in self._index[event.namespace].items():
            if re.match(pattern, event.title):
                event.topic = topic
                return
