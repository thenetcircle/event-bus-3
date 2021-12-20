from eventbus import config
from eventbus.event import Event


class TopicResolver:
    def __init__(self):
        self._current_topic_mapping = config.get().topic_mapping
        self.reindex()
        config.add_subscriber(self.topic_mapping_subscriber)

    def topic_mapping_subscriber(self) -> None:
        new_topic_mapping = config.get().topic_mapping
        if new_topic_mapping != self._current_topic_mapping:
            self.reindex()

    def reindex(self) -> None:
        pass

    def resolve(self, event: Event) -> None:
        pass
