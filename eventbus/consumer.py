from typing import Dict, List

from eventbus.config import ConsumerConfig


class KafkaConsumer:
    def __init__(self, kafka_conf: Dict[str, str], consumer_conf: ConsumerConfig):
        self._kafka_conf = kafka_conf
        self._consumer_conf = consumer_conf

    def get_subscribe_topics(self) -> List[str]:
        sub_topics = set()
        for sub_event in self._consumer_conf.subscribe_events:
            sub_topics.add()

    def start(self):
        """
        - get topics by listening events and topic mapping
        - get topic-partitions and offsets from redis
        - start the consumer on the topic-partitions
        - send to sink
        - update offsets on redis, and periodically commit to kafka
        """
