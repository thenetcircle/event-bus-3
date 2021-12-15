import asyncio
from threading import Thread

from confluent_kafka import KafkaException, Producer

from eventbus import config


class KafkaProducer:
    """
    KafkaProducer based on Asyncio, use another thread to poll and send result to event loop in current thread.

    modified from https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/asyncio_example.py
    the description of the implementation: https://www.confluent.io/blog/kafka-python-asyncio-integration/
    """

    def __init__(self):
        self._loop = asyncio.get_event_loop()
        self._main_producer = self._init_main_producer()
        self._cancelled = False
        self._poll_thread = Thread(target=self._poll_loop)
        self._poll_thread.start()

    def _init_main_producer(self) -> Producer:
        kafka_config = config.get().kafka
        common_config = kafka_config.common or {}
        common_config["bootstrap.servers"] = kafka_config.main_brokers
        producer_config = kafka_config.producer

        # merge common config and producer config
        final_config = common_config
        final_config.update(producer_config)

        return Producer(final_config)

    def _poll_loop(self):
        while not self._cancelled:
            self._main_producer.poll(0.1)

    def close(self):
        self._cancelled = True
        self._poll_thread.join()

    def produce(self, topic, value):
        """
        An awaitable produce method.
        """
        result = self._loop.create_future()

        def ack(err, msg):
            if err:
                self._loop.call_soon_threadsafe(
                    result.set_exception, KafkaException(err)
                )
            else:
                self._loop.call_soon_threadsafe(result.set_result, msg)

        self._main_producer.produce(topic, value, on_delivery=ack)
        return result
