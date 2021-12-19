import asyncio
from threading import Thread

from confluent_kafka import Producer

from eventbus import config
from eventbus.errors import EventProduceError, EventValidationError
from eventbus.event import Event


class KafkaProducer:
    """
    KafkaProducer based on Asyncio, use another thread to poll and send result to event loop in current thread.

    modified from https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/asyncio_example.py
    the description of the implementation: https://www.confluent.io/blog/kafka-python-asyncio-integration/
    """

    def __init__(self):
        self._loop = asyncio.get_event_loop()
        self._primary_producer = self._init_primary_producer()
        self._cancelled = False
        self._poll_thread = Thread(target=self._poll_loop)
        self._poll_thread.start()

    def _init_primary_producer(self) -> Producer:
        kafka_config = config.get().kafka
        common_config = kafka_config.common_config or {}
        common_config["bootstrap.servers"] = kafka_config.primary_brokers
        producer_config = kafka_config.producer_config

        # merge common config and producer config
        final_config = common_config
        final_config.update(producer_config)

        return Producer(final_config)

    def _poll_loop(self):
        while not self._cancelled:
            self._primary_producer.poll(0.1)

    def close(self):
        self._cancelled = True
        self._poll_thread.join()

    async def produce(self, event: Event):
        """
        An awaitable produce method.
        """
        if not event.topic:
            raise EventValidationError(f"The event ({event}) topic is not resolved.")

        result = self._loop.create_future()

        def ack(err, msg):
            if err:
                self._loop.call_soon_threadsafe(
                    result.set_exception, EventProduceError(err)
                )
            else:
                self._loop.call_soon_threadsafe(result.set_result, msg)

        self._primary_producer.produce(event.topic, event.payload, on_delivery=ack)
        return await result
