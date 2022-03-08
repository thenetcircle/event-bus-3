import asyncio
from asyncio import Future
from threading import Thread
from typing import Callable, Dict, Optional, Tuple

from confluent_kafka import Message, Producer

from eventbus import config
from eventbus.errors import EventProducingError, EventValidationError, InitProducerError
from eventbus.event import Event


class KafkaProducer:
    def __init__(self):
        self._loop = asyncio.get_event_loop()
        self._primary_producer: Optional[InternalKafkaProducer] = None
        self._secondary_producer: Optional[InternalKafkaProducer] = None
        self._current_producer_config = config.get().producer
        config.add_subscriber(self._config_subscriber)
        self._init_internal_producers()

    async def produce(self, topic: str, event: Event) -> Message:
        """
        An awaitable produce method.
        """
        if not self._primary_producer:
            raise InitProducerError("Primary producer must be inited.")

        primary_feature, primary_ack = self._create_ack()
        self._primary_producer.produce(topic, event.payload, on_delivery=primary_ack)

        try:
            return await primary_feature
        except EventProducingError:
            if self._secondary_producer:
                secondary_feature, secondary_ack = self._create_ack()
                self._secondary_producer.produce(
                    topic, event.payload, on_delivery=secondary_ack
                )
                return await secondary_feature
            else:
                raise

    def close(self):
        if self._primary_producer:
            self._primary_producer.close(wait_to_be_finished=True)
        if self._secondary_producer:
            self._secondary_producer.close(wait_to_be_finished=True)

    def _create_ack(self) -> Tuple[Future, Callable[[Exception, Message], None]]:
        feature = self._loop.create_future()

        def ack(err: Exception, msg: Message):
            if err:
                self._loop.call_soon_threadsafe(
                    feature.set_exception, EventProducingError(err)
                )
            else:
                self._loop.call_soon_threadsafe(feature.set_result, msg)

        return feature, ack

    def _config_subscriber(self) -> None:
        if self._current_producer_config != config.get().producer:
            self._init_internal_producers()

    def _init_internal_producers(self) -> None:
        # if there is already a primary producer (when config got updated), close it.
        if self._primary_producer:
            self._primary_producer.close(wait_to_be_finished=False)
        primary_producer_config = self._create_producer_config(is_primary=True)
        if not primary_producer_config:
            raise InitProducerError("Primary producer config is none.")
        self._primary_producer = InternalKafkaProducer(primary_producer_config)

        # if there is already a secondary producer (when config got updated), close it.
        if self._secondary_producer:
            self._secondary_producer.close(wait_to_be_finished=False)
        # if there is secondary_brokers config, init the secondary producer
        secondary_producer_config = self._create_producer_config(is_primary=False)
        if secondary_producer_config:
            self._secondary_producer = InternalKafkaProducer(secondary_producer_config)

    def _create_producer_config(self, is_primary=True) -> Optional[Dict[str, str]]:
        brokers = (
            self._current_producer_config.primary_brokers
            if is_primary
            else self._current_producer_config.secondary_brokers
        )
        if not brokers:
            return None

        return {
            **self._current_producer_config.kafka_config,
            "bootstrap.servers": brokers,
        }


class InternalKafkaProducer:
    """
    KafkaProducer based on Asyncio, use another thread to poll and send result to the event loop in current thread.

    modified from https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/asyncio_example.py
    the description of the implementation: https://www.confluent.io/blog/kafka-python-asyncio-integration/
    """

    def __init__(self, producer_config: Dict[str, str]):
        self._real_producer = Producer(producer_config)
        self._cancelled = False
        self._poll_thread = Thread(target=self._poll_loop)
        self._poll_thread.start()

    def _poll_loop(self):
        while not self._cancelled:
            self._real_producer.poll(0.1)

        # make sure all messages to be sent after cancelled
        self._real_producer.flush()

    # TODO add key
    def produce(self, topic, value, **kwargs) -> None:
        """Produce message to topic. This is an asynchronous operation, an application may use
        the callback (alias on_delivery) argument to pass a function (or lambda) that will be
        called from poll() when the message has been successfully delivered or permanently fails
        delivery.

        produce(topic[, value][, key][, partition][, on_delivery][, timestamp][, headers])

        - topic (str) – Topic to produce message to
        - value (str|bytes) – Message payload
        - key (str|bytes) – Message key
        - partition (int) – Partition to produce to, else uses the configured built-in partitioner.
        - on_delivery(err,msg) (func) – Delivery report callback to call (from poll() or flush()) on successful
                                        or failed delivery
        - timestamp (int) – Message timestamp (CreateTime) in milliseconds since epoch UTC (requires librdkafka >= v0.9.4,
                            api.version.request=true, and broker >= 0.10.0.0). Default value is current time.
        - dict|list (headers) – Message headers to set on the message. The header key must be a string while the value
                                must be binary, unicode or None. Accepts a list of (key,value) or a dict.
                                (Requires librdkafka >= v0.11.4 and broker version >= 0.11.0.0)

        ref: https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Producer.produce"""
        return self._real_producer.produce(topic, value, **kwargs)

    def close(self, wait_to_be_finished=False) -> None:
        """stop the poll thread"""
        self._cancelled = True
        if wait_to_be_finished:
            self._poll_thread.join()
