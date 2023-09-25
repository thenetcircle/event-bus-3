import asyncio
import logging
import time
from asyncio import Future
from typing import Callable, Optional, Tuple

from confluent_kafka import KafkaError, KafkaException, Message, Producer
from loguru import logger

from eventbus.config import ProducerConfig
from eventbus.errors import InitKafkaProducerError
from eventbus.event import Event, create_kafka_message
from eventbus.metrics import stats_client


class KafkaProducer:
    """
    KafkaProducer based on Asyncio, use another thread to poll and send result to the event loop in current thread.

    modified from https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/asyncio_example.py
    the description of the implementation: https://www.confluent.io/blog/kafka-python-asyncio-integration/
    """

    def __init__(self, id: str, producer_config: ProducerConfig):
        logger.info(
            'Constructing a new KafkaProducer with id: "{}", conf: {}',
            id,
            producer_config,
        )

        self._id = id
        self._config = producer_config

        self._is_closed = False
        self._loop = None
        self._real_producer: Optional[Producer] = None
        self._poll_task: Optional[asyncio.Task] = None

        self._check_config()

    @property
    def id(self) -> str:
        return self._id

    @property
    def config(self) -> ProducerConfig:
        return self._config

    @property
    def fullname(self) -> str:
        return f"KafkaProducer#{self.id}"

    async def init(self) -> None:
        logger.info("{} is initing", self.fullname)
        self._loop = asyncio.get_running_loop()
        self._real_producer = Producer(
            self.config.kafka_config, logger=logging.getLogger(self.fullname)
        )
        self._poll_task = asyncio.create_task(self.poll())
        logger.info("{} is inited", self.fullname)

    async def close(self) -> None:
        logger.info("{} is closing", self.fullname)
        # to stop the poll thread
        self._is_closed = True
        if self._poll_task:
            await self._poll_task
        logger.warning("{} is closed", self.fullname)

    async def produce(self, topic: str, event: Event) -> Message:
        """
        An awaitable produce method.
        """
        if not self._real_producer or not self._loop:
            raise InitKafkaProducerError(
                f"Need init {self.fullname} before call the produce method."
            )

        start_time = time.time()
        retry_times = 0
        try:
            msg: Message = None

            while True:
                try:
                    feature, feature_ack = self._generate_fut_ack()
                    key, value = create_kafka_message(event)
                    self._do_produce(topic, value, key=key, on_delivery=feature_ack)
                    msg = await feature
                    break
                except KafkaException as ex:
                    kafka_error: KafkaError = ex.args[0]
                    if (
                        isinstance(ex.args[0], KafkaError)
                        and kafka_error.retriable
                        and retry_times < (self.config.max_retries - 1)
                    ):
                        logger.warning(
                            'Producing an event "{}" to a Kafka topic "{}" by "{}", '
                            "failed in {} seconds after {} times retries, "
                            "with a retriable error: <{}> {}",
                            event,
                            topic,
                            self.fullname,
                            time.time() - start_time,
                            retry_times,
                            type(ex).__name__,
                            ex,
                        )
                        retry_times = retry_times + 1
                    else:
                        raise
                # except Exception as ex:
                #     # BufferError - if the internal producer message queue is full (queue.buffering.max.messages exceeded)
                #     # NotImplementedError – if timestamp is specified without underlying library support.
                #     raise

            cost_time = time.time() - start_time
            logger.info(
                'Produced an event "{}" to Kafka by "{}", '
                'with topic: "{}", partition: {}, offset: {},  '
                "in {} seconds after {} times retries",
                event,
                self.fullname,
                msg.topic(),
                msg.partition(),
                msg.offset(),
                cost_time,
                retry_times,
            )
            stats_client.incr("producer.event.succ")
            return msg

        except Exception as ex:
            cost_time = time.time() - start_time
            logger.error(
                'Producing an event "{}" to a Kafka topic "{}" by "{}", '
                "failed in {} seconds after {} times retries, "
                "with error: <{}> {}",
                event,
                topic,
                self.fullname,
                cost_time,
                retry_times,
                type(ex).__name__,
                ex,
            )
            raise

    async def poll(self):
        logger.info("`poll` of {} is starting", self.fullname)

        try:
            await self._loop.run_in_executor(None, self._poll)
            logger.info("`poll` of {} is over", self.fullname)

        except Exception as ex:
            logger.error(
                "`poll` of {} is aborted by: <{}> {}",
                self.fullname,
                type(ex).__name__,
                ex,
            )
            raise

    def update_config(self, producer_config: ProducerConfig):
        logger.info("Updating config of {}", self.fullname)

        old_real_producer = self._real_producer

        # start new producer
        new_real_producer = Producer(
            producer_config.kafka_config, logger=logging.getLogger(self.fullname)
        )
        self._config = producer_config

        # switch to new producer
        self._real_producer = new_real_producer

        # close old producer
        if old_real_producer:
            old_real_producer.flush()

        logger.info("Config of {} updated", self.fullname)

    # TODO add key
    def _do_produce(self, topic, value, **kwargs) -> None:
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

    def _poll(self) -> None:
        while not self._is_closed:
            processed_callbacks = self._real_producer.poll(0.1)
            logger.debug(
                "`_poll` of real_producer of {} processed {} `on_delivery` callbacks",
                self.fullname,
                processed_callbacks,
            )

        # make sure all messages to be sent after cancelled
        self._real_producer.flush()

    def _generate_fut_ack(self) -> Tuple[Future, Callable[[Exception, Message], None]]:
        fut = self._loop.create_future()

        def fut_ack(err: KafkaError, msg: Message):
            if err:
                self._loop.call_soon_threadsafe(fut.set_exception, KafkaException(err))
            else:
                self._loop.call_soon_threadsafe(fut.set_result, msg)

        return fut, fut_ack

    def _check_config(self) -> None:
        if "bootstrap.servers" not in self.config.kafka_config:
            raise InitKafkaProducerError(
                f'"bootstrap.servers" is required in {self.fullname} config'
            )
