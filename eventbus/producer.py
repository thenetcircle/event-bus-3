import asyncio
import time
from asyncio import Future
from threading import Thread
from typing import Callable, List, Set, Tuple

from config import EventProducerConfig
from confluent_kafka import KafkaError, KafkaException, Message, Producer
from loguru import logger

from eventbus import config, signals
from eventbus.errors import InitProducerError
from eventbus.event import Event, create_kafka_message


class EventProducer:
    def __init__(self, caller_id: str, producer_ids: List[str]):
        self._caller_id = caller_id
        self._producer_ids: List[str] = producer_ids
        self._producers: List[KafkaProducer] = []
        self._loop = None
        self._max_retry_times_in_one_producer = 3
        signals.CONFIG_PRODUCER_CHANGED.connect(self._config_subscriber)

    @property
    def caller_id(self) -> str:
        return self._caller_id

    @property
    def full_name(self) -> str:
        return f"EventProducer#{self.caller_id}"

    async def init(self) -> None:
        logger.info("{} is initing", self.full_name)
        await self._init_producers()
        self._loop = asyncio.get_running_loop()
        logger.info("{} is inited", self.full_name)

    async def close(self):
        logger.info("{} is closing", self.full_name)
        await asyncio.gather(*[p.close() for p in self._producers])
        logger.info("{} is closed", self.full_name)

    async def produce(self, topic: str, event: Event) -> Message:
        """
        An awaitable produce method.
        """
        if not self._producers or not self._loop:
            raise InitProducerError(
                f"Need init producers of {self.full_name} before call the produce method."
            )

        start_time = time.time()
        for i, producer in enumerate(self._producers):
            try:
                msg, retry_times = await self._do_produce(topic, event, producer, 0)
                cost_time = time.time() - start_time
                logger.info(
                    'Has sent an event "{}" to producer "{}#{}" with topic: "{}", partition: {}, offset: {}.  '
                    "in {} seconds after {} times retries",
                    event,
                    self.caller_id,
                    producer.id,
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                    cost_time,
                    retry_times,
                )
                return msg

            except Exception as ex:
                cost_time = time.time() - start_time
                logger.error(
                    'Sending an event "{}" to producer "{}#{}" is failed in {} seconds with error: <{}> {}',
                    event,
                    self.caller_id,
                    producer.id,
                    cost_time,
                    type(ex).__name__,
                    ex,
                )
                if (i + 1) == len(self._producers):
                    raise

        raise RuntimeError('somehow reached the end of "produce" func')

    async def _do_produce(
        self,
        topic: str,
        event: Event,
        producer: "KafkaProducer",
        retry_times: int,
    ) -> Tuple[Message, int]:
        try:
            feature, feature_ack = self._generate_fut_ack()
            key, value = create_kafka_message(event)
            producer.produce(topic, value, key=key, on_delivery=feature_ack)
            msg: Message = await feature
            return msg, retry_times

        except KafkaException as ex:
            kafka_error: KafkaError = ex.args[0]
            if kafka_error.retriable() and retry_times < (
                self._max_retry_times_in_one_producer - 1
            ):
                return await self._do_produce(topic, event, producer, retry_times + 1)
            else:
                raise

        except Exception as ex:
            # BufferError - if the internal producer message queue is full (queue.buffering.max.messages exceeded)
            # NotImplementedError – if timestamp is specified without underlying library support.
            raise

    async def _init_producers(self) -> None:
        for producer_id in self._producer_ids:
            if producer_id not in config.get().event_producers:
                raise InitProducerError(
                    f"Producer id {producer_id} can not be found in config"
                )

            producer = KafkaProducer(
                self.caller_id, producer_id, config.get().event_producers[producer_id]
            )
            self._producers.append(producer)

        await asyncio.gather(*[p.init() for p in self._producers])

    def _generate_fut_ack(self) -> Tuple[Future, Callable[[Exception, Message], None]]:
        fut = self._loop.create_future()

        def fut_ack(err: Exception, msg: Message):
            if err:
                self._loop.call_soon_threadsafe(fut.set_exception, err)
            else:
                self._loop.call_soon_threadsafe(fut.set_result, msg)

        return fut, fut_ack

    def _config_subscriber(
        self, sender, added: Set[str], removed: Set[str], changed: Set[str]
    ) -> None:
        changed_producer_ids = changed.intersection(self._producer_ids)
        for producer in self._producers:
            if producer.id in changed_producer_ids:
                producer.update_config(config.get().event_producers[producer.id])


class KafkaProducer:
    """
    KafkaProducer based on Asyncio, use another thread to poll and send result to the event loop in current thread.

    modified from https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/asyncio_example.py
    the description of the implementation: https://www.confluent.io/blog/kafka-python-asyncio-integration/
    """

    def __init__(
        self, caller_id: str, producer_id: str, producer_conf: EventProducerConfig
    ):
        self._caller_id = caller_id
        self._id = producer_id
        self._config = producer_conf
        self._is_closed = False
        self._is_polling = False
        self._real_producer = None
        self._poll_task = None

        self._check_config()

    @property
    def caller_id(self) -> str:
        return self._caller_id

    @property
    def id(self) -> str:
        return self._id

    @property
    def full_name(self) -> str:
        return f"KafkaProducer#{self.caller_id}/{self._id}"

    async def init(self) -> None:
        logger.info("{} is initing", self.full_name)

        self._real_producer = Producer(self._config.kafka_config)
        self._poll_task = asyncio.create_task(self.poll())
        logger.info("{} is inited", self.full_name)

    async def close(self, block=True) -> None:
        logger.info("{} is closing", self.full_name)

        """stop the poll thread"""
        self._is_closed = True
        if block:
            while self._is_polling:
                await asyncio.sleep(0.1)

        # TODO does _real_producer need close?

        logger.info("{} is closed", self.full_name)

    async def poll(self):
        logger.info("`poll` of {} is starting", self.full_name)

        try:
            self._is_polling = True
            await asyncio.get_running_loop().run_in_executor(None, self._poll)
            logger.warning("`poll` of {} is end", self.full_name)
        except Exception as ex:
            logger.error(
                "`poll` of {} is aborted by: <{}> {}",
                self.full_name,
                type(ex).__name__,
                ex,
            )
            raise
        finally:
            self._is_polling = False

    def update_config(self, producer_conf: EventProducerConfig):
        # self._real_producer = Producer(producer_conf)
        # old_poll_thread = self._poll_thread
        # self._poll_thread = Thread(
        #     target=self._poll,
        #     name=f"KafkaProducer#{self._id}_poll",
        #     daemon=True,
        # )
        # self._poll_thread.start()
        pass

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

    def _poll(self) -> None:
        while not self._is_closed:
            processed_callbacks = self._real_producer.poll(0.1)
            logger.debug(
                "`_poll` of real_producer of {} processed {} `on_delivery` callbacks",
                self.full_name,
                processed_callbacks,
            )

        # make sure all messages to be sent after cancelled
        self._real_producer.flush()

    def _check_config(self) -> None:
        if "bootstrap.servers" not in self._config.kafka_config:
            raise InitProducerError(
                f'"bootstrap.servers" is required in {self.full_name} config'
            )
