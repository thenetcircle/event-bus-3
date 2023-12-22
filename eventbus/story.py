import asyncio
import re
import socket
from typing import List, Optional, Tuple

from confluent_kafka import KafkaException
from loguru import logger

from eventbus import config
from eventbus.errors import (
    KafkaConsumerClosedError,
    KafkaConsumerPollingError,
    StoryDisabledError,
)
from eventbus.event import EventStatus, KafkaEvent
from eventbus.factories import SinkFactory, TransformFactory
from eventbus.kafka.confluent_consumer import ConfluentKafkaConsumer
from eventbus.kafka.confluent_producer import ConfluentKafkaProducer
from eventbus.model import AbsSink, StoryParams, StoryStatus


class Story:
    def __init__(self, story_params: StoryParams):
        self._id = f"{story_params.id}_{socket.gethostname()}"
        self._params = story_params

        logger.info(
            'Constructing a new Story with id: "{}", info: {}',
            self._id,
            story_params,
        )

        consumer_kafka_config = config.get().kafka.consumer.copy()
        if story_params.kafka.bootstrap_servers:
            consumer_kafka_config[
                "bootstrap.servers"
            ] = story_params.kafka.bootstrap_servers
        self._consumer = ConfluentKafkaConsumer(
            self.id,
            consumer_kafka_config,
            story_params.kafka.topics,
            story_params.kafka.group_id or self._create_group_id(),
        )

        producer_kafka_config = config.get().kafka.producer.copy()
        self._producer = ConfluentKafkaProducer(
            self.id, producer_kafka_config, story_params.max_produce_retry_times
        )

        self._transforms = []
        if story_params.transforms:
            for transform_type, transform_params in story_params.transforms.items():
                transform = TransformFactory.create_transform(
                    transform_type, self.id, transform_params
                )
                self._transforms.append(transform)

        self._sink: AbsSink = SinkFactory.create_sink(
            story_params.sink[0], self.id, story_params.sink[1]
        )

        self._closing = False

    @property
    def id(self):
        return self._id

    @property
    def fullname(self):
        return f"Story#{self.id}"

    async def init(self) -> None:
        logger.info("Initing {}", self.fullname)
        await asyncio.gather(
            self._producer.init(),
            self._consumer.init(),
            self._sink.init(),
            *[t.init() for t in self._transforms],
        )
        logger.info("{} inited", self.fullname)

    async def close(self) -> None:
        if not self._closing:
            self._closing = True
            logger.info("Closing {}", self.fullname)

            closing_tasks = [
                self._consumer.close(),
                self._producer.close(),
                self._sink.close(),
                *[t.close() for t in self._transforms],
            ]

            closing_results = await asyncio.gather(
                *closing_tasks, return_exceptions=True
            )

            logger.info(
                "Closed {} with results: {}",
                self.fullname,
                closing_results,
            )
            self._closing = False
        else:
            while self._closing:
                await asyncio.sleep(0.1)

    async def run(self) -> None:
        try:
            logger.info("Start running {}", self.fullname)
            while True:
                sending_events: List[KafkaEvent] = []

                # polling the specific amount of events unless empty or error
                for i in range(self._params.concurrent_events):
                    while True:
                        try:
                            event = await self._consumer.poll(
                                self._params.event_poll_interval
                            )

                            logger.debug("Polling event: {}", event)

                            if event is None:
                                if i > 0:
                                    break
                            else:
                                # run Transforms
                                event = await self._transform(event)
                                if event:
                                    logger.debug("Transformed event: {}", event)
                                    sending_events.append(event)
                                    break
                        except (KafkaConsumerClosedError, KafkaConsumerPollingError):
                            raise
                        except Exception as ex:
                            logger.error("Polling event failed with error: {}", ex)
                            pass

                # sending the events to the sink
                sending_tasks = []
                for event in sending_events:
                    #     # If the event is not subscribed, just use a future to represent the process result
                    #     task = asyncio.get_running_loop().create_future()
                    #     task.set_result((event, EventStatus.DISCARD))
                    task = asyncio.create_task(self._sink.send_event(event))
                    sending_tasks.append(task)
                sending_results: List[
                    Tuple[KafkaEvent, EventStatus]
                ] = await asyncio.gather(*sending_tasks)

                for event, status in sending_results:
                    if status == EventStatus.DONE:
                        pass
                    elif status == EventStatus.DISCARD:
                        pass
                    elif status == EventStatus.DEAD_LETTER:
                        dead_letter_topic = self._get_dead_letter_topic(event)
                        await self._producer.produce(dead_letter_topic, event)

                # commit the event to kafka
                commit_retry_times = 0
                while True:
                    try:
                        await self._consumer.commit(*[r[0] for r in sending_results])
                        break
                    except KafkaException as ex:
                        if commit_retry_times < self._params.max_commit_retry_times:
                            commit_retry_times = commit_retry_times + 1
                            continue
                        else:
                            logger.error(
                                "Storing an event offset failed after {} times retry, with exception: <{}> {}",
                                commit_retry_times,
                                type(ex).__name__,
                                ex,
                            )
                            raise

        except asyncio.CancelledError:
            logger.info(
                "{}'s running is aborted by asyncio.CancelledError",
                self.fullname,
            )

        except Exception as ex:
            logger.error(
                "{}'s running is aborted by <{}> {}. tp_queue: {}",
                self.fullname,
                type(ex).__name__,
                ex,
            )
            raise

    async def _transform(self, event: KafkaEvent) -> Optional[KafkaEvent]:
        for transform in self._transforms:
            event = await transform.run(event)
            if not event:
                return None
        return event

    def _create_group_id(self):
        return f"event-bus-3-consumer-{config.get().app.project_id}-{config.get().app.env}-{self.id}"

    @staticmethod
    def _get_dead_letter_topic(event: KafkaEvent) -> str:
        return re.sub(r"^(event-v[0-9]-)?", "\\1dead-letter-", event.topic)
