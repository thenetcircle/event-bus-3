import asyncio
import re
from typing import Dict, List, Optional, Tuple

from loguru import logger
from aiokafka.errors import ConsumerStoppedError

from eventbus import config
from eventbus.event import EventStatus, KafkaEvent, KafkaTP
from eventbus.factories import SinkFactory, TransformFactory
from eventbus.kafka_consumer import KafkaConsumer, KafkaConsumerParams
from eventbus.kafka_producer import KafkaProducer, KafkaProducerParams
from eventbus.model import AbsSink, StoryParams
from eventbus.utils import deep_merge_two_dict


class Story:
    def __init__(self, story_params: StoryParams):
        self._params = story_params

        logger.info(
            "Constructing a new Story with params: {}",
            story_params,
        )

        consumer_params = deep_merge_two_dict(
            config.get().kafka.consumer, story_params.consumer_params
        )
        assert (
            "topics" in consumer_params or "topic_pattern" in consumer_params
        ), "topics or topic_pattern must be set in consumer_params"
        if "group_id" not in consumer_params:
            consumer_params["group_id"] = self._create_group_id()
        consumer_topics = consumer_params.pop("topics", None)
        consumer_topic_pattern = consumer_params.pop("topic_pattern", None)
        self._consumer = KafkaConsumer(
            KafkaConsumerParams(
                client_args=consumer_params,
                topics=consumer_topics,
                topic_pattern=consumer_topic_pattern,
            )
        )

        producer_params = config.get().kafka.producer
        self._producer = KafkaProducer(KafkaProducerParams(client_args=producer_params))

        self._transforms = []
        if story_params.transforms:
            for transform_type, transform_params in story_params.transforms:
                transform = TransformFactory.create_transform(
                    transform_type, transform_params
                )
                self._transforms.append(transform)

        self._sink: AbsSink = SinkFactory.create_sink(
            story_params.sink[0], story_params.sink[1]
        )

    async def init(self) -> None:
        logger.info("Initing Story")
        await asyncio.gather(
            self._producer.init(),
            self._consumer.init(),
            self._sink.init(),
            *[t.init() for t in self._transforms],
        )
        logger.info("Inited Story")

    async def close(self) -> None:
        logger.info("Closing Story")
        closing_tasks = [
            self._consumer.close(),
            self._producer.close(),
            self._sink.close(),
            *[t.close() for t in self._transforms],
        ]
        closing_results = await asyncio.gather(*closing_tasks, return_exceptions=True)
        logger.info(
            "Closed Story. With results: {}",
            closing_results,
        )

    async def run(self) -> None:
        try:
            logger.info("Running Story")
            while True:
                events_buffer = await self._consumer.poll()

                while len(events_buffer) > 0:
                    sending_events: List[KafkaEvent] = []
                    committing_offsets: Dict[KafkaTP, int] = {}

                    for tp in list(events_buffer.keys()):
                        events = events_buffer[tp]
                        for _ in range(self._params.concurrent_per_partition):
                            while len(events) > 0:
                                event = events.pop(0)
                                committing_offsets[tp] = event.offset + 1
                                event = await self._transform(event)
                                if event:
                                    logger.debug("Transformed event: {}", event)
                                    sending_events.append(event)
                                    break

                        # if the tp buffer is empty, remove the topic-partition from the buffer
                        if len(events) == 0:
                            del events_buffer[tp]

                    if len(sending_events) == 0:
                        continue

                    # sending the events to the sink
                    sending_tasks = []
                    for event in sending_events:
                        logger.info("Sending event: {}", event)
                        task = asyncio.create_task(self._sink.send_event(event))
                        sending_tasks.append(task)

                    sending_results: List[
                        Tuple[KafkaEvent, EventStatus]
                    ] = await asyncio.gather(*sending_tasks)

                    # sending failed tasks to another kafka topics
                    producing_tasks = []
                    for event, status in sending_results:
                        if status == EventStatus.DONE:
                            pass
                        elif status == EventStatus.DISCARD:
                            pass
                        elif status == EventStatus.DEAD_LETTER:
                            dead_letter_topic = self._get_dead_letter_topic(event)
                            logger.info(
                                "Sending event {} to dead_letter_topic {}",
                                event,
                                dead_letter_topic,
                            )
                            producing_tasks.append(
                                self._producer.produce(dead_letter_topic, event)
                            )
                    if producing_tasks:
                        await asyncio.gather(*producing_tasks)

                    # https://aiokafka.readthedocs.io/en/stable/api.html#aiokafka.AIOKafkaConsumer.commit
                    for i in range(1, self._params.max_commit_retry_times + 1):
                        try:
                            logger.debug(
                                "Committing offsets {} at the {} times",
                                committing_offsets,
                                i,
                            )
                            await self._consumer.commit(committing_offsets)
                            logger.info("Committed offsets {}", committing_offsets)
                            break
                        except Exception as ex:
                            if i == self._params.max_commit_retry_times:
                                logger.error(
                                    "Commit offsets failed with exception: <{}> {}",
                                    type(ex).__name__,
                                    ex,
                                )
                                raise
                            else:
                                logger.error(
                                    "Commit offsets failed with exception: <{}> {}, going to retry.",
                                    type(ex).__name__,
                                    ex,
                                )

        except (asyncio.CancelledError, ConsumerStoppedError) as ex:
            logger.warning("Story quit by <{}> {}", type(ex).__name__, ex)

        except Exception as ex:
            logger.error(
                "Story quit by <{}> {}",
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
        return f"event-bus-3-consumer-{config.get().app.project_id}-{config.get().app.env}-{self._params.id}"

    @staticmethod
    def _get_dead_letter_topic(event: KafkaEvent) -> str:
        return re.sub(r"^(event-v[0-9]-)?", "\\1dead-letter-", event.topic)
