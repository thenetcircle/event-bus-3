import asyncio
import re
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from aiokafka.errors import ConsumerStoppedError
from aiokafka.coordinator.assignors.sticky.sticky_assignor import (
    StickyPartitionAssignor,
)
from aiokafka.coordinator.assignors.range import (
    RangePartitionAssignor,
)
from loguru import logger
from pydantic import StrictStr

from eventbus import config
from eventbus.event import Event, EventStatus, KafkaEvent, KafkaTP
from eventbus.factories import SinkFactory, TransformFactory
from eventbus.kafka_consumer import KafkaConsumer, KafkaConsumerParams
from eventbus.kafka_producer import KafkaProducer, KafkaProducerParams
from eventbus.model import (
    AbsSink,
    EventBusBaseModel,
    SinkResult,
    SinkType,
    TransformType,
)
from eventbus.utils import deep_merge_two_dict
from eventbus.metrics import stats_client


class StoryStatus(str, Enum):
    NORMAL = "NORMAL"
    DISABLED = "DISABLED"


class StoryParams(EventBusBaseModel):
    id: StrictStr
    consumer_params: Dict[str, Any]
    sink: Tuple[SinkType, Dict[str, Any]]
    status: StoryStatus = StoryStatus.NORMAL
    transforms: Optional[List[Tuple[TransformType, Dict[str, Any]]]] = None
    concurrent_per_partition: int = 1
    max_commit_retry_times: int = 2


# class StoryInfo(EventBusModel):
#     kafka_topic: StrictStr
#     sink: StrictStr
#     event_poll_interval: float = 1.0
#     include_events: Optional[List[StrictStr]] = None
#     exclude_events: Optional[List[StrictStr]] = None
#     concurrent_per_partition: int = 1
#     send_queue_size: int = 100
#     commit_queue_size: int = 10
#     tp_queue_size: int = 3
#     max_produce_retries = 3
#     max_commit_retries = 2
#     max_skipped_events = 100
#     disabled = False


class Story:
    def __init__(self, story_params: StoryParams):
        self._params = story_params
        self._is_closed = False

        logger.info(
            "Constructing a new Story with params: {}",
            story_params,
        )

        consumer_params = self._compose_consumer_params()
        consumer_topics = consumer_params.pop("topics", None)
        consumer_topic_pattern = consumer_params.pop("topic_pattern", None)
        self._consumer = KafkaConsumer(
            KafkaConsumerParams(
                client_args=consumer_params,
                topics=consumer_topics,
                topic_pattern=consumer_topic_pattern,
            )
        )

        producer_params = config.get().default_kafka_params.producer
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
        logger.info("Initializing Story {}", self._params.id)
        await asyncio.gather(
            self._producer.init(),
            self._consumer.init(),
            self._sink.init(),
            *[t.init() for t in self._transforms],
        )
        logger.info("Story has benn initialized")

    async def close(self) -> None:
        if self._is_closed:
            return
        self._is_closed = True

        logger.info("Closing Story {}", self._params.id)
        closing_tasks = [
            self._consumer.close(),
            self._producer.close(),
            self._sink.close(),
            *[t.close() for t in self._transforms],
        ]
        closing_results = await asyncio.gather(*closing_tasks, return_exceptions=True)
        logger.info(
            "Story has been closed with results: {}",
            closing_results,
        )

    async def run(self) -> None:
        try:
            logger.info("Running Story {}", self._params.id)
            while not self._is_closed:

                events_buffer = await self._consumer.poll()
                while len(events_buffer) > 0 and not self._is_closed:

                    sending_events: List[KafkaEvent] = []
                    committing_offsets: Dict[KafkaTP, int] = {}

                    for tp in list(events_buffer.keys()):
                        events = events_buffer[tp]
                        for _ in range(self._params.concurrent_per_partition):
                            while len(events) > 0:
                                event = events.pop(0)
                                committing_offsets[tp] = event.offset + 1
                                event = await self._transform_event(event)
                                if event:
                                    logger.bind(event=event).debug(
                                        "Event has been transformed successfully"
                                    )
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
                        stats_client.incr(f"consumer.{self._params.id}.event.new")
                        task = asyncio.create_task(self._sink.send_event(event))
                        sending_tasks.append(task)
                    sending_results: List[SinkResult] = await asyncio.gather(
                        *sending_tasks
                    )

                    is_event_status_clear = True
                    producing_tasks = []  # sending failed tasks to another kafka topics
                    for _result in sending_results:
                        if _result.status == EventStatus.DONE:
                            stats_client.incr(f"consumer.{self._params.id}.event.done")
                        elif _result.status == EventStatus.UNKNOWN:
                            stats_client.incr(
                                f"consumer.{self._params.id}.event.unknown"
                            )
                            is_event_status_clear = False
                        elif _result.status == EventStatus.DEAD_LETTER:
                            stats_client.incr(
                                f"consumer.{self._params.id}.event.dead_letter"
                            )
                            if isinstance(_result.event, KafkaEvent):
                                producing_tasks.append(
                                    self._send_to_dead_letter(_result.event)
                                )
                            else:
                                logger.error(
                                    "The event is not a KafkaEvent, ignore it."
                                )
                    if len(producing_tasks) > 0:
                        await asyncio.gather(*producing_tasks)

                    if is_event_status_clear:
                        await self._commit_offsets(committing_offsets)

        except (asyncio.CancelledError, ConsumerStoppedError) as ex:
            logger.warning(
                "Story {} has been quit by <{}> {}",
                self._params.id,
                type(ex).__name__,
                ex,
            )

        except Exception as ex:
            logger.exception("Story {} has been quit by an Exception", self._params.id)
            raise

    async def _send_to_dead_letter(self, event: KafkaEvent) -> None:
        dead_letter_topic = self._get_dead_letter_topic(event)
        await self._producer.produce(dead_letter_topic, event)

    async def _commit_offsets(self, offsets: Dict[KafkaTP, int]) -> None:
        # https://aiokafka.readthedocs.io/en/stable/api.html#aiokafka.AIOKafkaConsumer.commit
        for i in range(1, self._params.max_commit_retry_times + 1):
            if i > 1:
                stats_client.incr(f"consumer.{self._params.id}.commit.retry")

            with logger.contextualize(offsets=str(offsets), commit_times=i):
                try:
                    logger.debug("Committing offsets")
                    await self._consumer.commit(offsets)
                    logger.debug("Offsets committed")
                    break
                except Exception as ex:
                    if i == self._params.max_commit_retry_times:
                        logger.exception(
                            "Commit offsets to Kafka failed and exceed the max retry times"
                        )
                        raise
                    else:
                        logger.exception(
                            "Commit offsets to Kafka failed, will retry.",
                        )

    async def _transform_event(self, event: KafkaEvent) -> Optional[KafkaEvent]:
        for transform in self._transforms:
            event = await transform.run(event)
            if not event:
                return None
        return event

    def _compose_consumer_params(self):
        consumer_params = deep_merge_two_dict(
            config.get().default_kafka_params.consumer, self._params.consumer_params
        )
        assert (
            "topics" in consumer_params or "topic_pattern" in consumer_params
        ), "topics or topic_pattern must be set in consumer_params"

        if "group_id" not in consumer_params:
            consumer_params["group_id"] = self._create_group_id()
        consumer_params["partition_assignment_strategy"] = [RangePartitionAssignor()]
        return consumer_params

    def _create_group_id(self):
        return f"event-bus_consumer_{config.get().app.project_id}_{config.get().app.env}_{self._params.id}"

    @staticmethod
    def _get_dead_letter_topic(event: KafkaEvent) -> str:
        return re.sub(r"^(event-v[0-9]-)?", "\\1dead-letter-", event.topic)
