import asyncio
import re
from typing import List, Tuple

from confluent_kafka import KafkaException
from loguru import logger

from eventbus import config
from eventbus.errors import KafkaConsumerClosedError, KafkaConsumerPollingError
from eventbus.event import EventStatus, KafkaEvent
from eventbus.http_sink import HttpSink
from eventbus.kafka_consumer import KafkaConsumer
from eventbus.kafka_producer import KafkaProducer
from eventbus.model import AbsSink, StoryConfig, StoryStatus


class Story:
    def __init__(self, story_config: StoryConfig):
        assert (
            story_config.status != StoryStatus.DISABLED
        ), f"{self.fullname} is disabled, not allowed to be constructed."

        self._id = story_config.id
        self._config = story_config

        logger.info(
            'Constructing a new Story with id: "{}", info: {}',
            self.id,
            story_config,
        )

        self._consumer = KafkaConsumer(
            self.id,
            config.get().consumer,
            story_config.kafka_topics,
            self._create_group_id(),
        )
        self._producer = KafkaProducer(self.id, config.get().producer)

        assert (
            story_config.sink in config.get().sinks
        ), f"The sink {story_config.sink} is not defined"
        self._sink: AbsSink = HttpSink(self.id, config.get().sinks[story_config.sink])

        # TODO init dead letter producer

        self._closing = False

    @property
    def id(self):
        return self._id

    @property
    def fullname(self):
        return f"Story#{self.id}"

    @property
    def config(self):
        return self._config

    async def init(self) -> None:
        await asyncio.gather(
            self._producer.init(), self._consumer.init(), self._sink.init()
        )

    async def run(self) -> None:
        try:
            while True:
                sending_events: List[KafkaEvent] = []

                # polling the specific amount of events unless empty or error
                for i in range(self.config.concurrent_per_partition):
                    while True:
                        try:
                            event = await self._consumer.poll(
                                self.config.event_poll_interval
                            )
                            if event is None:
                                if i > 0:
                                    break
                            else:
                                sending_events.append(event)
                                break
                        except (KafkaConsumerClosedError, KafkaConsumerPollingError):
                            raise
                        except Exception:
                            pass

                # sending the events to the sink
                sending_tasks = []
                for event in sending_events:
                    if self._check_event(event):
                        task = asyncio.create_task(self._sink.send_event(event))
                    else:
                        # If the event is not subscribed, just use a future to represent the process result
                        task = asyncio.get_running_loop().create_future()
                        task.set_result((event, EventStatus.DISCARD))
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
                        if commit_retry_times < self._config.max_commit_retries:
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
                "{} run is aborted by asyncio.CancelledError",
                self.fullname,
            )

        except Exception as ex:
            logger.error(
                "{} run is aborted by <{}> {}. tp_queue: {}",
                self.fullname,
                type(ex).__name__,
                ex,
            )
            raise

    async def close(self) -> None:
        if not self._closing:
            self._closing = True
            logger.info("Closing {}", self.fullname)

            closing_tasks = [
                self._consumer.close(),
                self._sink.close(),
                self._producer.close(),
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

    def _check_event(self, event: KafkaEvent) -> bool:
        if self._config.include_events:
            if not self._match_event_title(self._config.include_events, event):
                return False

        if self._config.exclude_events:
            return not self._match_event_title(self._config.exclude_events, event)

        return True

    def _create_group_id(self):
        return f"event-bus-3-consumer-{config.get().app.project_id}-{config.get().app.env}-{self.id}"

    @staticmethod
    def _get_dead_letter_topic(event: KafkaEvent) -> str:
        return re.sub(r"^(event-v[0-9]-)?", "\\1dead-letter-", event.topic)

    @staticmethod
    def _match_event_title(patterns: List[str], event: KafkaEvent) -> bool:
        for p in patterns:
            if re.match(re.compile(p, re.I), event.title):
                return True
        return False
