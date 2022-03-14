import asyncio
import statistics
import sys
import time
from typing import Optional

import loguru
import pytest
import pytest_asyncio
from consumer import ConsumerCoordinator, KafkaConsumer
from janus import Queue as JanusQueue

from eventbus.config import ConsumerConfig, HttpSinkConfig, HttpSinkMethod
from eventbus.event import EventProcessStatus, KafkaEvent
from tests.utils import create_kafka_event_from_dict, create_kafka_message_from_dict


@pytest.fixture
def consumer_conf():
    consumer_conf = ConsumerConfig(
        id="test_consumer",
        listening_topics=["topic1"],
        kafka_config={
            "bootstrap.servers": "127.0.0.1:9093",
            "group.id": "test-group-1",
        },
        sink=HttpSinkConfig(
            url="/", method=HttpSinkMethod.POST, timeout=0.2, max_retry_times=3
        ),
        concurrent_per_partition=1,
    )
    yield consumer_conf


class MockInternalConsumer:
    def __init__(self):
        self.queue = JanusQueue(maxsize=100000)
        self.committed_data = []
        self.benchmark = False

    def put(self, item, block: bool = True, timeout: Optional[float] = None):
        return self.queue.sync_q.put(item, block, timeout)

    def poll(self, timeout):
        try:
            msg = self.queue.sync_q.get(block=True, timeout=timeout)
            if self.benchmark:
                msg._offset = int(time.time() * 1000000)
            return msg
        except:
            return None

    def commit(self, message=None, offsets=None, asynchronous=True):
        if self.benchmark:
            self.committed_data.append(
                [time.time() - (t.offset / 1000000) for t in offsets][0]
            )
        else:
            self.committed_data.append(offsets)

    def close(self):
        pass


@pytest_asyncio.fixture
async def coordinator(mocker, consumer_conf):
    async def mock_send_event(self, event: KafkaEvent):
        # await asyncio.sleep(0.01)
        return event, EventProcessStatus.DONE

    mocker.patch("eventbus.sink.HttpSink.send_event", mock_send_event)

    consumer = KafkaConsumer(consumer_conf)
    mock_consumer = MockInternalConsumer()
    consumer._internal_consumer = mock_consumer
    # commit_spy = mocker.spy(consumer._internal_consumer, "commit")

    coordinator = ConsumerCoordinator(consumer_conf)
    coordinator._consumer = consumer
    coordinator._send_queue: JanusQueue = JanusQueue(maxsize=100)
    coordinator._commit_queue = JanusQueue(maxsize=100)

    yield coordinator


@pytest.mark.asyncio
async def test_send_events(consumer_conf):
    send_queue = JanusQueue(maxsize=100)

    consumer = KafkaConsumer(consumer_conf)
    mock_consumer = MockInternalConsumer()
    consumer._internal_consumer = mock_consumer

    asyncio.create_task(
        consumer.fetch_events(send_queue)
    )  # trigger fetch events thread

    test_msg_1 = create_kafka_message_from_dict({"id": "e1"})
    mock_consumer.put(test_msg_1)
    event = await send_queue.async_q.get()
    assert event.id == "e1"
    assert send_queue.async_q.empty() == True

    test_msg_2 = create_kafka_message_from_dict({"id": "e2"})
    test_msg_3 = create_kafka_message_from_dict({"id": "e3"})
    mock_consumer.put(test_msg_2)
    mock_consumer.put(test_msg_3)
    event = await send_queue.async_q.get()
    assert event.id == "e2"
    event = await send_queue.async_q.get()
    assert event.id == "e3"
    assert send_queue.async_q.empty() == True

    test_msg_4 = create_kafka_message_from_dict({"published": "xxx"})
    mock_consumer.put(test_msg_4)
    assert send_queue.async_q.empty() == True

    await consumer.close()

    # assert _send_one_event.call_count == 3


@pytest.mark.asyncio
async def test_commit_events(mocker, consumer_conf):
    commit_queue = JanusQueue(maxsize=100)

    consumer = KafkaConsumer(consumer_conf)
    consumer._internal_consumer = MockInternalConsumer()
    commit_spy = mocker.spy(consumer._internal_consumer, "commit")

    asyncio.create_task(
        consumer.commit_events(commit_queue)
    )  # trigger commmit events thread

    test_event_1 = create_kafka_event_from_dict({"id": "e1"})
    test_event_2 = create_kafka_event_from_dict({"id": "e2"})
    commit_queue.sync_q.put((test_event_1, EventProcessStatus.DONE))
    commit_queue.sync_q.put((test_event_2, EventProcessStatus.DONE))

    await asyncio.sleep(0.1)
    await consumer.close()
    assert commit_spy.call_count == 2

    # assert _send_one_event.call_count == 3


@pytest.mark.asyncio
async def test_consumer_coordinator(coordinator):
    mock_consumer = coordinator._consumer._internal_consumer

    # let's do this two times to check if the coordinator are able to rerun
    asyncio.create_task(coordinator.run())

    # check the whole pipeline, if can get all events in commit method
    test_events_amount = 10
    for i in range(test_events_amount):
        mock_consumer.put(
            create_kafka_message_from_dict({"id": f"e{i+1}", "offset": i + 1})
        )

    await asyncio.sleep(0.1)
    await coordinator.cancel()
    assert len(mock_consumer.committed_data) == test_events_amount

    # check how it acts when new events come after the coordinator cancelled
    mock_consumer.put(create_kafka_message_from_dict({"id": f"ne", "offset": -1}))
    await asyncio.sleep(0.1)
    assert len(mock_consumer.committed_data) == test_events_amount

    # check the order of received commits
    assert [m[0].offset for m in mock_consumer.committed_data] == [
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
    ]


@pytest.mark.asyncio
async def test_consumer_coordinator_abnormal_cases(coordinator):
    pass


@pytest.mark.asyncio
@pytest.mark.benchmark
async def test_consumer_coordinator_benchmark(coordinator):
    import cProfile
    import io
    import pstats
    from pstats import SortKey

    loguru.logger.remove()
    loguru.logger.add(sys.stderr, level="INFO")

    mock_consumer = coordinator._consumer._internal_consumer
    mock_consumer.benchmark = True

    start_time = time.time()
    test_events_amount = 10000
    for i in range(test_events_amount):
        partition = i % 10
        mock_consumer.put(
            create_kafka_message_from_dict(
                {"id": f"e{i+1}", "partition": partition},
                faster=True,
            )
        )
    print("\nput events cost: ", time.time() - start_time)

    # https://towardsdatascience.com/how-to-profile-your-code-in-python-e70c834fad89
    pr = cProfile.Profile()
    pr.enable()

    # let's do this two times to check if the coordinator are able to rerun
    asyncio.create_task(coordinator.run())

    # while True:
    #     await asyncio.sleep(0.1)
    #     if coordinator._send_queue.async_q.empty():
    #         break

    await asyncio.sleep(10)
    await coordinator.cancel()
    await asyncio.sleep(1)

    print("\n---\n")
    # print(mock_consumer.committed_data)
    print("Length: ", len(mock_consumer.committed_data))
    print("Max: ", max(mock_consumer.committed_data))
    print("Median: ", statistics.median(mock_consumer.committed_data))
    print("Mean: ", statistics.mean(mock_consumer.committed_data))
    print("Min: ", min(mock_consumer.committed_data))
    # print(mock_consumer.committed_data)
    print("\n---\n")

    pr.disable()
    si = io.StringIO()
    ps = pstats.Stats(pr, stream=si).sort_stats(SortKey.CUMULATIVE)
    ps.print_stats(15)
    print(si.getvalue())

    assert len(mock_consumer.committed_data) == test_events_amount
