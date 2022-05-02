import asyncio
import statistics
import time
from typing import Optional

import pytest
import pytest_asyncio
from janus import Queue as JanusQueue
from utils import create_kafka_event_from_dict, create_kafka_message_from_dict

from eventbus.config import (
    ConsumerConfig,
    HttpSinkConfig,
    HttpSinkMethod,
    UseProducersConfig,
)
from eventbus.consumer import EventConsumer, KafkaConsumer
from eventbus.event import EventProcessStatus, KafkaEvent


@pytest.fixture
def consumer_conf():
    consumer_conf = ConsumerConfig(
        kafka_topics=["topic1"],
        kafka_config={
            "bootstrap.servers": "127.0.0.1:9093",
            "group.id": "test-group-1",
        },
        use_producers=UseProducersConfig(producer_ids=["p1", "p2"]),
        include_events=[r"test\..*"],
        exclude_events=[r"test\.exclude"],
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
        self.closed = False

    def put(self, item, block: bool = True, timeout: Optional[float] = None):
        return self.queue.sync_q.put(item, block, timeout)

    def poll(self, timeout):
        if self.closed:
            raise RuntimeError
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

    def store_offsets(self, message=None, offsets=None):
        self.commit(message, offsets)

    def close(self):
        self.closed = True


@pytest_asyncio.fixture
async def event_consumer(mocker, consumer_conf):
    async def mock_send_event(self, event: KafkaEvent):
        # await asyncio.sleep(0.01)
        return event, EventProcessStatus.DONE

    mocker.patch("eventbus.sink.HttpSink.send_event", mock_send_event)

    consumer = KafkaConsumer("t1", consumer_conf)
    mock_consumer = MockInternalConsumer()
    consumer._internal_consumer = mock_consumer
    # commit_spy = mocker.spy(consumer._internal_consumer, "commit")

    event_consumer = EventConsumer("t1", consumer_conf)
    event_consumer._consumer = consumer
    event_consumer._send_queue: JanusQueue = JanusQueue(maxsize=100)
    event_consumer._commit_queue = JanusQueue(maxsize=100)

    yield event_consumer


@pytest.mark.asyncio
async def test_send_events(consumer_conf):
    send_queue = JanusQueue(maxsize=100)

    consumer = KafkaConsumer("t1", consumer_conf)
    mock_consumer = MockInternalConsumer()
    consumer._internal_consumer = mock_consumer

    asyncio.create_task(
        consumer.fetch_events(send_queue)
    )  # trigger fetch events thread

    test_msg_1 = create_kafka_message_from_dict({"title": "test.e1"})
    mock_consumer.put(test_msg_1)
    event = await send_queue.async_q.get()
    assert event.title == "test.e1"
    assert send_queue.async_q.empty() == True

    test_msg_2 = create_kafka_message_from_dict({"title": "test.e2"})
    test_msg_3 = create_kafka_message_from_dict({"title": "test.e3"})
    mock_consumer.put(test_msg_2)
    mock_consumer.put(test_msg_3)
    event = await send_queue.async_q.get()
    assert event.title == "test.e2"
    event = await send_queue.async_q.get()
    assert event.title == "test.e3"
    assert send_queue.async_q.empty() == True

    test_msg_4 = create_kafka_message_from_dict({"published": "xxx"})
    mock_consumer.put(test_msg_4)
    assert send_queue.async_q.empty() == True

    await consumer.close()

    # assert _send_one_event.call_count == 3


@pytest.mark.asyncio
async def test_commit_events(mocker, consumer_conf):
    commit_queue = JanusQueue(maxsize=100)

    consumer = KafkaConsumer("t1", consumer_conf)
    consumer._internal_consumer = MockInternalConsumer()
    store_spy = mocker.spy(consumer._internal_consumer, "store_offsets")

    asyncio.create_task(
        consumer.commit_events(commit_queue)
    )  # trigger commmit events thread

    test_event_1 = create_kafka_event_from_dict({"title": "test.e1"})
    test_event_2 = create_kafka_event_from_dict({"title": "test.e2"})
    commit_queue.sync_q.put((test_event_1, EventProcessStatus.DONE))
    commit_queue.sync_q.put((test_event_2, EventProcessStatus.DONE))

    await asyncio.sleep(0.1)
    await consumer.close()
    assert store_spy.call_count == 2

    # assert _send_one_event.call_count == 3


@pytest.mark.asyncio
async def test_event_consumer(event_consumer):
    mock_consumer = event_consumer._consumer._internal_consumer

    # let's do this two times to check if the coordinator are able to rerun
    asyncio.create_task(event_consumer.run())

    # check the whole pipeline, if can get all events in commit method
    test_events_amount = 10
    for i in range(test_events_amount):
        mock_consumer.put(
            create_kafka_message_from_dict({"title": f"test.e{i+1}", "offset": i + 1})
        )

    await asyncio.sleep(0.1)
    await event_consumer.cancel()
    assert len(mock_consumer.committed_data) == test_events_amount

    # check how it acts when new events come after the coordinator cancelled
    mock_consumer.put(
        create_kafka_message_from_dict({"title": f"test.ne", "offset": -1})
    )
    await asyncio.sleep(0.1)
    assert len(mock_consumer.committed_data) == test_events_amount

    # check the order of received commits
    assert [m[0].offset for m in mock_consumer.committed_data] == [
        i for i in range(2, 12)
    ]


@pytest.mark.asyncio
async def test_event_consumer_abnormal_cases(event_consumer):
    pass


@pytest.mark.asyncio
@pytest.mark.benchmark
async def test_event_consumer_benchmark(event_consumer):
    import cProfile
    import io
    import pstats
    from pstats import SortKey

    mock_consumer = event_consumer._consumer._internal_consumer
    mock_consumer.benchmark = True

    start_time = time.time()
    test_events_amount = 10000
    for i in range(test_events_amount):
        partition = i % 10
        mock_consumer.put(
            create_kafka_message_from_dict(
                {"title": f"test.e{i+1}", "partition": partition},
                faster=True,
            )
        )
    print("\nput events cost: ", time.time() - start_time)

    # https://towardsdatascience.com/how-to-profile-your-code-in-python-e70c834fad89
    pr = cProfile.Profile()
    pr.enable()

    # let's do this two times to check if the coordinator are able to rerun
    asyncio.create_task(event_consumer.run())

    # while True:
    #     await asyncio.sleep(0.1)
    #     if coordinator._send_queue.async_q.empty():
    #         break

    await asyncio.sleep(10)
    await event_consumer.cancel()
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


@pytest.mark.asyncio
async def test_event_consumer_skip_events(event_consumer):
    mock_consumer = event_consumer._consumer._internal_consumer
    asyncio.create_task(event_consumer.run())

    mock_consumer.put(
        create_kafka_message_from_dict({"title": f"test.e1", "offset": 1})
    )
    mock_consumer.put(
        create_kafka_message_from_dict({"title": f"test.e2", "offset": 2})
    )
    mock_consumer.put(
        create_kafka_message_from_dict({"title": f"test.exclude", "offset": 3})
    )

    for i in range(4, 310):
        mock_consumer.put(
            create_kafka_message_from_dict({"title": f"skip.e{i+1}", "offset": i + 1})
        )

    await asyncio.sleep(0.5)
    await event_consumer.cancel()
    assert len(mock_consumer.committed_data) == 5

    # check the order of received commits
    assert [m[0].offset for m in mock_consumer.committed_data] == [2, 3, 105, 206, 307]
