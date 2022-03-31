import asyncio
import json
import os
import time
from concurrent.futures import Future
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pytest
from aiohttp import web
from confluent_kafka import Consumer, Message, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from loguru import logger

from eventbus import config
from eventbus.config import UseProducersConfig
from eventbus.consumer import EventConsumer
from eventbus.producer import EventProducer
from eventbus.sink import HttpSink
from tests.utils import create_event_from_dict


@pytest.fixture
def setup_kafka_cluster():
    config_path = Path(__file__).parent / "config.it.yml"
    config.update_from_yaml(config_path)

    if "EVENTBUS_TEST_BROKERS" in os.environ:
        brokers = os.environ["EVENTBUS_TEST_BROKERS"]
        config_dict = config.get().dict()
        for _, c in config_dict["consumers"].items():
            c["kafka_config"]["bootstrap.servers"] = brokers
        for _, p in config_dict["producers"].items():
            p["kafka_config"]["bootstrap.servers"] = brokers
        config.update_from_dict(config_dict)

        admin_client = AdminClient({"bootstrap.servers": brokers})

        curr_datetime = datetime.now().strftime("%Y%m%d%H%M%S-%f")
        test_topic_name = f"event-bus-3-integration-test-{curr_datetime}"
        test_topics = [NewTopic(test_topic_name, 3, 3)]
        try:
            result: Dict[str, Future] = admin_client.create_topics(test_topics)
            logger.info("New topics creating result: {}", result)
            for _, fut in result.items():
                fut.result(timeout=3.0)

            yield test_topic_name, admin_client

        finally:
            admin_client.delete_topics([test_topic_name])
    else:
        yield


@pytest.mark.integration
@pytest.mark.asyncio
async def test_producer(setup_kafka_cluster, producer_ids=None):
    if setup_kafka_cluster:
        temp_topic, admin_client = setup_kafka_cluster

        event_producer = EventProducer(
            "it", UseProducersConfig(producer_ids=(producer_ids or ["p1"]))
        )
        await event_producer.init()

        events_num = 10
        try:
            for i in range(events_num):
                event = create_event_from_dict(
                    {"id": str(i + 100), "payload": f"event{i}"}
                )
                await event_producer.produce(temp_topic, event)
        finally:
            await event_producer.close()

        consumer_conf = config.get().consumers["c1"].kafka_config
        #  consumer_conf["auto.offset.reset"] = "earliest"
        consumer = Consumer(consumer_conf)
        consumer.assign([TopicPartition(temp_topic, i) for i in range(3)])
        msgs: List[Message] = consumer.consume(events_num, timeout=5.0)
        msg_values = set([m.value().decode("utf-8") for m in msgs if not m.error()])
        msg_partitions = set([m.partition() for m in msgs if not m.error()])
        assert len(msg_values) == events_num
        for i in range(events_num):
            assert f'"event{i}"' in msg_values
        assert len(msg_partitions) == 3


@pytest.mark.integration
@pytest.mark.asyncio
async def test_switch_producer(setup_kafka_cluster):
    if setup_kafka_cluster:
        config_dict = config.get().dict()
        config_dict["producers"]["p1"]["kafka_config"][
            "bootstrap.servers"
        ] = "localhost:11111"
        config.update_from_dict(config_dict)
        await test_producer(setup_kafka_cluster, ["p1", "p2"])


@pytest.mark.integration
@pytest.mark.asyncio
async def test_consumer(
    setup_kafka_cluster, aiohttp_client, round=1, consumer_group_id=None
):
    if setup_kafka_cluster:
        temp_topic, admin_client = setup_kafka_cluster

        consumer_group_id = consumer_group_id or f"event-bus-3-it-{time.time()}"

        # send some test msgs into the temp_topic
        producer_conf = config.get().producers["p1"].kafka_config
        producer = Producer(producer_conf)
        events_num = 100
        for i in range(events_num):
            _id = str(i + 100 * round)
            event_data = json.dumps(
                {
                    "id": _id,
                    "title": f"event{_id}",
                    "published": "2022-03-31 10:22:00",
                }
            )
            producer.produce(temp_topic, event_data, _id)
        # producer.poll(5)
        producer.flush()

        # --- mock objects

        received_reqs = []

        async def mock_server(request):
            try:
                req_body = await request.json()
                received_reqs.append(req_body)
                return web.Response(text="ok")
            except Exception as ex:
                logger.error(ex)

        app = web.Application()
        app.router.add_post("/", mock_server)
        client = await aiohttp_client(app)
        consumer_conf = config.get().consumers["c1"]
        consumer_conf.kafka_topics[0] = temp_topic
        consumer_conf.kafka_config["group.id"] = consumer_group_id
        consumer = EventConsumer("c1", consumer_conf)
        await consumer.init()
        sink = HttpSink("test_sink", consumer_conf)
        sink._client = client
        consumer._sink = sink

        # ---

        curr_positions = []

        async def check_reqs():
            while True:
                if len(received_reqs) == events_num:
                    nonlocal curr_positions
                    curr_positions = consumer._consumer._internal_consumer.position(
                        [TopicPartition(temp_topic, i) for i in range(3)]
                    )
                    await consumer.cancel()
                    break

                # if round == 2:
                #     print(len(received_reqs))
                #     print(sorted([int(r["id"]) for r in received_reqs]))

                await asyncio.sleep(0.1)

        await asyncio.wait_for(
            asyncio.gather(consumer.run(), check_reqs()), timeout=10.0
        )

        assert len(received_reqs) == events_num
        assert sorted([int(r["id"]) for r in received_reqs]) == [
            i + 100 * round for i in range(events_num)
        ]
        assert sum([p.offset for p in curr_positions]) == events_num * round


@pytest.mark.integration
@pytest.mark.asyncio
async def test_consumer_reconnect(setup_kafka_cluster, aiohttp_client):
    consumer_group_id = f"event-bus-3-it-{time.time()}"
    for round in range(1, 4):
        await test_consumer(
            setup_kafka_cluster, aiohttp_client, round, consumer_group_id
        )
