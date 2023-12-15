import json

import pytest

from eventbus import config
from eventbus.model import KafkaParams, SinkType
from eventbus.zoo_client import ZooClient
from eventbus.zoo_data_parser import ZooDataParser


@pytest.fixture
def zoo_client() -> ZooClient:
    zoo_client = ZooClient(
        hosts=config.get().zookeeper.hosts,
        timeout=config.get().zookeeper.timeout,
    )
    zoo_client.init()
    yield zoo_client


@pytest.fixture
def zoo_data_parser(zoo_client) -> ZooDataParser:
    yield ZooDataParser(zoo_client)


def test_parse_story_data(zoo_data_parser: ZooDataParser):
    stories_path = "/event-bus/popp/dev/stories/payment-callback"
    story_params = zoo_data_parser.get_story_params(stories_path)
    assert story_params.id == "payment-callback"
    assert story_params.kafka == KafkaParams(
        topics=["event-v2-popp-payment-callback"],
        topic_pattern=None,
        group_id=None,
        bootstrap_servers="maggie-kafka-1:9094,maggie-kafka-2:9094,maggie-kafka-3:9094",
    )
    assert story_params.sink == (
        SinkType.HTTP,
        {
            "url": "http://rose.kevin.poppen2.lab/api/internal/eventbus/receiver",
            "headers": {"accept": "application/json;version=1"},
        },
    )
    assert story_params.transforms == None
    print(json.dumps(json.loads(story_params.json()), indent=4))


@pytest.mark.skip
def test_print_all_stories(zoo_client: ZooClient, zoo_data_parser: ZooDataParser):
    stories_path = "/event-bus/popp/dev/stories"
    stories_ids = zoo_client.get_children(stories_path)
    for story_id in stories_ids:
        story_path = f"{stories_path}/{story_id}"
        story_params = zoo_data_parser.get_story_params(story_path)
        print("=======\n", story_id)
        if story_params is None:
            print(None)
        else:
            print(json.dumps(json.loads(story_params.json()), indent=4))
        print("\n\n\n")
