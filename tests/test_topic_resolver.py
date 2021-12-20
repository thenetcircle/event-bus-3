from typing import List, Tuple

import pytest
from pytest_mock import MockFixture

from eventbus import config
from eventbus.config import Config, Env, KafkaConfig, TopicMapping
from eventbus.topic_resolver import TopicResolver

SIMPLIFIED_MAPPING_TYPE = Tuple[str, List[str], List[str]]


@pytest.mark.parametrize(
    "init_mapping, new_mapping, do_reindex",
    [
        # No change
        (
            [("topic1", ["n1"], ["user\..*"]), ("topic2", ["n1"], [".*"])],
            [("topic1", ["n1"], ["user\..*"]), ("topic2", ["n1"], [".*"])],
            False,
        ),
        # Topic change
        (
            [("topic1", ["n1"], ["user\..*"]), ("topic2", ["n1"], [".*"])],
            [("topic1", ["n1"], ["user\..*"]), ("topic3", ["n1"], [".*"])],
            True,
        ),
        # NS change
        (
            [("topic1", ["n1"], ["user\..*"]), ("topic2", ["n1"], [".*"])],
            [("topic1", ["n1"], ["user\..*"]), ("topic2", ["n2"], [".*"])],
            True,
        ),
        # Pattern change
        (
            [("topic1", ["n1"], ["user\..*"]), ("topic2", ["n1"], [".*"])],
            [("topic1", ["n1"], ["user\..*"]), ("topic2", ["n1"], [".*?"])],
            True,
        ),
        # New pattern
        (
            [("topic1", ["n1"], ["user\..*"]), ("topic2", ["n1"], [".*"])],
            [("topic1", ["n1"], ["message.*", "user\..*"]), ("topic2", ["n1"], [".*"])],
            True,
        ),
        # Length change
        (
            [("topic1", ["n1"], ["user\..*"]), ("topic2", ["n1"], [".*"])],
            [("topic1", ["n1"], ["user\..*"])],
            True,
        ),
        # Order change
        (
            [("topic1", ["n1"], ["user\..*"]), ("topic2", ["n1"], [".*"])],
            [("topic2", ["n1"], [".*"]), ("topic1", ["n1"], ["user\..*"])],
            True,
        ),
    ],
)
def test_topic_mapping_subscriber(
    init_mapping, new_mapping, do_reindex, mocker: MockFixture
):
    patch_config_get_with_new_mapping(mocker, init_mapping)

    resolver = TopicResolver()
    mocker.patch.object(resolver, "reindex")

    resolver.topic_mapping_subscriber()  # call first time
    resolver.reindex.assert_not_called()

    patch_config_get_with_new_mapping(mocker, new_mapping)
    resolver.topic_mapping_subscriber()  # call second time
    if do_reindex:
        resolver.reindex.assert_called_once()
    else:
        resolver.reindex.assert_not_called()


def patch_config_get_with_new_mapping(
    mocker: MockFixture, mapping: List[SIMPLIFIED_MAPPING_TYPE]
) -> None:
    mocker.patch.object(config, "get", lambda: create_config(mapping))


def create_config(topic_mapping: List[SIMPLIFIED_MAPPING_TYPE]) -> Config:
    return Config(
        env=Env.TEST,
        debug=True,
        kafka=KafkaConfig(primary_brokers="", producer_config={}, consumer_config={}),
        topic_mapping=[
            TopicMapping(topic=ele[0], namespaces=ele[1], patterns=ele[2])
            for ele in topic_mapping
        ],
    )
