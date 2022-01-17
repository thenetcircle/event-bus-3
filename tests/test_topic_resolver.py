from typing import List, Tuple

import pytest
from pytest_mock import MockFixture

from eventbus import config
from eventbus.config import Config, Env, KafkaConfig, TopicMapping
from eventbus.topic_resolver import TopicResolver
from tests.utils import create_event_from_dict

SIMPLIFIED_MAPPING_TYPE = Tuple[str, List[str]]


@pytest.mark.parametrize(
    "mapping, test_cases",
    [
        (
            [
                ("user_topic", [r"user\..*"]),
                ("message_topic", [r"message\..*"]),
                ("profile_topic", [r"profile\..*"]),
                ("user2_topic", [r"use.*"]),
                ("default_topic", [r".*"]),
            ],
            [
                ("message.send", "message_topic"),
                ("my.message.send", "default_topic"),
                ("user.login", "user_topic"),
                ("user_.login", "user2_topic"),
                ("profile.visit", "profile_topic"),
            ],
        )
    ],
)
def test_resolve(mapping, test_cases, mocker: MockFixture):
    patch_config_get_with_new_mapping(
        mocker,
        mapping,
    )
    resolver = TopicResolver()
    for case in test_cases:
        event = create_event_from_dict({"title": case[0]})
        real = resolver.resolve(event.title)
        assert real == case[1]


@pytest.mark.parametrize(
    "init_mapping, new_mapping, do_reindex",
    [
        # No change
        (
            [("topic1", [r"user\..*"]), ("topic2", [r".*"])],
            [("topic1", [r"user\..*"]), ("topic2", [r".*"])],
            False,
        ),
        # Topic change
        (
            [("topic1", [r"user\..*"]), ("topic2", [r".*"])],
            [("topic1", [r"user\..*"]), ("topic3", [r".*"])],
            True,
        ),
        # Pattern change
        (
            [("topic1", [r"user\..*"]), ("topic2", [r".*"])],
            [("topic1", [r"user\..*"]), ("topic2", [r".*?"])],
            True,
        ),
        # New pattern
        (
            [("topic1", [r"user\..*"]), ("topic2", [r".*"])],
            [
                ("topic1", [r"message.*", r"user\..*"]),
                ("topic2", [r".*"]),
            ],
            True,
        ),
        # Length change
        (
            [("topic1", [r"user\..*"]), ("topic2", [r".*"])],
            [("topic1", [r"user\..*"])],
            True,
        ),
        # Order change
        (
            [("topic1", [r"user\..*"]), ("topic2", [r".*"])],
            [("topic2", [r".*"]), ("topic1", [r"user\..*"])],
            True,
        ),
    ],
)
def test_topic_mapping_update(
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
            TopicMapping(topic=ele[0], patterns=ele[1]) for ele in topic_mapping
        ],
        consumers=[],
    )
