from typing import List, Tuple

import pytest
from pytest_mock import MockFixture
from utils import create_event_from_dict

from eventbus import config
from eventbus.config import TopicMapping
from eventbus.topic_resolver import TopicResolver

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
def test_resolve(mapping, test_cases):
    _update_topic_mapping(mapping)
    resolver = TopicResolver()
    for case in test_cases:
        event = create_event_from_dict({"title": case[0]})
        real = resolver.resolve(event)
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
def test_topic_mapping_signals(
    init_mapping, new_mapping, do_reindex, mocker: MockFixture
):
    _update_topic_mapping(init_mapping)

    resolver = TopicResolver()
    mocker.patch.object(resolver, "reindex")

    _update_topic_mapping(new_mapping)

    if do_reindex:
        resolver.reindex.assert_called_once()
    else:
        resolver.reindex.assert_not_called()


def _update_topic_mapping(mapping: List[SIMPLIFIED_MAPPING_TYPE]):
    config_dict = config.get().dict()
    config_dict["topic_mapping"] = [
        TopicMapping(topic=m[0], patterns=m[1]) for m in mapping
    ]
    config.update_from_dict(config_dict)
