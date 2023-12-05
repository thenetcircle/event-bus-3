from typing import List, Tuple

import pytest
from pytest_mock import MockFixture
from utils import create_event_from_dict

from eventbus import config
from eventbus.model import TopicMapping
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
@pytest.mark.asyncio
async def test_resolve(mapping, test_cases):
    _update_topic_mapping(mapping)
    resolver = TopicResolver()
    await resolver.init(config.get().topic_mapping)
    for case in test_cases:
        event = create_event_from_dict({"title": case[0]})
        real = resolver.resolve(event)
        assert real == case[1]


@pytest.mark.parametrize(
    "init_mapping, new_mapping, should_do_reindex, event_cases",
    [
        # No change
        (
            [("topic1", [r"user\..*"]), ("topic2", [r".*"])],
            [("topic1", [r"user\..*"]), ("topic2", [r".*"])],
            False,
            [("event.test", "topic2", "topic2")],
        ),
        # Topic change
        (
            [("topic1", [r"user\..*"]), ("topic2", [r".*"])],
            [("topic1", [r"user\..*"]), ("topic3", [r".*"])],
            True,
            [("event.test", "topic2", "topic3")],
        ),
        # Pattern change
        (
            [("topic1", [r"user\..*"]), ("topic2", [r".*"])],
            [("topic1", [r"user\..*"]), ("topic2", [r".*?"])],
            True,
            [],
        ),
        # New pattern
        (
            [("topic1", [r"user\..*"]), ("topic2", [r".*"])],
            [
                ("topic1", [r"message.*", r"user\..*"]),
                ("topic2", [r".*"]),
            ],
            True,
            [("message.test", "topic2", "topic1")],
        ),
        # Length change
        (
            [("topic1", [r"user\..*"]), ("topic2", [r".*"])],
            [("topic1", [r"user\..*"])],
            True,
            [("message.test", "topic2", None)],
        ),
        # Order change
        (
            [("topic1", [r"user\..*"]), ("topic2", [r".*"])],
            [("topic2", [r".*"]), ("topic1", [r"user\..*"])],
            True,
            [("user.test", "topic1", "topic2")],
        ),
    ],
)
@pytest.mark.asyncio
async def test_config_change_signal(
    init_mapping, new_mapping, should_do_reindex, event_cases, mocker: MockFixture
):
    _update_topic_mapping(init_mapping)

    resolver = TopicResolver()
    spy = mocker.spy(
        resolver,
        "_handle_config_change_signal",
    )
    await resolver.init(config.get().topic_mapping)

    for event_title, topic1, _ in event_cases:
        assert (
            resolver.resolve(create_event_from_dict({"title": event_title})) == topic1
        )

    _update_topic_mapping(new_mapping)

    if should_do_reindex:
        spy.assert_called_once()
    else:
        spy.assert_not_called()

    for event_title, _, topic2 in event_cases:
        assert (
            resolver.resolve(create_event_from_dict({"title": event_title})) == topic2
        )


def _update_topic_mapping(mapping: List[SIMPLIFIED_MAPPING_TYPE]):
    config_dict = config.get().dict()
    config_dict["topic_mapping"] = [
        TopicMapping(topic=m[0], patterns=m[1]) for m in mapping
    ]
    config.update_from_dict(config_dict)
    config.send_signals()
