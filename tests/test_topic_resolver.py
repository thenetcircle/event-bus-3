import pytest
from pytest_mock import MockFixture
from utils import create_event_from_dict

from eventbus.topic_resolver import TopicResolver, TopicMappingEntry


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
    resolver = TopicResolver()
    await resolver.set_topic_mapping(
        [TopicMappingEntry(topic=m[0], patterns=m[1]) for m in mapping]
    )
    for case in test_cases:
        event = create_event_from_dict({"title": case[0]})
        real = resolver.resolve(event)
        assert real == case[1]


def test_convert_str_to_topic_mapping():
    thestr = '[{"topic": "event-v2-happ-greenarrow", "patterns": ["greenarrow\\\\..*"]}, {"topic": "event-v2-happ-default", "patterns": [".*"]}]'

    assert TopicResolver.convert_str_to_topic_mapping(thestr) == [
        TopicMappingEntry(
            topic="event-v2-happ-greenarrow", patterns=["greenarrow\\..*"]
        ),
        TopicMappingEntry(topic="event-v2-happ-default", patterns=[".*"]),
    ]


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
async def test_update_mapping(
    init_mapping, new_mapping, should_do_reindex, event_cases, mocker: MockFixture
):
    resolver = TopicResolver()
    await resolver.set_topic_mapping(
        [TopicMappingEntry(topic=m[0], patterns=m[1]) for m in init_mapping]
    )

    for event_title, topic1, _ in event_cases:
        assert (
            resolver.resolve(create_event_from_dict({"title": event_title})) == topic1
        )

    await resolver.set_topic_mapping(
        [TopicMappingEntry(topic=m[0], patterns=m[1]) for m in new_mapping]
    )

    for event_title, _, topic2 in event_cases:
        assert (
            resolver.resolve(create_event_from_dict({"title": event_title})) == topic2
        )
