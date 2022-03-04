from datetime import datetime
from typing import Any

import confluent_kafka
from pytest_mock import MockFixture

from eventbus.event import Event, KafkaEvent


def create_event_from_dict(_dict: [str, Any]) -> Event:
    return Event(
        title=_dict.get("title") or "user.login",
        id=_dict.get("id") or "test_event_1",
        published=_dict.get("published") or datetime.now(),
        payload=_dict.get("payload") or "{}",
        summary=_dict.get("summary"),
        topic=_dict.get("topic"),
    )


def create_kafka_event_from_dict(mocker: MockFixture, _dict: [str, Any]) -> KafkaEvent:
    kafka_msg = mocker.patch("confluent_kafka.cimpl.Message", autospec=True)
    kafka_msg.topic.return_value = _dict.get("topic") or "topic"
    kafka_msg.partition.return_value = _dict.get("partition") or 1
    kafka_msg.offset.return_value = _dict.get("offset") or -1

    return KafkaEvent(
        kafka_msg=kafka_msg,
        event=create_event_from_dict(_dict),
    )
