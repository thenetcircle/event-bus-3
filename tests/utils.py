import json
from datetime import datetime
from typing import Any
from unittest import mock

from confluent_kafka.cimpl import TIMESTAMP_CREATE_TIME, TIMESTAMP_NOT_AVAILABLE

from eventbus.event import Event, KafkaEvent


def create_event_from_dict(_dict: [str, Any]) -> Event:
    return Event(
        title=_dict.get("title") or "user.login",
        id=_dict.get("id") or "test_event_1",
        published=_dict.get("published") or datetime.now(),
        payload=_dict.get("payload") or "{}",
    )


def create_kafka_event_from_dict(_dict: [str, Any]) -> KafkaEvent:
    return KafkaEvent(
        title=_dict.get("title") or "user.login",
        id=_dict.get("id") or "test_event_1",
        published=_dict.get("published") or datetime.now(),
        payload=_dict.get("payload") or "{}",
        topic=_dict.get("topic") or "topic",
        partition=_dict.get("partition") or 1,
        offset=_dict.get("offset") or -1,
        timestamp=_dict.get("timestamp") or None,
    )


def create_kafka_message_from_dict(
    _dict: [str, Any], faster=False
) -> "confluent_kafka.Message":
    if faster:

        class MockMessage:
            def __init__(self):
                self._topic = _dict.get("topic") or "topic1"
                self._offset = _dict.get("offset") or 1

            def topic(self):
                return self._topic

            def partition(self):
                return _dict.get("partition") or 1

            def offset(self):
                return self._offset

            def key(self):
                return _dict.get("key") or ""

            def value(self):
                value = {}
                value["id"] = _dict.get("id") or "test_event_1"
                value["title"] = _dict.get("title") or "user.login"
                # YYYY-MM-DD[T]HH:MM[:SS[.ffffff]][Z or [±]HH[:]MM]]]
                value["published"] = _dict.get("published") or (
                    datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                )
                return json.dumps(value)

            def timestamp(self):
                msg_timestamp = _dict.get("timestamp")
                return (
                    (TIMESTAMP_CREATE_TIME, msg_timestamp)
                    if msg_timestamp
                    else (TIMESTAMP_NOT_AVAILABLE, None)
                )

            def error(self):
                _dict.get("error") or None

        return MockMessage()

    else:
        with mock.patch("confluent_kafka.Message", autospec=True) as MessageClass:
            msg = MessageClass()
            msg.topic.return_value = _dict.get("topic") or "topic1"
            msg.partition.return_value = _dict.get("partition") or 1
            msg.offset.return_value = _dict.get("offset") or 1
            msg.key.return_value = _dict.get("key") or ""

            value = {}
            value["id"] = _dict.get("id") or "test_event_1"
            value["title"] = _dict.get("title") or "user.login"
            # YYYY-MM-DD[T]HH:MM[:SS[.ffffff]][Z or [±]HH[:]MM]]]
            value["published"] = _dict.get("published") or (
                datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            )
            msg.value.return_value = json.dumps(value)

            msg_timestamp = _dict.get("timestamp")
            # datetime.utcnow().microsecond * 1000
            msg.timestamp.return_value = (
                (TIMESTAMP_CREATE_TIME, msg_timestamp)
                if msg_timestamp
                else (TIMESTAMP_NOT_AVAILABLE, None)
            )

            msg.error.return_value = _dict.get("error") or None

            return msg
