import asyncio
import json

import pytest
from pytest_mock import MockFixture
from starlette.testclient import TestClient

from eventbus.topic_resolver import TopicMappingEntry


@pytest.fixture()
def client(mocker: MockFixture):
    """Need mock the Kafka producer first, otherwise the `librdkafka` will be inited,
    and the tests won't be finished"""

    async def send_and_wait(self, topic, value, **kwargs) -> None:
        data = json.loads(value)

        if data["title"] == "normal_event":
            pass
        if data["title"] == "timeout_event":
            await asyncio.sleep(0.5)
        elif data["title"] == "exceptional_event":
            raise RuntimeError("exceptional_event")

    mocker.patch("eventbus.config.load_from_environ")
    mocker.patch("eventbus.aio_zoo_client.AioZooClient.init")
    mocker.patch("eventbus.aio_zoo_client.AioZooClient.watch_data")
    mocker.patch("eventbus.aio_zoo_client.AioZooClient.close")
    mocker.patch("aiokafka.producer.producer.AIOKafkaProducer.start")
    mocker.patch(
        "aiokafka.producer.producer.AIOKafkaProducer.send_and_wait", send_and_wait
    )

    from eventbus.app_producer import app, topic_resolver

    asyncio.run(
        topic_resolver.set_topic_mappings(
            [TopicMappingEntry(topic="event-v3-test", patterns=[".*"])]
        )
    )

    with TestClient(app) as client:
        yield client


def check_event(client: TestClient, request_json, expect_succ=True):
    response = client.post(
        "/",
        json=request_json,
    )
    if expect_succ:
        assert response.status_code == 200
        assert response.content == b"ok"
    else:
        assert response.status_code != 200
        assert response.content != b"ok"


def test_health_check(client: TestClient):
    response = client.get("/check")
    assert response.status_code == 200
    assert response.content == b"running"


def test_show_config(client: TestClient):
    response = client.get("/config")
    assert response.status_code == 200
    assert response.headers.get("content-type") == "application/json"
    resp_json = json.loads(response.content)
    assert resp_json["app"]["env"] == "test"


def test_send_an_event(client: TestClient):
    response = client.post(
        "/?resp_format=json",
        json={
            "id": "e1",
            "title": "normal_event",
            "published": "2021-08-01T18:57:44+02:00",
        },
    )
    assert response.status_code == 200
    assert response.json()["status"] == "all_succ"

    response = client.post(
        "/?resp_format=json",
        json={
            "id": "e1",
            "title": "exceptional_event",
            "published": "2021-08-01T18:57:44+02:00",
        },
    )
    assert response.status_code != 200
    assert response.json()["status"] == "all_fail"


def test_event_format(client: TestClient):
    response = client.post(
        "/?resp_format=json",
        json={"id": "e1"},
    )
    assert response.json()["status"] == "all_fail"
    assert "EventValidationError" in response.json()["details"]["root"]

    response = client.post(
        "/?resp_format=json",
        json={
            "id": "e1",
            "title": "normal_event",
            "published": "2021-08-01",
        },
    )
    assert response.json()["status"] == "all_succ"


def test_resp_format(client: TestClient):
    response = client.post(
        "/",
        json={
            "id": "e1",
            "title": "normal_event",
            "published": "2021-08-01T18:57:44+02:00",
        },
    )
    assert response.status_code == 200
    assert response.content == b"ok"


def test_send_multiple_events(client: TestClient):
    response = client.post(
        "/?resp_format=json",
        json=[
            {
                "id": "e1",
                "title": "normal_event",
                "published": "2021-08-01T18:57:44+02:00",
            },
            {
                "id": "e2",
                "title": "exceptional_event",
                "published": "2021-08-01T18:58:44.154+02:00",
            },
            {
                "id": "e3",
                "title": "normal_event",
                "published": "2021-08-01T18:59:44.123+02:00",
            },
        ],
    )
    assert response.status_code != 200
    assert response.json() == {
        "status": "part_succ",
        "details": {"e2": "<RuntimeError> exceptional_event"},
    }


def test_timeout(client: TestClient):
    response = client.post(
        "/?resp_format=json&max_resp_time=0.2",
        json={
            "id": "e1",
            "title": "timeout_event",
            "published": "2021-08-01T18:57:44+02:00",
        },
    )
    assert response.status_code != 200
    assert response.json() == {
        "status": "all_fail",
        "details": {"e1": "<TimeoutError> "},
    }


# === Test different sort of events ===
def test_non_json_event(client: TestClient):
    check_event(client, "abc", False)


def test_no_id_event(client: TestClient):
    check_event(client, {"verb": "login"}, False)
    check_event(client, {"title": "user.login"}, False)


def test_normal_event(client: TestClient):
    check_event(
        client,
        {
            "version": "1.0",
            "id": "ED-providerId-message.send-actorId-59e704843e9cb",
            "title": "message.send",
            "verb": "send",
            "actor": {"id": "actorId", "objectType": "actorType"},
            "provider": {"id": "providerId", "objectType": "providerType"},
            "published": "2017-08-15T13:49:55Z",
        },
    )


def test_real_event(client: TestClient):
    check_event(
        client,
        {
            "version": "1.0",
            "content": '{"a": "testa", "b": "testb"}',
            "actor": {
                "id": "actorId",
                "objectType": "actorType",
                "content": '{"a": 1, "b": 2}',
                "attachments": [
                    {
                        "id": "attachmentId1",
                        "objectType": "attachmentType1",
                        "content": "abc",
                        "attachments": [
                            {
                                "id": "subAttachmentId1",
                                "objectType": "subAttachmentType1",
                                "content": "subcontent",
                            }
                        ],
                    },
                    {
                        "id": "attachmentId2",
                        "objectType": "attachmentType2",
                        "content": "def",
                    },
                ],
            },
            "object": {"id": "objectId", "objectType": "objectType"},
            "target": {"id": "targetId", "objectType": "targetType"},
            "provider": {"id": "providerId", "objectType": "providerType"},
            "published": "2017-08-15T13:49:55Z",
            "title": "message.send",
            "verb": "send",
            "id": "ED-providerId-message.send-actorId-59e704843e9cb",
            "generator": {
                "id": "tnc-event-dispatcher",
                "objectType": "library",
                "content": '{"mode":"async","class":"dfEvent_Profile_Visit"}',
            },
        },
    )
