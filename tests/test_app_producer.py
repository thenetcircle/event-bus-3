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
        topic_resolver.set_topic_mapping(
            [TopicMappingEntry(topic="event-v3-test", patterns=[".*"])]
        )
    )

    with TestClient(app) as client:
        yield client


def test_home(client: TestClient):
    response = client.get("/")
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
        "/main?resp_format=json",
        json={
            "id": "e1",
            "title": "normal_event",
            "published": "2021-08-01T18:57:44+02:00",
        },
    )
    assert response.status_code == 200
    assert response.json()["status"] == "all_succ"

    response = client.post(
        "/main?resp_format=json",
        json={
            "id": "e1",
            "title": "exceptional_event",
            "published": "2021-08-01T18:57:44+02:00",
        },
    )
    assert response.status_code == 200
    assert response.json()["status"] == "all_fail"


def test_event_format(client: TestClient):
    response = client.post(
        "/main?resp_format=json",
        json={"id": "e1"},
    )
    assert response.json()["status"] == "all_fail"
    assert "EventValidationError" in response.json()["details"]["root"]

    response = client.post(
        "/main?resp_format=json",
        json={
            "id": "e1",
            "title": "normal_event",
            "published": "2021-08-01",
        },
    )
    assert response.json()["status"] == "all_succ"


# TODO test published date format


def test_resp_format(client: TestClient):
    response = client.post(
        "/main",
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
        "/main?resp_format=json",
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
    assert response.status_code == 200
    assert response.json() == {
        "status": "part_succ",
        "details": {"e2": "<RuntimeError> exceptional_event"},
    }


def test_timeout(client: TestClient):
    response = client.post(
        "/main?resp_format=json&max_resp_time=0.2",
        json={
            "id": "e1",
            "title": "timeout_event",
            "published": "2021-08-01T18:57:44+02:00",
        },
    )
    assert response.status_code == 200
    assert response.json() == {
        "status": "all_fail",
        "details": {"e1": "<TimeoutError> "},
    }


# TODO test different sort of events
