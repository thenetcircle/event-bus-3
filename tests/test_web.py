import json
from threading import Thread

import pytest
from pytest_mock import MockFixture
from requests.models import Response
from starlette.testclient import TestClient
from utils import create_kafka_message_from_dict


@pytest.fixture()
def client(mocker: MockFixture):
    """Need mock the Kafka producer first, otherwise the `librdkafka` will be inited,
    and the tests won't be finished"""

    def produce_mock(self, topic, value, **kwargs) -> None:
        def delivery(err, msg):
            Thread(target=kwargs["on_delivery"], args=(err, msg), daemon=True).start()

        data = json.loads(value)

        if data["title"] == "normal_event":
            msg = create_kafka_message_from_dict({})
            delivery(None, msg)
        elif data["title"] == "exceptional_event":
            delivery(RuntimeError("exceptional_event"), None)

    mocker.patch("eventbus.config_watcher.async_watch_config_file")
    mocker.patch("eventbus.config.load_from_environ")
    mocker.patch("eventbus.producer.KafkaProducer.init")
    mocker.patch("eventbus.producer.KafkaProducer.produce", produce_mock)

    from eventbus.main_producer import app

    with TestClient(app) as client:
        yield client


def test_home(client: TestClient):
    response: Response = client.get("/")
    assert response.status_code == 200
    assert response.content == b"running"


def test_show_config(client: TestClient):
    response: Response = client.get("/config")
    assert response.status_code == 200
    assert response.headers.get("content-type") == "application/json"
    resp_json = json.loads(response.content)
    assert resp_json["env"] == "test"


def test_send_an_event(client: TestClient):
    response: Response = client.post(
        "/new_events",
        json={
            "id": "e1",
            "title": "normal_event",
            "published": "2021-08-01T18:57:44+02:00",
        },
    )
    assert response.status_code == 200
    assert response.json()["status"] == "all_succ"

    response: Response = client.post(
        "/new_events",
        json={
            "id": "e1",
            "title": "exceptional_event",
            "published": "2021-08-01T18:57:44+02:00",
        },
    )
    assert response.status_code == 200
    assert response.json()["status"] == "all_fail"


def test_event_format(client: TestClient):
    response: Response = client.post(
        "/new_events",
        json={"id": "e1"},
    )
    assert response.json()["status"] == "all_fail"
    assert "EventValidationError" in response.json()["details"]["root"]

    response: Response = client.post(
        "/new_events",
        json={
            "id": "e1",
            "title": "normal_event",
            "published": "2021-08-01",
        },
    )
    assert response.json()["status"] == "all_fail"
    assert "EventValidationError" in response.json()["details"]["root"]


# TODO test published date format


def test_resp_format(client: TestClient):
    response: Response = client.post(
        "/new_events?resp_format=2",
        json={
            "id": "e1",
            "title": "normal_event",
            "published": "2021-08-01T18:57:44+02:00",
        },
    )
    assert response.status_code == 200
    assert response.content == b"ok"


def test_send_multiple_events(client: TestClient):
    response: Response = client.post(
        "/new_events",
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
        "details": {"e2": "<KafkaException> exceptional_event"},
    }


def test_timeout(client: TestClient):
    response: Response = client.post(
        "/new_events",
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
