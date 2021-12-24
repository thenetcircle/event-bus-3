import json

import pytest
from pytest_mock import MockFixture
from requests.models import Response
from starlette.testclient import TestClient

from eventbus.errors import NamespaceValidationError


@pytest.fixture()
def client(mock_internal_kafka_producer, mocker: MockFixture):
    """Need mock the Kafka producer first, otherwise the `librdkafka` will be inited,
    and the tests won't be finished"""
    mocker.patch("eventbus.config_watcher.watch_file")

    from eventbus.http_app import app

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


def test_invalid_namespace(client: TestClient):
    with pytest.raises(NamespaceValidationError, match="Namespace.*"):
        client.post("/new_events/invliad_ns", {})


def test_send_an_event(client: TestClient):
    response: Response = client.post(
        "/new_events/n1",
        json={
            "id": "test_event_1",
            "title": "test.primary-success",
            "published": "2021-08-01T18:57:44+02:00",
        },
    )
    assert response.status_code == 200
    assert response.content == b"ok"


def test_send_multiple_events(client: TestClient):
    response: Response = client.post(
        "/new_events/n1",
        json=[
            {
                "id": "test_event_1",
                "title": "test.primary-success",
                "published": "2021-08-01T18:57:44+02:00",
            },
            {
                "id": "test_event_2",
                "title": "test.primary-success",
                "published": "2021-08-01T18:58:44.154+02:00",
            },
            {
                "id": "test_event_3",
                "title": "test.primary-success",
                "published": "2021-08-01T18:59:44.123+02:00",
            },
        ],
    )
    assert response.status_code == 200
    assert response.content == b"ok"
