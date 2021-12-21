import pytest
from pytest_mock import MockFixture
from requests.models import Response
from starlette.testclient import TestClient


@pytest.fixture()
def client(mocker: MockFixture):
    mocker.patch("eventbus.config_watcher.watch_file")
    from eventbus.http_app import app

    client = TestClient(app)
    yield client


def test_home(client: TestClient):
    response: Response = client.get("/")
    assert response.status_code == 200
    assert response.content == b"running"
