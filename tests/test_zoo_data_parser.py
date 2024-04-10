import pytest
from eventbus.model import SinkType
from eventbus import config
from eventbus.zoo_data_parser import ZooDataParser
from eventbus.zoo_client import ZooClient


@pytest.fixture
def zoo_data_parser():
    zoo_client = ZooClient(
        hosts=config.get().zookeeper.hosts,
        timeout=config.get().zookeeper.timeout,
    )
    zoo_client.init()
    yield ZooDataParser(zoo_client)
    zoo_client.close()


def test_parse_sink_params(zoo_data_parser: ZooDataParser):
    # v3 format
    result = zoo_data_parser.parse_sink_params(
        'http#{"method":"POST","uri":"http://localhost:8081/receive_events","headers": {"Content-type": "application/json"},"timeout": 3,"max_retry_times":0}'
    )
    assert result[0] == SinkType.HTTP
    assert result[1] == {
        "url": "http://localhost:8081/receive_events",
        "method": "POST",
        "headers": {"Content-type": "application/json"},
        "timeout": 3,
        "max_retry_times": 0,
    }

    # v2 format
    result = zoo_data_parser.parse_sink_params(
        'http#{"default-request":{"method":"POST","uri":"http://rose.kevin.poppen2.lab/api/internal/eventbus/receiver","headers":{"accept":"application/json;version=1"}}}'
    )
    assert result[0] == SinkType.HTTP
    assert result[1] == {
        "url": "http://rose.kevin.poppen2.lab/api/internal/eventbus/receiver",
        "method": "POST",
        "headers": {"accept": "application/json;version=1"},
    }
