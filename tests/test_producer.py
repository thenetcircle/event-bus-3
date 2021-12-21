import pytest

from eventbus import config
from eventbus.errors import EventProduceError
from eventbus.producer import KafkaProducer
from tests.utils import create_event_from_dict


@pytest.mark.asyncio
async def test_produce_from_primary_producer(mock_internal_kafka_producer):
    primary_msg, secondary_msg = mock_internal_kafka_producer

    # test only primary producer
    producer = KafkaProducer()
    succ_event = create_event_from_dict({"topic": "primary-success"})
    result = await producer.produce(succ_event)
    assert result is primary_msg

    with pytest.raises(EventProduceError):
        fail_event = create_event_from_dict({"topic": "fail"})
        result = await producer.produce(fail_event)


@pytest.mark.asyncio
async def test_produce_from_both_producers(mock_internal_kafka_producer):
    primary_msg, secondary_msg = mock_internal_kafka_producer

    # add secondary brokers config
    old_config = config.get()
    new_config = old_config.copy(
        update={
            "kafka": old_config.kafka.copy(
                update={"secondary_brokers": "localhost:12182"}
            )
        },
        deep=True,
    )
    config.update_from_config(new_config)
    assert config.get().kafka.primary_brokers == "localhost:12181"
    assert config.get().kafka.secondary_brokers == "localhost:12182"

    # test result from two internal producers
    producer = KafkaProducer()
    primary_success_event = create_event_from_dict({"topic": "primary-success"})
    result = await producer.produce(primary_success_event)
    assert result is primary_msg

    secondary_success_event = create_event_from_dict({"topic": "secondary-success"})
    result = await producer.produce(secondary_success_event)
    assert result is secondary_msg

    with pytest.raises(EventProduceError):
        fail_event = create_event_from_dict({"topic": "other"})
        await producer.produce(fail_event)
