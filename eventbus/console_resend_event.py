from eventbus import config
import asyncio
from datetime import datetime
from typing import List, Optional
from eventbus.utils import setup_logger
from eventbus.event import parse_aiokafka_msg
from eventbus.http_sink import HttpSink, HttpSinkParams
from aiokafka import AIOKafkaConsumer, ConsumerRecord, TopicPartition
from loguru import logger


async def create_sink(sink: str, headers: Optional[List[str]] = None) -> HttpSink:
    params = {"url": sink}
    if headers:
        params["headers"] = dict([h.split("=", 1) for h in headers])
    sink = HttpSink(HttpSinkParams(**params))
    await sink.init()
    return sink


async def main(
    topics: List[str],
    sink: str,
    start_time: datetime,
    end_time: Optional[datetime] = None,
    headers: Optional[List[str]] = None,
):
    default_kafka_params = config.get().default_kafka_params.consumer
    consumer: AIOKafkaConsumer = None
    http_sink: HttpSink = None
    try:
        consumer = AIOKafkaConsumer(*topics, **default_kafka_params)
        await consumer.start()

        tps = [
            TopicPartition(topic=_t, partition=_p)
            for _t in topics
            for _p in consumer.partitions_for_topic(_t)
        ]

        tps_offsets = await consumer.offsets_for_times(
            dict([(tp, start_time.timestamp()) for tp in tps])
        )

        for tp, offset in tps_offsets.items():
            logger.info("TopicPartition: {}, Offset: {}", tp, offset)
            consumer.seek(tp, offset.offset)

        http_sink = await create_sink(sink, headers)

        while True:
            try:
                msg = await asyncio.wait_for(consumer.getone(), timeout=10)
            except asyncio.TimeoutError:
                logger.info("Timeout reached, break")
                break
            except:
                logger.exception("Error occurred while consuming message")
                break

            if end_time and msg.timestamp > end_time.timestamp():
                pause_tp = TopicPartition(msg.topic, msg.partition)
                consumer.pause(pause_tp)
                logger.info("End time reached, pause tp {}", pause_tp)
                continue

            event = parse_aiokafka_msg(msg)
            result = await http_sink.send_event(event)
            logger.info('Sent event to sink with result: "{}"', result)

    except Exception:
        logger.exception("Error occurred")

    finally:
        if consumer:
            await consumer.stop()
        if http_sink:
            await http_sink.close()


if __name__ == "__main__":
    import argparse

    def parse_datetime(time_string):
        """Converts a string in the format 'YYYY-MM-DDTHH:MM:SS' into a datetime object."""
        try:
            return datetime.strptime(time_string, "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            msg = f"Not a valid date-time: '{time_string}'. Expected format: YYYY-MM-DDTHH:MM:SS."
            raise argparse.ArgumentTypeError(msg)

    parser = argparse.ArgumentParser(
        description="EventBus v3 - Send Dead Letter Events"
    )
    parser.add_argument(
        "-c",
        "--config_file",
        help="Config file path. If not specified, it will look for environment variable `EB_CONF_FILE`",
    )
    parser.add_argument(
        "-t",
        "--topics",
        required=True,
        nargs="+",
        help="The Kafka topics to consume",
    )
    parser.add_argument(
        "-s",
        "--sink",
        required=True,
        help="The http sink to send",
    )
    parser.add_argument(
        "--start_time",
        help="Start time in the format YYYY-MM-DDTHH:MM:SS",
        required=True,
        type=parse_datetime,
    )
    parser.add_argument(
        "--headers",
        help="The headers in the sink",
        nargs="+",
        default=None,
    )
    parser.add_argument(
        "--end_time",
        help="End time in the format YYYY-MM-DDTHH:MM:SS",
        type=parse_datetime,
        default=None,
    )
    args = parser.parse_args()

    if args.config_file:
        config.update_from_yaml(args.config_file)
    else:
        config.load_from_environ()

    asyncio.run(
        main(args.topics, args.sink, args.start_time, args.end_time, args.headers)
    )
