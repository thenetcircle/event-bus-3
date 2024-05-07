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
    wait_time: int = 10,
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

        start_time_timestamp = int(start_time.timestamp() * 1000)
        logger.info(
            'Seeking start_time offsets: "{}", "{}"', start_time, start_time_timestamp
        )
        tps_offsets = await consumer.offsets_for_times(
            dict([(tp, start_time_timestamp) for tp in tps])
        )

        for tp, offset in tps_offsets.items():
            logger.info("TopicPartition: {}, Offset: {}", tp, offset)
            if offset is None or offset.timestamp < start_time_timestamp:
                consumer.pause(tp)
            else:
                consumer.seek(tp, offset.offset)

        http_sink = await create_sink(sink, headers)

        total_send_amount = 0

        while True:
            try:
                msg = await asyncio.wait_for(consumer.getone(), timeout=wait_time)
            except asyncio.TimeoutError:
                logger.info("Timeout reached, break")
                break
            except:
                logger.exception("Error occurred while consuming message")
                break

            if end_time and msg.timestamp > int(end_time.timestamp() * 1000):
                pause_tp = TopicPartition(msg.topic, msg.partition)
                consumer.pause(pause_tp)
                logger.info("End time reached, pause tp {}", pause_tp)
                continue

            event = parse_aiokafka_msg(msg)
            result = await http_sink.send_event(event)
            logger.info('Sent event to sink with result: "{}"', result)
            total_send_amount += 1

    except Exception:
        logger.exception("Error occurred")

    finally:
        logger.info('Total sent amount: "{}"', total_send_amount)
        if consumer:
            await consumer.stop()
        if http_sink:
            await http_sink.close()


if __name__ == "__main__":
    import argparse

    def parse_datetime(time_string):
        """Parse a datetime string with timezone specified as +HHMM or -HHMM."""
        # Typical formats could include something like '2021-03-30T14:17:36+0200'
        fmt = "%Y-%m-%dT%H:%M:%S%z"  # '%z' parses timezone offsets like '+0200'
        try:
            return datetime.strptime(time_string, fmt)
        except ValueError:
            raise argparse.ArgumentTypeError(
                f"Time should be in the ISO format with timezone: YYYY-MM-DDTHH:MM:SS+HHMM. Got {time_string} instead."
            )

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
    parser.add_argument(
        "--wait_time",
        help="Max wait time in seconds (default: 10)",
        type=int,
        default=10,
    )
    args = parser.parse_args()

    if args.config_file:
        config.update_from_yaml(args.config_file)
    else:
        config.load_from_environ()

    asyncio.run(
        main(
            args.topics,
            args.sink,
            args.start_time,
            args.wait_time,
            args.end_time,
            args.headers,
        )
    )
