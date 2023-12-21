import json
from typing import Optional

from loguru import logger
from kazoo.protocol.states import ZnodeStat

from eventbus import config
from eventbus.model import KafkaParams, SinkType, StoryParams, TransformType
from eventbus.zoo_client import ZooClient


class ZooDataParser:
    def __init__(self, zoo_client: ZooClient):
        self._zoo_client = zoo_client

    def get_story_params(
        self, story_id: str, story_data: bytes, znode_stats: ZnodeStat
    ) -> Optional[StoryParams]:
        try:
            story_path = config.get().zookeeper.story_path + "/" + story_id

            # check if it has `params` node

            # source
            source_data, _ = self._zoo_client.get(f"{story_path}/source")
            source_type, source_params = source_data.decode("utf-8").split("#", 1)
            assert source_type == "kafka"
            source_params = json.loads(source_params)
            topic_patterns = source_params.get("topic-pattern")
            if topic_patterns == "":
                topic_patterns = None
            kafka_params = KafkaParams(
                topics=source_params.get("topics"),
                topic_pattern=topic_patterns,
                group_id=source_params.get("group_id"),
                bootstrap_servers=source_params.get("bootstrap-servers"),
            )

            # sink
            sink_data, _ = self._zoo_client.get(f"{story_path}/sink")
            sink_type, sink_params = sink_data.decode("utf-8").split("#", 1)
            assert sink_type == "http"
            sink_params = json.loads(sink_params)
            sink_params = sink_params["default-request"]
            assert sink_params["method"] == "POST"

            http_sink_params = {"url": sink_params["uri"]}
            if sink_params.get("headers"):
                http_sink_params["headers"] = sink_params["headers"]
            sink = (SinkType.HTTP, http_sink_params)

            # transforms
            transforms = None
            if self._zoo_client.exists(f"{story_path}/operators"):
                op_data, _ = self._zoo_client.get(f"{story_path}/operators")
                op_type, op_params = op_data.decode("utf-8").split("#", 1)
                assert op_type == "filter"
                op_params = json.loads(op_params)
                transform_params = {}
                if op_params.get("event-name-white-list"):
                    transform_params["include_events"] = op_params[
                        "event-name-white-list"
                    ]
                if op_params.get("event-name-black-list"):
                    transform_params["exclude_events"] = op_params[
                        "event-name-black-list"
                    ]
                transforms = [(TransformType.FILTER, transform_params)]

            status, _ = self._zoo_client.get(f"{story_path}/status")
            assert status == b"INIT"

            story_params = StoryParams(
                id=story_id,
                kafka=kafka_params,
                sink=sink,
                transforms=transforms,
            )
            return story_params

        except Exception as ex:
            logger.error("parse story data error: {}", ex)
            return None
