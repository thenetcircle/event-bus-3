import json
from typing import Any, Dict, List, Tuple

from loguru import logger

from eventbus import config
from eventbus.model import SinkType, TransformType
from eventbus.story import StoryParams, StoryStatus
from eventbus.zoo_client import ZooClient


class ZooDataParser:
    def __init__(self, zoo_client: ZooClient):
        self._zoo_client = zoo_client

    def create_story_params(self, story_id: str) -> StoryParams:
        try:
            story_path = self.get_stories_path() + "/" + story_id

            # check if it has `params` node

            # source
            source_data, _ = self._zoo_client.get(f"{story_path}/source")
            source_type, source_params = source_data.decode("utf-8").split("#", 1)
            assert source_type == "kafka"
            source_params = json.loads(source_params)
            consumer_params = {}
            if source_params.get("topics", "") != "":
                consumer_params["topics"] = source_params.get("topics")
            if source_params.get("topic-pattern", "") != "":
                consumer_params["topic_pattern"] = source_params.get("topic-pattern")
            if source_params.get("group-id", "") != "":
                consumer_params["group_id"] = source_params.get("group-id")
            # if source_params.get("bootstrap-servers", "") != "":
            #     consumer_params["bootstrap_servers"] = source_params.get(
            #         "bootstrap-servers"
            #     )

            # sink
            sink_data, _ = self._zoo_client.get(f"{story_path}/sink")
            sink = self.parse_sink_params(sink_data.decode("utf-8"))

            # transforms
            transforms = None
            if self._zoo_client.exists(f"{story_path}/operators"):
                op_data, _ = self._zoo_client.get(f"{story_path}/operators")
                op_type, op_params = op_data.decode("utf-8").split("#", 1)
                assert op_type == "filter"
                op_params = json.loads(op_params)
                transform_params = {}
                if op_params.get("event-name-white-list", "") != "":
                    transform_params["include_events"] = op_params[
                        "event-name-white-list"
                    ]
                if op_params.get("event-name-black-list", "") != "":
                    transform_params["exclude_events"] = op_params[
                        "event-name-black-list"
                    ]
                transforms = [(TransformType.FILTER, transform_params)]

            _status, _ = self._zoo_client.get(f"{story_path}/status")
            # assert _status == b"INIT"
            if _status == b"DISABLED":
                status = StoryStatus.DISABLED
            else:
                status = StoryStatus.NORMAL

            story_params = StoryParams(
                id=story_id,
                consumer_params=consumer_params,
                sink=sink,
                transforms=transforms,
                status=status,
            )
            return story_params

        except Exception as ex:
            logger.exception("Parse story params from Zookeeper failed")
            raise

    @staticmethod
    def get_stories_path() -> str:
        return f"{config.get().zookeeper.root_path}/stories"

    @staticmethod
    def get_topics_path() -> str:
        return f"{config.get().zookeeper.root_path}/topics"

    @staticmethod
    def get_sinks_path() -> str:
        return f"{config.get().zookeeper.root_path}/sinks"

    @staticmethod
    def get_v2_runner_stories_path() -> List[Tuple[str, str]]:
        v2_runners_config = config.get().v2_runners
        if not v2_runners_config:
            return []

        project_id = config.get().app.project_id
        if project_id in v2_runners_config:
            v2_runners = v2_runners_config[project_id]
        elif "all" in v2_runners_config:
            v2_runners = v2_runners_config["all"]
        else:
            return []

        return [
            (
                v2_runner,
                f"{config.get().zookeeper.root_path}/runners/{v2_runner}/stories",
            )
            for v2_runner in v2_runners
        ]

    @staticmethod
    def parse_sink_params(sink_data: str) -> Tuple[SinkType, Dict[str, Any]]:
        sink_type, v2_sink_params = sink_data.split("#", 1)

        if sink_type == "http":
            v2_sink_params = json.loads(v2_sink_params)
            if "default-request" in v2_sink_params:
                v2_sink_params = v2_sink_params["default-request"]

            sink_params = {
                "url": v2_sink_params["uri"],
                "method": v2_sink_params["method"].upper(),
            }
            other_params = [
                "headers",
                "timeout",
                "max_retry_times",
                "exp_backoff_factor",
                "exp_backoff_max_delay",
            ]
            for param in other_params:
                if v2_sink_params.get(param, "") != "":
                    sink_params[param] = v2_sink_params[param]
            return (SinkType.HTTP, sink_params)
        else:
            raise ValueError("Invalid sink type")
