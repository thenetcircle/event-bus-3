from typing import Any, Dict

from eventbus.filter_transform import FilterTransform
from eventbus.http_sink import HttpSink
from eventbus.model import (
    AbsSink,
    AbsTransform,
    FilterTransformParams,
    HttpSinkParams,
    SinkType,
    TransformType,
)


class SinkFactory:
    @staticmethod
    def create_sink(
        sink_type: SinkType, sink_id: str, sink_params: Dict[str, Any]
    ) -> AbsSink:
        if sink_type == SinkType.HTTP:
            return HttpSink(sink_id, HttpSinkParams(**sink_params))

        raise ValueError("Invalid sink type")


class TransformFactory:
    @staticmethod
    def create_transform(
        transform_type: TransformType,
        transform_id: str,
        transform_params: Dict[str, Any],
    ) -> AbsTransform:
        if transform_type == TransformType.FILTER:
            return FilterTransform(
                transform_id, FilterTransformParams(**transform_params)
            )

        raise ValueError("Invalid transform type")
