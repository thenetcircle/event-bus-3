from typing import Any, Dict

from eventbus.filter_transform import FilterTransform, FilterTransformParams
from eventbus.http_sink import HttpSink, HttpSinkParams
from eventbus.model import (
    AbsSink,
    AbsTransform,
    SinkType,
    TransformType,
)


class SinkFactory:
    @staticmethod
    def create_sink(sink_type: SinkType, sink_params: Dict[str, Any]) -> AbsSink:
        if sink_type == SinkType.HTTP:
            return HttpSink(HttpSinkParams(**sink_params))

        raise ValueError("Invalid sink type")

    @staticmethod
    def update_sink_params(sink: AbsSink, new_sink_params: Dict[str, Any]) -> Any:
        if sink.get_sink_type() == SinkType.HTTP:
            return sink.update_params(HttpSinkParams(**new_sink_params))

        raise ValueError("Invalid sink type")


class TransformFactory:
    @staticmethod
    def create_transform(
        transform_type: TransformType,
        transform_params: Dict[str, Any],
    ) -> AbsTransform:
        if transform_type == TransformType.FILTER:
            return FilterTransform(FilterTransformParams(**transform_params))

        raise ValueError("Invalid transform type")
