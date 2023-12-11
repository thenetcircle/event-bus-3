from eventbus.http_sink import HttpSink, HttpSinkParams
from eventbus.model import AbsSink, AbsSinkParams, SinkType


class SinkFactory:
    @staticmethod
    def get_sink(sink_type: SinkType, sink_config: AbsSinkParams, **kwargs) -> AbsSink:
        if sink_type == SinkType.HTTP:
            assert isinstance(
                sink_config, HttpSinkParams
            ), "HTTP sink must have a HttpSinkConfig"
            return HttpSink(sink_config)

        raise ValueError("Invalid sink type")
