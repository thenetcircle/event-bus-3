from eventbus.http_sink import HttpSink, HttpSinkConfig
from eventbus.model import AbsSink, AbsSinkConfig, SinkType


class SinkFactory:
    @staticmethod
    def get_sink(sink_type: SinkType, sink_config: AbsSinkConfig, **kwargs) -> AbsSink:
        if sink_type == SinkType.HTTP:
            assert isinstance(
                sink_config, HttpSinkConfig
            ), "HTTP sink must have a HttpSinkConfig"
            return HttpSink(sink_config)

        raise ValueError("Invalid sink type")
