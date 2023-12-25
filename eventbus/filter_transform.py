from typing import List, Optional

import re
from eventbus.event import KafkaEvent
from eventbus.model import AbsTransform, FilterTransformParams


class FilterTransform(AbsTransform):
    def __init__(self, params: FilterTransformParams):
        assert params.include_events or params.exclude_events, "Empty filter params"

        self._params = params

    async def init(self):
        pass

    async def close(self):
        pass

    async def run(self, event: KafkaEvent) -> Optional[KafkaEvent]:
        if self._params.include_events:
            if not self._match_event_title(self._params.include_events, event):
                return None

        if self._params.exclude_events:
            if self._match_event_title(self._params.exclude_events, event):
                return None

        return event

    @staticmethod
    def _match_event_title(patterns: List[str], event: KafkaEvent) -> bool:
        for p in patterns:
            if re.match(re.compile(p, re.I), event.title):
                return True
        return False
