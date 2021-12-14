from typing import Dict

from eventbus.models import Event

_topic_mapping = {}


def update_topic_mapping(new_mapping: Dict[str, str]) -> None:
    pass


def resolve(event: Event) -> str:
    """resolve the topic name for an event"""
    pass
