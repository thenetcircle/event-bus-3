import pytest

from eventbus.errors import EventValidationError
from eventbus.event import parse_request_body


def test_parse_request_body():
    req_body = """abc"""
    with pytest.raises(EventValidationError):
        parse_request_body(req_body)

    req_body = """{"verb": "login"}"""
    with pytest.raises(EventValidationError):
        parse_request_body(req_body)
