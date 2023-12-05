import pytest

from eventbus.errors import EventValidationError
from eventbus.event import parse_request_body


def test_parse_request_body():
    req_body = """abc"""
    with pytest.raises(EventValidationError):
        parse_request_body(req_body)

    req_body = """{"id": "login"}"""
    with pytest.raises(EventValidationError):
        parse_request_body(req_body)

    req_body = """{ \
"id": "id1", \
"title": "test_event", \
"published": "anything" \
}"""
    events = parse_request_body(req_body)
    assert events[0].id == "id1"
    assert events[0].published == "anything"

    req_body = """{ \
  "version": "1.0", \
  "content": "{\\"a\\": \\"testa\\", \\"b\\": \\"testb\\"}", \
  "actor": { \
    "id": "actorId", \
    "objectType": "actorType", \
    "content": "{\\"a\\": 1, \\"b\\": 2}", \
    "attachments": [ \
      { \
        "id": "attachmentId1", \
        "objectType": "attachmentType1", \
        "content": "abc", \
        "attachments": [ \
          { \
            "id": "subAttachmentId1", \
            "objectType": "subAttachmentType1", \
            "content": "subcontent" \
          } \
        ] \
      }, \
      { \
        "id": "attachmentId2", \
        "objectType": "attachmentType2", \
        "content": "def" \
      } \
    ] \
  }, \
  "object": { \
    "id": "objectId", \
    "objectType": "objectType" \
  }, \
  "target": { \
    "id": "targetId", \
    "objectType": "targetType" \
  }, \
  "provider": { \
    "id": "providerId", \
    "objectType": "providerType" \
  }, \
  "published": "2017-08-15T13:49:55Z", \
  "title": "message.send", \
  "verb": "send", \
  "id": "ED-providerId-message.send-actorId-59e704843e9cb", \
  "generator": { \
    "id": "tnc-event-dispatcher", \
    "objectType": "library", \
    "content": "{\\"mode\\":\\"async\\",\\"class\\":\\"dfEvent_Profile_Visit\\"}" \
  } \
}"""
    events = parse_request_body(req_body)
    assert events[0].id == "ED-providerId-message.send-actorId-59e704843e9cb"
    assert events[0].published == "2017-08-15T13:49:55Z"
