from __future__ import annotations

import json

import pytest
from django.db import transaction

from djoutbox.models import PendingMessage
from djoutbox.publisher import OutboxMessage, bulk_publish, publish
from djoutbox.utils import tracking

from .utils import Person


@pytest.mark.django_db
def test_publish():
    publish("routing_key", "hello")

    msgs = list(PendingMessage.objects.values())
    assert len(msgs) == 1
    assert msgs[0]["routing_key"] == "routing_key"
    assert bytes(msgs[0]["body"]) == b'"hello"'
    assert msgs[0]["tracking_ids"] is not None


@pytest.mark.django_db
def test_bulk_publish():
    messages = [
        OutboxMessage("key_1", "body_1"),
        OutboxMessage("key_2", "body_2"),
    ]
    bulk_publish(messages)

    msgs = list(PendingMessage.objects.order_by("pk"))
    assert len(msgs) == 2
    assert bytes(msgs[0].body) == b'"body_1"'
    assert bytes(msgs[1].body) == b'"body_2"'


@pytest.mark.django_db
def test_serialize_dict():
    bulk_publish([OutboxMessage("k", {"a": 1})])

    msg = PendingMessage.objects.first()
    assert msg is not None
    assert bytes(msg.body) == b'{"a": 1}'


@pytest.mark.django_db
def test_serialize_bytes():
    bulk_publish([OutboxMessage("k", b"raw bytes")])

    msg = PendingMessage.objects.first()
    assert msg is not None
    assert bytes(msg.body) == b"raw bytes"


@pytest.mark.django_db
def test_serialize_pydantic():
    bulk_publish([OutboxMessage("k", Person(name="Alice"))])

    msg = PendingMessage.objects.first()
    assert msg is not None
    assert json.loads(bytes(msg.body)) == {"name": "Alice"}


@pytest.mark.django_db
def test_serialize_unserializable():
    class Opaque:
        pass

    with pytest.raises(ValueError, match="routing_key='bad'"):
        bulk_publish([OutboxMessage("bad", Opaque())])


@pytest.mark.django_db
def test_tracking_ids_chain():
    with tracking():
        publish("k", "v")

    msg = PendingMessage.objects.first()
    assert msg is not None
    assert len(msg.tracking_ids) == 2
    assert len(msg.tracking_ids[0]) == 36
    assert len(msg.tracking_ids[1]) == 36
    assert msg.tracking_ids[0] != msg.tracking_ids[1]


@pytest.mark.django_db
def test_eta():
    from datetime import timedelta

    eta = timedelta(hours=1)
    bulk_publish([OutboxMessage("k", "v", eta=eta)])

    msg = PendingMessage.objects.first()
    assert msg is not None
    assert msg.send_after > msg.created_at
    diff = msg.send_after - msg.created_at
    assert diff.total_seconds() >= 3599
    assert diff.total_seconds() <= 3601


@pytest.mark.django_db
def test_expiration():
    from datetime import timedelta

    exp = timedelta(minutes=5)
    bulk_publish([OutboxMessage("k", "v", expiration=exp)])

    msg = PendingMessage.objects.first()
    assert msg is not None
    assert msg.expiration is not None
    assert msg.expiration.total_seconds() == 300


@pytest.mark.django_db
def test_rollback():
    try:
        with transaction.atomic():
            publish("k", "v")
            raise RuntimeError("boom")
    except RuntimeError:
        pass

    assert PendingMessage.objects.count() == 0


@pytest.mark.django_db
def test_eta_none_send_after_equals_created_at():
    bulk_publish([OutboxMessage("k", "v")])

    msg = PendingMessage.objects.first()
    assert msg is not None
    assert msg.send_after == msg.created_at
    assert msg.expiration is None
