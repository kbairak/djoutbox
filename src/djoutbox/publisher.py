from __future__ import annotations

import json
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from aio_pika.abc import DateType
from aio_pika.message import encode_expiration
from django.utils import timezone

from djoutbox.models import PendingMessage
from djoutbox.utils import get_tracking_ids


@dataclass
class OutboxMessage:
    routing_key: str
    body: Any
    expiration: DateType | None = None
    eta: DateType | None = None


def _serialize_message(
    msg: OutboxMessage, now: datetime, tracking_ids: tuple[str, ...]
) -> PendingMessage:
    try:
        from django.conf import settings

        serializers = getattr(settings, "DJOUTBOX", {}).get("serializers", ())
        for type_, serializer, _ in serializers:
            if isinstance(msg.body, type_):
                body_bytes = serializer(msg.body)
                break
        else:
            body_bytes = msg.body if isinstance(msg.body, bytes) else json.dumps(msg.body).encode()
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Cannot serialize message body for routing_key={msg.routing_key!r}: "
            f"{type(exc).__name__}: {exc}"
        ) from exc

    expiration_td = None
    if msg.expiration is not None:
        ms = encode_expiration(msg.expiration)
        if ms is None:
            raise ValueError(f"Invalid expiration for routing_key={msg.routing_key!r}")
        expiration_td = timedelta(milliseconds=int(ms))

    send_after = now
    if msg.eta is not None:
        ms = encode_expiration(msg.eta)
        if ms is None:
            raise ValueError(f"Invalid eta for routing_key={msg.routing_key!r}")
        send_after = now + timedelta(milliseconds=int(ms))

    ids = list(tracking_ids + (str(uuid.uuid4()),))

    return PendingMessage(
        routing_key=msg.routing_key,
        body=body_bytes,
        tracking_ids=ids,
        created_at=now,
        send_after=send_after,
        expiration=expiration_td,
    )


def publish(
    routing_key: str, body: Any, *, expiration: DateType | None = None, eta: DateType | None = None
) -> None:
    bulk_publish([OutboxMessage(routing_key, body, expiration, eta)])


def bulk_publish(messages: Sequence[OutboxMessage]) -> None:
    now = timezone.now()
    tracking_ids = get_tracking_ids()
    instances = [_serialize_message(m, now, tracking_ids) for m in messages]
    PendingMessage.objects.bulk_create(instances)
