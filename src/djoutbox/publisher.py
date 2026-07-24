from __future__ import annotations

import json
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from aio_pika.abc import DateType
from aio_pika.message import encode_expiration
from django.utils import timezone

from djoutbox.models import PendingMessage
from djoutbox.utils import get_tracking_ids

try:
    from pydantic import BaseModel
except ImportError:
    if not TYPE_CHECKING:
        BaseModel = type(None)


@dataclass
class OutboxMessage:
    routing_key: str
    body: Any
    expiration: DateType | None = None
    eta: DateType | None = None


def _serialize_message(
    msg: OutboxMessage, now, tracking_ids
) -> PendingMessage:
    try:
        if isinstance(msg.body, BaseModel):
            body_bytes = msg.body.model_dump_json().encode()
        elif isinstance(msg.body, bytes):
            body_bytes = msg.body
        else:
            body_bytes = json.dumps(msg.body).encode()
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Cannot serialize message body for routing_key={msg.routing_key!r}: "
            f"{type(exc).__name__}: {exc}"
        ) from exc

    expiration_td = None
    if msg.expiration is not None:
        ms = encode_expiration(msg.expiration)
        if ms is None:
            raise ValueError(
                f"Invalid expiration for routing_key={msg.routing_key!r}"
            )
        expiration_td = timedelta(milliseconds=int(ms))

    send_after = now
    if msg.eta is not None:
        ms = encode_expiration(msg.eta)
        if ms is None:
            raise ValueError(
                f"Invalid eta for routing_key={msg.routing_key!r}"
            )
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


def publish(routing_key: str, body: Any, *, expiration=None, eta=None) -> None:
    bulk_publish([OutboxMessage(routing_key, body, expiration, eta)])


def bulk_publish(messages: Sequence[OutboxMessage]) -> None:
    now = timezone.now()
    tracking_ids = get_tracking_ids()
    instances = [_serialize_message(m, now, tracking_ids) for m in messages]
    PendingMessage.objects.bulk_create(instances)
