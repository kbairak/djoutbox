import re
import reprlib
import uuid
from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

import aio_pika
from aio_pika.abc import AbstractConnection


class Reject(Exception):
    pass


_body_repr = reprlib.Repr()
_body_repr.maxstring = 100
_body_repr.maxother = 200
_body_repr.maxlist = 5
_body_repr.maxdict = 5


def truncate_body(body: Any) -> str:
    return _body_repr.repr(body)


async def get_rmq_connection(rmq_connection_url: str) -> AbstractConnection:
    try:
        return await aio_pika.connect_robust(rmq_connection_url)
    except Exception as exc:
        raise ValueError(
            f"Failed to connect to RabbitMQ at '{rmq_connection_url}': {exc}\n"
            "Example: rmq_url='amqp://guest:guest@localhost/'"
        ) from exc


tracking_ids_contextvar: ContextVar[tuple[str, ...]] = ContextVar[tuple[str, ...]](
    "tracking_ids", default=()
)


def get_tracking_ids() -> tuple[str, ...]:
    return tracking_ids_contextvar.get()


@contextmanager
def tracking() -> Generator[None, None, None]:
    tracking_ids = tracking_ids_contextvar.get()
    tracking_ids = tracking_ids + (str(uuid.uuid4()),)
    token = tracking_ids_contextvar.set(tracking_ids)
    yield
    tracking_ids_contextvar.reset(token)


def parse_duration(s: str) -> int:
    if s in ("0", "0ms", "0s", "0m", "0h", "0d"):
        return 0

    match = re.search(r"^([^0]\d*d)?([^0]\d*h)?([^0]\d*m)?([^0]\d*s)?([^0]\d*ms)?$", s)
    if match is None:
        raise ValueError(f"Invalid duration string: {s!r}")
    days_string, hours_string, minutes_string, seconds_string, milliseconds_string = match.groups()

    result = 0

    if days_string:
        days = int(days_string[:-1])
        result += days * 24 * 60 * 60 * 1000

    if hours_string:
        hours = int(hours_string[:-1])
        if not 1 <= hours <= 23:
            raise ValueError(f"Invalid value for {hours=}, must be between 1 and 23")
        result += hours * 60 * 60 * 1000

    if minutes_string:
        minutes = int(minutes_string[:-1])
        if not 1 <= minutes <= 59:
            raise ValueError(f"Invalid value for {minutes=}, must be between 1 and 60")
        result += minutes * 60 * 1000

    if seconds_string:
        seconds = int(seconds_string[:-1])
        if not 1 <= seconds <= 59:
            raise ValueError(f"Invalid value for {seconds=}, must be between 1 and 60")
        result += seconds * 1000

    if milliseconds_string:
        milliseconds = int(milliseconds_string[:-2])
        if not 1 <= milliseconds <= 999:
            raise ValueError(f"Invalid value for {milliseconds=}, must be between 1 and 999")
        result += milliseconds

    if result == 0:
        raise ValueError(f"Invalid duration string: {s!r}")

    return result
