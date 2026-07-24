from djoutbox.relay import Relay
from djoutbox.utils import Reject, get_tracking_ids, tracking
from djoutbox.worker import Consumer, Worker, consume

__all__ = [
    "Consumer",
    "Worker",
    "consume",
    "Relay",
    "Reject",
    "get_tracking_ids",
    "tracking",
    "publish",
    "bulk_publish",
    "OutboxMessage",
]


def __getattr__(name):
    if name in ("publish", "bulk_publish", "OutboxMessage"):
        from djoutbox import publisher

        return getattr(publisher, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
