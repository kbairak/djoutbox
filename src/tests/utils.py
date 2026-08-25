from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Sequence

from aio_pika.abc import AbstractConnection

from djoutbox import Consumer, Worker


async def run_worker(worker: Worker, consumers: Sequence[Consumer], timeout: float) -> None:
    prev_consumers = worker.consumers
    worker.consumers = consumers
    with contextlib.suppress(asyncio.TimeoutError):
        await asyncio.wait_for(worker.run(), timeout=timeout)
    worker.consumers = prev_consumers


async def get_dlq_message_count(rmq_connection: AbstractConnection, queue_name: str) -> int:
    channel = await rmq_connection.channel()
    dlq = await channel.get_queue(f"{queue_name}.dlq")
    message_count = dlq.declaration_result.message_count
    assert message_count is not None
    return message_count
