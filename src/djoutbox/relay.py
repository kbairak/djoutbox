from __future__ import annotations

import asyncio
import contextlib
import datetime
import json
import logging
import signal
import time
from typing import cast

import aio_pika
import asyncpg
from aio_pika.abc import DateType, HeadersType

from djoutbox import metrics
from djoutbox.log import logger
from djoutbox.partitions import ensure_partitions
from djoutbox.utils import get_rmq_connection

RETRY_DELAY_SECONDS = 10


class Relay:
    def __init__(
        self,
        *,
        db_dsn: str,
        rmq_url: str,
        exchange_name: str = "outbox",
        batch_size: int = 50,
        notification_timeout: float = 60.0,
        expiration: DateType | None = None,
        sent_archive_enabled: bool = False,
        sent_archive_granularity: str = "1d",
        partition_admin_interval: float = 300.0,
        **_ignored: object,
    ) -> None:
        self.db_dsn = db_dsn
        self.rmq_url = rmq_url
        self.exchange_name = exchange_name
        self.batch_size = batch_size
        self.notification_timeout = notification_timeout
        self.expiration = expiration
        self.sent_archive_enabled = sent_archive_enabled
        self.sent_archive_granularity = sent_archive_granularity
        self.partition_admin_interval = partition_admin_interval
        self._pool: asyncpg.Pool | None = None
        self._rmq_connection: aio_pika.abc.AbstractConnection | None = None
        self._shutdown_event = asyncio.Event()
        for key in _ignored:
            logger.debug("Relay ignoring unknown setting: %s", key)

    async def run(self) -> None:
        logging.basicConfig(level=logging.INFO)
        logger.setLevel(logging.INFO)

        self._pool = await asyncpg.create_pool(self.db_dsn, min_size=1, max_size=4)
        self._rmq_connection = await get_rmq_connection(self.rmq_url)
        channel = await self._rmq_connection.channel()
        exchange = await channel.declare_exchange(
            self.exchange_name, aio_pika.ExchangeType.TOPIC, durable=True
        )

        async with self._pool.acquire() as listen_conn:
            await ensure_partitions(listen_conn, self.sent_archive_granularity)

            logger.info(
                "Relay started — exchange=%s, batch_size=%s, notification_timeout=%ss",
                self.exchange_name,
                self.batch_size,
                self.notification_timeout,
            )

            notification_event = asyncio.Event()
            await listen_conn.add_listener("djoutbox_channel", lambda *_: notification_event.set())

            partition_task = asyncio.create_task(self._partition_admin_loop())

            loop = asyncio.get_event_loop()
            loop.add_signal_handler(signal.SIGINT, self._shutdown_event.set)
            loop.add_signal_handler(signal.SIGTERM, self._shutdown_event.set)

            try:
                await self._main_loop(exchange, notification_event)
            finally:
                partition_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await partition_task

    async def _main_loop(self, exchange, notification_event):
        while not self._shutdown_event.is_set():
            try:
                async with self._pool.acquire() as conn:
                    backlog = await conn.fetchval(
                        "SELECT COUNT(*) FROM djoutbox_pending WHERE send_after <= $1",
                        datetime.datetime.now(datetime.timezone.utc),
                    )
                    backlog = cast(int, backlog or 0)
                    metrics.table_backlog.labels(exchange_name=self.exchange_name).set(backlog)
                    if backlog > 100:
                        logger.warning("Outbox backlog: %d unsent messages", backlog)
                    elif backlog > 0:
                        logger.debug("Outbox backlog: %d unsent messages", backlog)

                await self._drain_batches(exchange)

                async with self._pool.acquire() as conn:
                    next_send = await conn.fetchval(
                        "SELECT MIN(send_after) FROM djoutbox_pending WHERE send_after > $1",
                        datetime.datetime.now(datetime.timezone.utc),
                    )

                now = datetime.datetime.now(datetime.timezone.utc)
                if next_send:
                    timeout = min(
                        (next_send - now).total_seconds(),
                        self.notification_timeout,
                    )
                else:
                    timeout = self.notification_timeout

                if timeout > 0:
                    try:
                        await asyncio.wait(
                            [
                                asyncio.create_task(
                                    asyncio.wait_for(notification_event.wait(), timeout=timeout)
                                ),
                                asyncio.create_task(self._shutdown_event.wait()),
                            ],
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                    except asyncio.TimeoutError:
                        pass
                    finally:
                        notification_event.clear()
            except Exception as exc:
                logger.error(
                    "Relay error: %s. Retrying in %ds...",
                    exc,
                    RETRY_DELAY_SECONDS,
                    exc_info=True,
                )
                await asyncio.sleep(RETRY_DELAY_SECONDS)

    async def _drain_batches(self, exchange):
        while True:
            async with self._pool.acquire() as conn, conn.transaction():
                count = await self._consume_batch(exchange, conn)
                if count == 0:
                    break

    async def _consume_batch(self, exchange, conn):
        poll_start = time.perf_counter()
        rows = await conn.fetch(
            """SELECT id, routing_key, body, tracking_ids, created_at, expiration
FROM djoutbox_pending
WHERE send_after <= $1
ORDER BY created_at
LIMIT $2
FOR UPDATE SKIP LOCKED""",
            datetime.datetime.now(datetime.timezone.utc),
            self.batch_size,
        )
        if not rows:
            return 0

        results = await asyncio.gather(
            *[
                exchange.publish(
                    aio_pika.Message(
                        body=row["body"],
                        content_type="application/json",
                        expiration=row["expiration"] or self.expiration,
                        headers=cast(
                            HeadersType,
                            {
                                "x-outbox-tracking-ids": (
                                    json.dumps(row["tracking_ids"])
                                    if isinstance(row["tracking_ids"], list)
                                    else row["tracking_ids"]
                                )
                            },
                        ),
                    ),
                    routing_key=row["routing_key"],
                )
                for row in rows
            ],
            return_exceptions=True,
        )

        successful_ids = []
        now = datetime.datetime.now(datetime.timezone.utc)
        for i, result in enumerate(results):
            row = rows[i]
            if isinstance(result, Exception):
                error_type = type(result).__name__
                metrics.publish_failures.labels(
                    exchange_name=self.exchange_name,
                    failure_type="main",
                    error_type=error_type,
                ).inc()
                logger.error(
                    "Failed to publish message id=%s routing_key=%s: %s: %s",
                    row["id"],
                    row["routing_key"],
                    error_type,
                    result,
                )
            else:
                successful_ids.append(row["id"])
                metrics.messages_published.labels(exchange_name=self.exchange_name).inc()
                age = (now - row["created_at"]).total_seconds()
                metrics.message_age.labels(exchange_name=self.exchange_name).observe(age)

        metrics.poll_duration.labels(exchange_name=self.exchange_name).observe(
            time.perf_counter() - poll_start
        )

        if successful_ids:
            if self.sent_archive_enabled:
                await conn.execute(
                    """WITH moved AS (
    DELETE FROM djoutbox_pending
    WHERE id = ANY($1::bigint[])
    RETURNING id, routing_key, body, tracking_ids, created_at, send_after, expiration
)
INSERT INTO djoutbox_sent
    (id, routing_key, body, tracking_ids, created_at, send_after, expiration, sent_at)
SELECT id, routing_key, body, tracking_ids, created_at, send_after, expiration, $2
FROM moved""",
                    successful_ids,
                    now,
                )
            else:
                await conn.execute(
                    "DELETE FROM djoutbox_pending WHERE id = ANY($1::bigint[])",
                    successful_ids,
                )

        return len(rows)

    async def _partition_admin_loop(self):
        while not self._shutdown_event.is_set():
            try:
                async with self._pool.acquire() as conn:
                    await ensure_partitions(conn, self.sent_archive_granularity)
            except Exception as exc:
                logger.error("Partition admin error: %s", exc, exc_info=True)
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(
                    self._shutdown_event.wait(), timeout=self.partition_admin_interval
                )
