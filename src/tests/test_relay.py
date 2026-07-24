from __future__ import annotations

import datetime

import aio_pika
import asyncpg
import pytest
import pytest_asyncio

from djoutbox.relay import Relay

INSERT_PENDING = (
    "INSERT INTO djoutbox_pending"
    " (routing_key, body, tracking_ids, created_at, send_after)"
    " VALUES ($1, $2, $3, $4, $5)"
)


@pytest_asyncio.fixture(autouse=True)
async def cleanup_relay(db_connection: asyncpg.Connection):
    yield
    await db_connection.execute("TRUNCATE TABLE djoutbox_pending CASCADE")
    await db_connection.execute("TRUNCATE TABLE djoutbox_sent CASCADE")


@pytest.mark.asyncio
async def test_consume_batch(
    db_connection: asyncpg.Connection,
    rmq_connection: aio_pika.abc.AbstractConnection,
):
    relay = Relay(db_dsn="", rmq_url="", batch_size=10)

    channel = await rmq_connection.channel()
    exchange = await channel.declare_exchange(
        "outbox", aio_pika.ExchangeType.TOPIC, durable=True,
    )
    queue = await channel.declare_queue("", exclusive=True)
    await queue.bind(exchange, routing_key="test_key")

    now = datetime.datetime.now(datetime.timezone.utc)
    await db_connection.execute(
        INSERT_PENDING,
        "test_key", b'"test_body"', '[]', now, now,
    )

    async with db_connection.transaction():
        count = await relay._consume_batch(exchange, db_connection)

    assert count == 1

    inbox_msg = await queue.get(timeout=5)
    assert inbox_msg is not None
    assert inbox_msg.body == b'"test_body"'
    assert inbox_msg.content_type == "application/json"
    await inbox_msg.ack()

    archived = await db_connection.fetchrow("SELECT * FROM djoutbox_sent")
    assert archived is not None
    assert archived["routing_key"] == "test_key"
    assert archived["body"] == b'"test_body"'
    assert archived["sent_at"] is not None

    remaining = await db_connection.fetchval("SELECT COUNT(*) FROM djoutbox_pending")
    assert remaining == 0


@pytest.mark.asyncio
async def test_consume_batch_batch_limit(
    db_connection: asyncpg.Connection,
    rmq_connection: aio_pika.abc.AbstractConnection,
):
    relay = Relay(db_dsn="", rmq_url="", batch_size=2)

    channel = await rmq_connection.channel()
    exchange = await channel.declare_exchange(
        "outbox", aio_pika.ExchangeType.TOPIC, durable=True,
    )

    now = datetime.datetime.now(datetime.timezone.utc)
    for i in range(3):
        await db_connection.execute(
            INSERT_PENDING,
            f"key_{i}", f'"body_{i}"'.encode(), '[]', now, now,
        )

    async with db_connection.transaction():
        count = await relay._consume_batch(exchange, db_connection)

    assert count == 2

    archived = await db_connection.fetch("SELECT id, routing_key FROM djoutbox_sent ORDER BY id")
    assert len(archived) == 2
    assert archived[0]["routing_key"] == "key_0"
    assert archived[1]["routing_key"] == "key_1"

    remaining = await db_connection.fetchval("SELECT COUNT(*) FROM djoutbox_pending")
    assert remaining == 1


@pytest.mark.asyncio
async def test_drain_batches(
    db_connection: asyncpg.Connection,
    rmq_connection: aio_pika.abc.AbstractConnection,
    db_settings,
):
    from django.conf import settings as dj_settings
    db = dj_settings.DATABASES["default"]
    url = f"postgresql://{db['USER']}:{db['PASSWORD']}@{db['HOST']}:{db['PORT']}/{db['NAME']}"
    relay = Relay(db_dsn="", rmq_url="", batch_size=2)
    relay._pool = await asyncpg.create_pool(url)

    channel = await rmq_connection.channel()
    exchange = await channel.declare_exchange(
        "outbox", aio_pika.ExchangeType.TOPIC, durable=True,
    )

    now = datetime.datetime.now(datetime.timezone.utc)
    for i in range(3):
        await db_connection.execute(
            INSERT_PENDING,
            f"key_{i}", f'"body_{i}"'.encode(), '[]', now, now,
        )

    try:
        await relay._drain_batches(exchange)
    finally:
        await relay._pool.close()

    archived = await db_connection.fetchval("SELECT COUNT(*) FROM djoutbox_sent")
    assert archived == 3

    remaining = await db_connection.fetchval("SELECT COUNT(*) FROM djoutbox_pending")
    assert remaining == 0


@pytest.mark.asyncio
async def test_consume_batch_empty(
    db_connection: asyncpg.Connection,
    rmq_connection: aio_pika.abc.AbstractConnection,
):
    relay = Relay(db_dsn="", rmq_url="", batch_size=10)

    channel = await rmq_connection.channel()
    exchange = await channel.declare_exchange(
        "outbox", aio_pika.ExchangeType.TOPIC, durable=True,
    )

    async with db_connection.transaction():
        count = await relay._consume_batch(exchange, db_connection)

    assert count == 0


@pytest.mark.asyncio
async def test_consume_batch_no_archive(
    db_connection: asyncpg.Connection,
    rmq_connection: aio_pika.abc.AbstractConnection,
):
    relay = Relay(db_dsn="", rmq_url="", batch_size=10, sent_archive_enabled=False)

    channel = await rmq_connection.channel()
    exchange = await channel.declare_exchange(
        "outbox", aio_pika.ExchangeType.TOPIC, durable=True,
    )
    queue = await channel.declare_queue("", exclusive=True)
    await queue.bind(exchange, routing_key="test_key")

    now = datetime.datetime.now(datetime.timezone.utc)
    await db_connection.execute(
        INSERT_PENDING,
        "test_key", b'"test_body"', '[]', now, now,
    )

    async with db_connection.transaction():
        count = await relay._consume_batch(exchange, db_connection)

    assert count == 1

    inbox_msg = await queue.get(timeout=5)
    assert inbox_msg is not None
    await inbox_msg.ack()

    archived = await db_connection.fetchval("SELECT COUNT(*) FROM djoutbox_sent")
    assert archived == 0

    remaining = await db_connection.fetchval("SELECT COUNT(*) FROM djoutbox_pending")
    assert remaining == 0
