from __future__ import annotations

import pytest
from pydantic import BaseModel

from djoutbox import Consumer, Worker, consume


class _Payload(BaseModel):
    name: str


@pytest.mark.asyncio
async def test_register_consumer():
    @consume(binding_key="test_binding", queue_name="test_queue")
    async def handler(_: object) -> None:
        pass

    assert handler.queue == "test_queue"
    assert handler.binding_key == "test_binding"
    assert handler._queue_obj is None
    assert handler._consumer_tag is None


@pytest.mark.asyncio
async def test_consume_decorator():
    @consume(binding_key="routing_key", queue_name="test_consume_queue")
    async def handler(_: object) -> None:
        pass

    assert handler.queue == "test_consume_queue"
    assert isinstance(handler, Consumer)


@pytest.mark.asyncio
async def test_consumer_queue_name_auto():
    @consume(binding_key="k")
    async def my_custom_handler(_: object) -> None:
        pass

    assert "my_custom_handler" in my_custom_handler.queue
    assert my_custom_handler.queue_name is None


@pytest.mark.asyncio
async def test_consumer_default_retry_delays(worker: Worker):
    @consume(binding_key="k", queue_name="test_default_retry")
    async def handler(_: object) -> None:
        pass

    assert handler.retry_delays is None

    prev = worker.consumers
    worker.consumers = [handler]
    await worker._set_up_queues()
    assert handler.retry_delays == worker.default_retry_delays
    worker.consumers = prev


@pytest.mark.asyncio
async def test_consumer_initially_no_retry_delays():
    @consume(binding_key="k", queue_name="test_no_retries")
    async def handler(_: object) -> None:
        pass

    assert handler.retry_delays is None


@pytest.mark.asyncio
async def test_consumer_deserializes_via_registry(worker: Worker):
    received = []

    @consume(binding_key="k", queue_name="test_registry_deser")
    async def handler(payload: _Payload) -> None:
        received.append(payload)

    worker.serializers = [
        (
            BaseModel,
            lambda m: m.model_dump_json().encode(),
            lambda cls, d: cls.model_validate_json(d),
        )
    ]

    class FakeMessage:
        body = b'{"name": "Bob"}'
        routing_key = "k"
        delivery_tag = 1
        headers = {}
        content_type = "application/json"

        async def ack(self):
            pass

        async def nack(self, requeue=False):
            pass

    consumer = handler
    consumer._exchange_name = "outbox"
    consumer._worker_serializers = worker.serializers
    await consumer._handle(FakeMessage())

    assert len(received) == 1
    assert received[0].name == "Bob"


@pytest.mark.asyncio
async def test_consumer_bytes_passthrough(worker: Worker):
    received = []

    @consume(binding_key="k", queue_name="test_bytes_passthrough")
    async def handler(payload: bytes) -> None:
        received.append(payload)

    class FakeMessage:
        body = b"raw bytes"
        routing_key = "k"
        delivery_tag = 1
        headers = {}
        content_type = "application/octet-stream"

        async def ack(self):
            pass

        async def nack(self, requeue=False):
            pass

    consumer = handler
    consumer._exchange_name = "outbox"
    consumer._worker_serializers = None
    await consumer._handle(FakeMessage())

    assert received == [b"raw bytes"]
