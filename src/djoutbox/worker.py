from __future__ import annotations

import asyncio
import inspect
import json
import signal
import time
from collections.abc import Callable, Coroutine, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, cast, get_type_hints

import aio_pika
from aio_pika.abc import (
    AbstractConnection,
    AbstractExchange,
    AbstractIncomingMessage,
    AbstractQueue,
    ConsumerTag,
)

from djoutbox import metrics
from djoutbox.log import logger
from djoutbox.utils import (
    Reject,
    get_rmq_connection,
    parse_duration,
    tracking_ids_contextvar,
    truncate_body,
)

try:
    from pydantic import BaseModel
except ImportError:
    if not TYPE_CHECKING:
        BaseModel = type(None)


@dataclass
class Consumer:
    binding_key: str
    callback: Callable[..., Any] | Callable[..., Coroutine[Any, Any, None]]
    queue_name: str | None = None
    retry_delays: Sequence[str] | None = None
    _queue_obj: AbstractQueue | None = None
    _consumer_tag: ConsumerTag | None = None
    _delay_exchanges: dict[str, AbstractExchange] = field(default_factory=dict)
    _exchange_name: str | None = None

    queue: str = field(init=False)

    def __post_init__(self) -> None:
        if self.queue_name:
            self.queue = self.queue_name
        else:
            callback_func = cast(Any, self.callback)
            self.queue = f"{callback_func.__module__}.{callback_func.__qualname__}".replace(
                "<", ""
            ).replace(">", "")

        if not asyncio.iscoroutinefunction(self.callback):
            sync_callback = self.callback

            async def async_wrapper(
                *args: Any, _sync_callback: Callable[..., Any] = sync_callback, **kwargs: Any
            ) -> None:
                return await asyncio.to_thread(_sync_callback, *args, **kwargs)

            async_wrapper.__signature__ = inspect.signature(sync_callback)

            self.callback = async_wrapper

    def __call__(self, *args: Any, **kwargs: Any) -> Coroutine[Any, Any, None]:
        return self.callback(*args, **kwargs)

    async def _handle(self, message: AbstractIncomingMessage) -> None:
        assert self._exchange_name is not None
        metrics.messages_received.labels(queue=self.queue, exchange_name=self._exchange_name).inc()

        parameters = inspect.signature(self.callback).parameters
        parameter_keys = set(parameters.keys()) - {
            "routing_key",
            "message",
            "tracking_ids",
            "attempt_count",
        }
        if len(parameter_keys) != 1:
            raise ValueError("Worker functions must accept exactly one argument")
        body_param_key = parameter_keys.pop()
        body_param = parameters[body_param_key]

        type_hints = get_type_hints(self.callback)
        body_type = type_hints.get(body_param_key, body_param.annotation)

        tracking_ids = tuple(
            json.loads(cast(str, message.headers.get("x-outbox-tracking-ids", "[]")))
        )
        token = tracking_ids_contextvar.set(tracking_ids)

        attempt_count_header = cast(str | None, message.headers.get("x-delivery-count"))
        retry_delays = self.retry_delays or ()

        if attempt_count_header is not None:
            attempt_count = int(attempt_count_header)
            if retry_delays and attempt_count > len(retry_delays) + 1:
                logger.warning(
                    f"Message {message.routing_key} with tracking IDs {tracking_ids} exceeded "
                    f"retry attempts ({attempt_count} > {len(retry_delays) + 1}), sending to DLQ"
                )
                await message.nack(requeue=False)
                tracking_ids_contextvar.reset(token)
                return
        else:
            attempt_count = 1

        routing_key = (
            cast(str | None, message.headers.get("x-original-routing-key")) or message.routing_key
        )
        assert routing_key is not None
        body: Any = message.body
        try:
            if inspect.isclass(body_type) and issubclass(body_type, BaseModel):
                body = body_type.model_validate_json(message.body)
            elif inspect.isclass(body_type) and issubclass(body_type, bytes):
                body = message.body
            else:
                body = json.loads(message.body)
        except Exception as exc:
            logger.error(
                f"Failed to deserialize message body {routing_key=}, {tracking_ids=}, "
                f"error={type(exc).__name__}: {exc}",
                exc_info=True,
            )
            await message.nack(requeue=False)
            metrics.messages_processed.labels(
                queue=self.queue,
                exchange_name=self._exchange_name,
                status="deserialization_failed",
            ).inc()
            tracking_ids_contextvar.reset(token)
            return
        else:
            logger.debug(
                f"Processing message {routing_key=}, {tracking_ids=}, body={truncate_body(body)}"
            )

        kwargs: dict[str, Any] = {body_param_key: body}
        if "routing_key" in parameters:
            kwargs["routing_key"] = routing_key
        if "message" in parameters:
            kwargs["message"] = message
        if "tracking_ids" in parameters:
            kwargs["tracking_ids"] = tracking_ids
        if "attempt_count" in parameters:
            kwargs["attempt_count"] = attempt_count

        processing_start_time = time.perf_counter()

        try:
            await self.callback(**kwargs)
        except Reject:
            logger.warning(
                f"Rejecting, this message will end up in DLQ {routing_key=}, "
                f"{tracking_ids=}, body={truncate_body(body)}"
            )
            await message.nack(requeue=False)
            metrics.messages_processed.labels(
                queue=self.queue, exchange_name=self._exchange_name, status="rejected"
            ).inc()
        except Exception as exc:
            logger.warning(
                f"Handler failed with {type(exc).__name__}: {exc}, retrying "
                f"{routing_key=}, {tracking_ids=}, body={truncate_body(body)}, "
                f"attempt={attempt_count}/{len(retry_delays)}",
                exc_info=True,
            )
            await self._delayed_retry(message, attempt_count, tracking_ids)
            metrics.messages_processed.labels(
                queue=self.queue, exchange_name=self._exchange_name, status="failed"
            ).inc()
        else:
            logger.debug(f"Successfully processed {routing_key=}, {tracking_ids=}")
            await message.ack()
            metrics.messages_processed.labels(
                queue=self.queue, exchange_name=self._exchange_name, status="success"
            ).inc()
        finally:
            processing_duration_seconds = time.perf_counter() - processing_start_time
            metrics.message_processing_duration.labels(
                queue=self.queue, exchange_name=self._exchange_name
            ).observe(processing_duration_seconds)
            tracking_ids_contextvar.reset(token)

    async def _delayed_retry(
        self,
        message: AbstractIncomingMessage,
        attempt_count: int,
        tracking_ids: tuple[str, ...],
    ) -> None:
        retry_delays = self.retry_delays or ()

        if attempt_count > len(retry_delays):
            logger.warning(
                f"Exceeded retry attempts ({attempt_count} > {len(retry_delays)}), sending to DLQ"
            )
            await message.nack(requeue=False)
            return

        delay_str = retry_delays[attempt_count - 1]
        delay_ms = parse_duration(delay_str)

        if delay_ms == 0:
            await message.nack(requeue=True)
            metrics.retry_attempts.labels(queue=self.queue, delay_seconds=delay_str).inc()
            logger.info(
                f"Message requeued for instant retry "
                f"(attempt {attempt_count}/{len(retry_delays)}) "
                f"routing_key={message.routing_key}, {tracking_ids=}"
            )
            return

        delay_exchange = self._delay_exchanges[delay_str]

        new_headers = dict(message.headers) if message.headers else {}
        new_headers["x-delivery-count"] = str(attempt_count + 1)

        assert message.routing_key is not None
        if "x-original-routing-key" not in new_headers:
            new_headers["x-original-routing-key"] = message.routing_key

        try:
            await delay_exchange.publish(
                aio_pika.Message(
                    body=message.body,
                    content_type=message.content_type,
                    headers=new_headers,
                ),
                routing_key=self.queue,
            )
        except Exception as exc:
            metrics.publish_failures.labels(
                exchange_name=delay_exchange.name,
                failure_type="retry",
                error_type=type(exc).__name__,
            ).inc()
            logger.error(
                f"Failed to publish message to delay exchange: routing_key={self.queue}, "
                f"original_routing_key={message.routing_key}, {tracking_ids=}, "
                f"delay_exchange={delay_exchange.name}, error={type(exc).__name__}, {exc=}"
            )
            raise

        await message.ack()
        metrics.retry_attempts.labels(queue=self.queue, delay_seconds=delay_str).inc()
        logger.info(
            f"Message sent to delay exchange (attempt {attempt_count}/{len(retry_delays)}, "
            f"delay {delay_str}) routing_key={self.queue}, "
            f"original_routing_key={message.routing_key}, {tracking_ids=}"
        )


def consume(
    binding_key: str,
    queue_name: str | None = None,
    retry_delays: Sequence[str] | None = None,
) -> Callable[[Callable[..., Any] | Callable[..., Coroutine[Any, Any, None]]], Consumer]:
    def decorator(
        func: Callable[..., Any] | Callable[..., Coroutine[Any, Any, None]],
    ) -> Consumer:
        return Consumer(binding_key, func, queue_name, retry_delays)

    return decorator


@dataclass
class Worker:
    rmq_connection: AbstractConnection | None = None
    rmq_url: str | None = None
    consumers: Sequence[Consumer] = field(default_factory=list)
    exchange_name: str = "outbox"
    default_retry_delays: Sequence[str] = ("1s", "10s", "1m", "5m")
    prefetch_count: int = 10
    _shutdown_future: asyncio.Future[None] | None = None

    def __post_init__(self) -> None:
        if self.rmq_connection is not None and self.rmq_url is not None:
            raise ValueError("You cannot set both rmq_connection and rmq_url")

    async def run(self) -> None:
        if self.rmq_connection is None and self.rmq_url is not None:
            self.rmq_connection = await get_rmq_connection(self.rmq_url)
        if self.rmq_connection is None:
            logger.warning("Cannot update DLQ metrics: RabbitMQ connection not available")
            return

        await self._set_up_queues()

        self._shutdown_future = asyncio.Future()
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGINT, self._shutdown_future.set_result, None)
        loop.add_signal_handler(signal.SIGTERM, self._shutdown_future.set_result, None)

        tasks = set()

        dlq_metrics_task = asyncio.create_task(self._update_dlq_metrics())

        logger.info(
            f"Starting worker on exchange '{self.exchange_name}' with "
            f"{len(self.consumers)} consumers, prefetch_count={self.prefetch_count}, "
            f"default_retry_delays={self.default_retry_delays}"
        )
        for consumer in self.consumers:
            assert consumer._queue_obj is not None

            async def _task(
                message: AbstractIncomingMessage, consumer: Consumer = consumer
            ) -> None:
                if self._shutdown_future is not None and self._shutdown_future.done():
                    await message.nack(requeue=True)
                    return
                task = asyncio.create_task(consumer._handle(message))
                tasks.add(task)
                task.add_done_callback(tasks.discard)

            consumer._consumer_tag = await consumer._queue_obj.consume(_task)
            metrics.active_consumers.labels(
                queue=consumer.queue, exchange_name=self.exchange_name
            ).inc()

        await self._shutdown_future

        logger.info("Received shutdown signal, waiting for ongoing tasks and exiting...")

        for consumer in self.consumers:
            if consumer._consumer_tag is None:
                continue
            assert consumer._queue_obj is not None
            await consumer._queue_obj.cancel(consumer._consumer_tag)
            metrics.active_consumers.labels(
                queue=consumer.queue, exchange_name=self.exchange_name
            ).dec()

        if tasks:
            await asyncio.wait(tasks)

        dlq_metrics_task.cancel()

    async def _update_dlq_metrics(self) -> None:
        assert self.rmq_connection is not None

        while True:
            if self._shutdown_future is not None and self._shutdown_future.done():
                break
            try:
                channel = await self.rmq_connection.channel()
                for consumer in self.consumers:
                    dlq_name = f"{consumer.queue}.dlq"
                    try:
                        dlq = await channel.get_queue(dlq_name)
                        declaration_result = await dlq.declare()
                        metrics.dlq_messages.labels(queue=consumer.queue).set(
                            float(declaration_result.message_count or 0)
                        )
                    except Exception as exc:
                        logger.debug(f"Failed to get DLQ message count for {dlq_name}: {exc}")
            except Exception as exc:
                logger.warning(f"Error updating DLQ metrics: {exc}")
            await asyncio.sleep(30)

    async def _set_up_queues(self) -> None:
        if self.rmq_connection is None:
            raise ValueError("RabbitMQ connection is not set up.")

        channel = await self.rmq_connection.channel()
        await channel.set_qos(prefetch_count=self.prefetch_count)

        exchange = await channel.declare_exchange(
            self.exchange_name, aio_pika.ExchangeType.TOPIC, durable=True
        )

        dead_letter_exchange = await channel.declare_exchange(
            f"{self.exchange_name}.dlx", aio_pika.ExchangeType.DIRECT, durable=True
        )

        all_delay_strs: set[str] = set(self.default_retry_delays)
        for consumer in self.consumers:
            if consumer.retry_delays:
                all_delay_strs.update(consumer.retry_delays)

        delay_map: dict[str, int] = {}
        for delay_str in all_delay_strs:
            delay_ms = parse_duration(delay_str)
            delay_map[delay_str] = delay_ms

        delay_exchanges = {}
        for delay_str, delay_ms in delay_map.items():
            if delay_ms == 0:
                continue
            exchange_and_queue_name = f"{self.exchange_name}.delay_{delay_str}"
            delay_exchange = await channel.declare_exchange(
                exchange_and_queue_name, aio_pika.ExchangeType.FANOUT, durable=True
            )
            delay_exchanges[delay_str] = delay_exchange
            delay_queue = await channel.declare_queue(
                exchange_and_queue_name,
                durable=True,
                arguments={
                    "x-message-ttl": delay_ms,
                    "x-dead-letter-exchange": "",
                    "x-queue-type": "quorum",
                },
            )
            await delay_queue.bind(delay_exchange)

        for consumer in self.consumers:
            if consumer.retry_delays is None:
                consumer.retry_delays = self.default_retry_delays
            consumer._delay_exchanges = delay_exchanges
            consumer._exchange_name = self.exchange_name

            dead_letter_queue_obj = await channel.declare_queue(
                f"{consumer.queue}.dlq", durable=True, arguments={"x-queue-type": "quorum"}
            )
            await dead_letter_queue_obj.bind(dead_letter_exchange, consumer.queue)

            logger.debug(
                f"Binding queue {consumer.queue} to exchange {self.exchange_name} with "
                f"binding key {consumer.binding_key}"
            )
            consumer._queue_obj = await channel.declare_queue(
                consumer.queue,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": f"{self.exchange_name}.dlx",
                    "x-dead-letter-routing-key": consumer.queue,
                    "x-queue-type": "quorum",
                },
            )
            await consumer._queue_obj.bind(exchange, consumer.binding_key)
