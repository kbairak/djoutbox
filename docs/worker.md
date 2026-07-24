# Worker

The worker consumes messages from RabbitMQ and dispatches them to your consumer functions. It has no Django dependency — it runs standalone with any async Python environment.

## Writing consumers

### Using the `@consume` decorator

```python
from djoutbox import Worker, consume

@consume("user.created")
async def handle_user_created(user: dict):
    print(f"User created: {user}")

worker = Worker(
    consumers=[handle_user_created],
    rmq_url="amqp://guest:guest@localhost/",
)
```

### Using `Consumer` directly

Both approaches are equivalent:

```python
from djoutbox import Worker, Consumer

async def handle_user_created(user: dict):
    print(f"User created: {user}")

worker = Worker(
    consumers=[
        Consumer(
            binding_key="user.created",
            queue_name="my_service.on_user_created",  # optional, auto-generated if not provided
            callback=handle_user_created,
        )
    ],
    rmq_url="amqp://guest:guest@localhost/",
)
```

## Running the worker

### Option 1 — Standalone script (no Django)

```python
import asyncio
from djoutbox import Worker, consume

@consume("my.routing.key")
async def my_handler(payload: dict):
    ...

worker = Worker(consumers=[my_handler], rmq_url="amqp://guest:guest@localhost/")
asyncio.run(worker.run())
```

### Option 2 — Env vars

```python
import asyncio
import os
from djoutbox import Worker, consume

worker = Worker(
    consumers=[my_handler],
    rmq_url=os.environ["RMQ_URL"],
    exchange_name=os.environ.get("EXCHANGE_NAME", "outbox"),
)
asyncio.run(worker.run())
```

### Option 3 — Import settings.py (for shared config)

```python
import os
import sys

sys.path.insert(0, "/path/to/project")
os.environ["DJANGO_SETTINGS_MODULE"] = "myproject.settings"

import asyncio
from django.conf import settings
from djoutbox import Worker, consume

worker = Worker(consumers=[my_handler], **settings.DJOUTBOX)
asyncio.run(worker.run())
```

### Option 4 — Management command (full Django ORM access)

If your consumer needs the Django ORM or other Django features (including publishing messages to the outbox table), put `Worker.run()` in a management command:

```python
# myapp/management/commands/run_worker.py
import asyncio
from django.core.management.base import BaseCommand
from django.conf import settings
from djoutbox import Worker, consume

@consume("my.routing.key")
async def my_handler(payload: dict):
    # Full Django ORM access here
    User.objects.filter(...)

class Command(BaseCommand):
    def handle(self, *args, **options):
        worker = Worker(consumers=[my_handler], **settings.DJOUTBOX)
        asyncio.run(worker.run())
```

Run with: `./manage.py run_worker`

## `Worker` API

```python
class Worker:
    def __init__(
        self,
        *,
        rmq_connection: AbstractConnection | None = None,   # mutually exclusive with rmq_url
        rmq_url: str | None = None,                          # mutually exclusive with rmq_connection
        consumers: Sequence[Consumer] = (),
        exchange_name: str = "outbox",
        default_retry_delays: Sequence[str] = ("1s", "10s", "1m", "5m"),
        prefetch_count: int = 10,
    ) -> None
```

| Parameter              | Default                     | Description                                             |
| ---------------------- | --------------------------- | ------------------------------------------------------- |
| `rmq_url`              | `None`                      | RabbitMQ connection string                              |
| `rmq_connection`       | `None`                      | Existing aio-pika connection (alternative to `rmq_url`) |
| `consumers`            | `()`                        | List of `Consumer` instances                            |
| `exchange_name`        | `"outbox"`                  | Topic exchange name                                     |
| `default_retry_delays` | `("1s", "10s", "1m", "5m")` | Default retry delays for all consumers                  |
| `prefetch_count`       | `10`                        | RabbitMQ prefetch count                                 |

### Methods

- `run()` — Start consuming. Blocks until SIGINT/SIGTERM.

## `Consumer` API

```python
@dataclass
class Consumer:
    binding_key: str                                          # required
    callback: Callable                                       # required
    queue_name: str | None = None
    retry_delays: Sequence[str] | None = None
```

| Field          | Default      | Description                                                                 |
| -------------- | ------------ | --------------------------------------------------------------------------- |
| `binding_key`  | _(required)_ | Topic binding key (supports wildcards like `user.*`)                        |
| `callback`     | _(required)_ | Async or sync function to handle messages                                   |
| `queue_name`   | `None`       | Queue name (auto-generated from callback module + qualname if not provided) |
| `retry_delays` | `None`       | Per-consumer retry delays (overrides Worker's default)                      |

The `Consumer` is callable — it delegates to the callback, making it transparent for testing.

## `consume()` decorator

```python
consume(
    binding_key: str,
    *,
    queue_name: str | None = None,
    retry_delays: Sequence[str] | None = None,
) -> Callable[[Callable], Consumer]
```

## Consumer callback arguments

The worker inspects your callback's signature and injects arguments by name:

```python
@consume("user.*")
async def handler(
    body: dict,              # The deserialized message body (exactly one non-reserved param required)
    routing_key: str,        # The original routing key (useful with wildcard bindings)
    message,                 # The raw aio-pika message object
    tracking_ids: list,      # List of UUIDs tracing the message lineage
    attempt_count: int,      # Current attempt number (1-based)
):
    ...
```

You must have exactly **one** parameter that isn't one of the reserved names (`routing_key`, `message`, `tracking_ids`, `attempt_count`). That parameter receives the deserialized message body.

### Body deserialization

The body type hint determines deserialization:

| Type hint                     | Deserialization         |
| ----------------------------- | ----------------------- |
| Pydantic `BaseModel` subclass | `model_validate_json()` |
| `bytes`                       | Raw body                |
| `dict` / no hint / other      | `json.loads()`          |

Pydantic is optional. Without it installed, plain `json.loads()` is used for all non-bytes types.

## Retries and dead-lettering

When a consumer raises an exception, the worker implements delayed retries using RabbitMQ's TTL-based delay queues.

### How retries work

1. Consumer raises an exception
2. Worker publishes the message to a delay exchange with TTL
3. After TTL expires, RabbitMQ routes it back to the original queue
4. Consumer receives it again (with incremented `attempt_count`)

### Configuring retry delays

```python
from djoutbox import Worker, consume, Reject

# Per-consumer override
@consume("user.created", retry_delays=("5s", "30s", "2m"))
async def handle_user_created(user):
    ...

# Disable retries (straight to DLQ on failure)
@consume("user.deleted", retry_delays=())
async def handle_user_deleted(user):
    ...

# Global default
worker = Worker(default_retry_delays=("1s", "10s", "1m", "5m"), ...)
```

### Duration format

| Input                          | Milliseconds        |
| ------------------------------ | ------------------- |
| `"0"`, `"0ms"`, `"0s"`, `"0m"` | 0 (instant requeue) |
| `"500ms"`                      | 500                 |
| `"1s"`                         | 1000                |
| `"30s"`                        | 30000               |
| `"1m"`                         | 60000               |
| `"1m30s"`                      | 90000               |
| `"1h"`                         | 3600000             |
| `"1d"`                         | 86400000            |

### Dead-letter queues (DLQ)

After exhausting all retry attempts, messages are sent to a dead-letter queue (`{queue_name}.dlq`). This allows you to inspect failures, fix bugs, and reprocess messages.

```python
from djoutbox import Reject

@consume("order.created")
async def handle_order(order):
    if some_permanent_error:
        raise Reject()  # Skip retries, send directly to DLQ
```

## Graceful shutdown

When the worker receives SIGINT or SIGTERM:

1. It requests disconnect from all queues
2. Messages sent between shutdown and disconnect are rejected with `requeue=True` (they'll be consumed by other workers)
3. In-flight messages continue processing until the callback finishes
4. When all pending tasks complete, the worker exits

```mermaid
sequenceDiagram
    participant Pub as Publisher
    participant Q as RabbitMQ Queue
    participant W as Worker
    participant OW as Other Worker

    W->>Q: Connect, start consuming
    Pub->>Q: Publish message 1
    Q-->>W: Deliver message 1
    W->>W: Process message 1

    Note right of W: SIGINT/SIGTERM received
    W->>Q: Request disconnect

    Pub->>Q: Publish message 2
    Q-->>W: Deliver message 2
    W->>Q: Reject message 2 (requeue=True)

    Q-->>W: Acknowledge disconnect
    W->>W: Finish processing message 1
    W->>Q: Ack message 1
    W->>W: Exit

    OW->>Q: Connect
    Q-->>OW: Deliver message 2
    OW->>OW: Process message 2
    OW->>Q: Ack message 2
```

## Blocking I/O and CPU-bound work

### Blocking I/O

If you have synchronous code, use a non-async callback. The worker wraps it in `asyncio.to_thread()` automatically:

```python
# Async callback (preferred)
@consume("user.created")
async def handle_user_created(user):
    await send_email_async(user)

# Sync callback (auto-runs in thread pool)
@consume("order.created")
def handle_order(order):
    send_email_blocking(order)
    update_crm_blocking(order)
```

### CPU-bound work

If your consumer needs to perform CPU-intensive work (image processing, data transformations, heavy computations), offload it to a **process pool** (not thread pool). This is not built into the library because most outbox use cases are I/O-bound, and users have different parallelism needs.

```python
import asyncio
from concurrent.futures import ProcessPoolExecutor
from djoutbox import consume

process_pool = ProcessPoolExecutor(max_workers=4)

def cpu_intensive_task(image_data: bytes) -> bytes:
    """Runs in a separate process, doesn't block the event loop"""
    from PIL import Image
    import io

    image = Image.open(io.BytesIO(image_data))
    image.thumbnail((800, 600))

    output = io.BytesIO()
    image.save(output, format="JPEG")
    return output.getvalue()

@consume("image.uploaded", queue="process_image")
async def process_image(image_data: bytes):
    loop = asyncio.get_event_loop()
    processed_data = await loop.run_in_executor(
        process_pool, cpu_intensive_task, image_data
    )
    await upload_to_storage(processed_data)
```

**Why process pool instead of thread pool for CPU work?**

- Python's GIL prevents true parallelism in threads for CPU-bound tasks
- Process pools bypass the GIL by using separate processes
- For blocking I/O, threads are sufficient (and more efficient) because I/O releases the GIL

## Topic exchange and wildcard matching

Use RabbitMQ topic exchange wildcards in binding keys:

```python
# Publish
publish("user.created", {"id": 123})
publish("user.updated", {"id": 123})
publish("user.deleted", {"id": 123})

# Consumer catches all user events
@consume("user.*", queue="user_event_handler")
async def handle_user_event(user):
    ...

# Consumer catches everything
@consume("#", queue="catch_all")
async def catch_all(event):
    ...
```

If you use wildcards and need the original routing key, add a `routing_key` parameter to the callback:

```python
@consume("user.*", queue="user_event_handler")
async def handle_user_event(routing_key: str, user):
    logger.info(f"Received {routing_key=}")
```
