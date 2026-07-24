# Publishing messages

Publishing happens inside your Django view, management command, or any other sync code. The publish call writes to `djoutbox_pending` within the current database transaction — if the transaction rolls back, the message is never written.

## `publish()`

```python
from djoutbox import publish

def create_user(request):
    with transaction.atomic():
        user = User.objects.create(username="johndoe")
        publish("user.created", {"id": user.id, "username": user.username})
```

Parameters:

- `routing_key` — RabbitMQ routing key (e.g., `"user.created"`, `"order.placed"`)
- `body` — Message body. Accepts dicts, lists, Pydantic models, bytes, or any JSON-serializable value
- `expiration` — Optional TTL in RabbitMQ. Can be `datetime`, `timedelta`, `int` (seconds), or `float` (milliseconds)
- `eta` — Optional future delivery time. Same accepted types as `expiration`

## `bulk_publish()`

For high-throughput scenarios, publish multiple messages in a single database operation:

```python
from djoutbox import bulk_publish, OutboxMessage

messages = [
    OutboxMessage(routing_key="user.created", body={"id": 1, "username": "alice"}),
    OutboxMessage(routing_key="user.created", body={"id": 2, "username": "bob"}),
    OutboxMessage(routing_key="order.placed", body={"order_id": 456}),
]
bulk_publish(messages)
```

This is significantly faster than calling `publish()` individually for large batches.

## `OutboxMessage`

```python
@dataclass
class OutboxMessage:
    routing_key: str
    body: Any
    expiration: DateType | None = None
    eta: DateType | None = None
```

## Serialization

The body is serialized as follows:

- **Pydantic `BaseModel`** — serialized via `model_dump_json().encode()` (requires `djoutbox[pydantic]`)
- **`bytes`** — used as-is
- **Anything else** — serialized via `json.dumps().encode()`

Unserializable values raise `ValueError` with the routing key in the message.

## Delayed execution (`eta`)

Schedule a message for future delivery:

```python
from datetime import timedelta
from django.utils import timezone

with transaction.atomic():
    user = User.objects.create(username="johndoe")
    publish(
        "user.created",
        {"id": user.id, "username": user.username},
        eta=timezone.now() + timedelta(hours=1),
    )
```

The message stays in `djoutbox_pending` until `send_after` is reached. The relay checks `MIN(send_after)` to compute its sleep timeout rather than waking up every heartbeat.

## Message expiration

Set a TTL for the message in RabbitMQ:

```python
publish("user.created", {"id": 123}, expiration=3600)  # 1 hour in seconds
```

If the message isn't consumed within the TTL, RabbitMQ discards it. Expiration counts from the time the relay publishes, not from `publish()` call time.

## Tracking IDs

Use tracking IDs to trace message lineage across services. Each `publish()` call appends a UUID to the chain. Consumers receive the chain and can append their own UUIDs when publishing.

### Entrypoint tracking

Wrap your entrypoint with `tracking()` to include the originating operation's UUID:

```python
from djoutbox import tracking, publish

def create_user(request):
    with tracking():
        with transaction.atomic():
            user = User.objects.create(username="johndoe")
            publish("user.created", {"id": user.id, "username": user.username})
```

### Reading tracking IDs

Inside a consumer, access the current tracking IDs:

```python
from djoutbox import get_tracking_ids

@consume("user.created")
async def handle_user_created(user, tracking_ids: list[str]):
    print(f"Tracking IDs: {tracking_ids}")
    # The consumer can publish with the chain extended
    publish("user.welcome_email", {"id": user["id"]})
```

The `tracking_ids` parameter is injected automatically if present in the consumer function signature.

## Important: call inside `transaction.atomic()`

The outbox pattern only works if `publish()` is called inside the same database transaction as your business writes:

```python
# ✅ Correct — atomic
with transaction.atomic():
    user = User.objects.create(...)
    publish("user.created", {"id": user.id})

# ❌ Wrong — message could be published even if user creation fails
publish("user.created", {"id": user.id})
user = User.objects.create(...)
```

This is also why `publish()` is a sync function, not a coroutine: Django's transaction handling is sync-only (see [Django docs](https://docs.djangoproject.com/en/stable/topics/async/#queries-the-orm)). The ORM supports async queries (`User.objects.acreate()`, `async for`), but `transaction.atomic()` requires sync execution. Since the outbox pattern depends on atomic transactions, the publish call must be sync.

### Async views

Wrap the transaction + publish inside a sync function and call it with `sync_to_async`:

```python
from asgiref.sync import sync_to_async
from django.db import transaction
from djoutbox import publish

def _create_user_sync(user_data):
    with transaction.atomic():
        user = User.objects.create(**user_data)
        publish("user.created", {"id": user.id, **user_data})
    return user

async def create_user_view(request):
    user_data = {"username": "johndoe"}
    user = await sync_to_async(_create_user_sync)(user_data)
    return JsonResponse({"id": user.id})
```

The `sync_to_async` call runs `_create_user_sync` in a separate thread where the sync transaction executes safely. The rest of your async view (handling the request, calling external APIs, etc.) remains async.

If you're using Django's `ATOMIC_REQUESTS = True` (Django's default for PostgreSQL), sync views already have an active transaction, so no `atomic()` block is needed — `publish()` alone is fine in a sync or `sync_to_async`-wrapped context.