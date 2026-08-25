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

The body is serialized as follows, in order:

1. **Custom serializers** — if `DJOUTBOX["serializers"]` is configured, the first tuple whose `type` matches `isinstance(body, type)` is used.
2. **`bytes`** — used as-is.
3. **Anything else** — serialized via `json.dumps().encode()`.

Unserializable values raise `ValueError` with the routing key in the message.

Example — register Pydantic:

```python
# settings.py
from pydantic import BaseModel

DJOUTBOX = {
    ...,
    "serializers": [(
        BaseModel,
        lambda m: m.model_dump_json().encode(),
        lambda cls, d: cls.model_validate_json(d),
    )],
}
```

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

While using the outbox pattern, you will be publishing messages from an entrypoint (usually an API endpoint) which will be picked up by consumers which will in turn publish their own messages and so on. Tracking IDs assign a UUID chain to every message so you can trace the entire lineage.

Every `publish()` call automatically appends a new UUID to the chain.

### Reading tracking IDs via `tracking_ids` parameter

Inside a consumer, add a `tracking_ids` parameter to your function signature — the worker injects it automatically:

```python
from djoutbox import consume, publish

@consume("user.created", queue="on_user_created")
async def on_user_created(user, tracking_ids: list[str]):
    print(f"User created {user['id']}, tracking IDs: {tracking_ids}")
    publish("user.welcome_email", {"id": user["id"]})
    publish("user.created_notification", {"id": user["id"]})

@consume("user.welcome_email", queue="on_user_welcome_email")
async def on_user_welcome_email(user, tracking_ids: list[str]):
    print(f"Welcome email sent for user {user['id']}, tracking IDs: {tracking_ids}")

@consume("user.created_notification", queue="on_user_created_notification")
async def on_user_created_notification(user, tracking_ids):
    print(f"Notification created for user {user['id']}, tracking IDs: {tracking_ids}")
```

If the view publishes without `tracking()`:

```python
def create_user_view(request):
    with transaction.atomic():
        user = User.objects.create(username="johndoe")
        publish("user.created", {"id": user.id, "username": "johndoe"})
```

The output will be:

```
User created 123, tracking IDs: ['uuid1']
Welcome email sent for user 123, tracking IDs: ['uuid1', 'uuid2']
Notification created for user 123, tracking IDs: ['uuid1', 'uuid3']
```

### Reading tracking IDs via `get_tracking_ids()`

Alternatively, call `get_tracking_ids()` from inside the consumer:

```python
from djoutbox import consume, get_tracking_ids

@consume("user.created")
async def handle_user_created(user):
    tracking_ids = get_tracking_ids()
    print(f"User created, tracking IDs: {tracking_ids}")
```

### Entrypoint tracking with `tracking()`

To include a UUID for the originating entrypoint, wrap your publish actions with `tracking()`:

```python
from djoutbox import tracking, publish

def create_user_view(request):
    with tracking():
        with transaction.atomic():
            user = User.objects.create(username="johndoe")
            publish("user.created", {"id": user.id, "username": user.username})
```

Now the output will include the entrypoint's UUID:

```
User created 123, tracking IDs: ['uuid1', 'uuid2']
Welcome email sent for user 123, tracking IDs: ['uuid1', 'uuid2', 'uuid3']
Notification created for user 123, tracking IDs: ['uuid1', 'uuid2', 'uuid4']
```

The first UUID (`uuid1`) comes from the `tracking()` context manager, the second (`uuid2`) from the view's `publish()`, the third (`uuid3`) from the welcome_email `publish()`, and so on.

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