# djoutbox

Transactional outbox pattern for Django + PostgreSQL + RabbitMQ.

## What is the transactional outbox pattern?

When building applications, you often need to perform two operations together: save data to a database and trigger side effects (send an email, call an external API, publish an event). Both operations must succeed or fail together to maintain consistency. If an error occurs between the database change and the side effect, your application is left in an inconsistent state and requires manual intervention.

**The outbox solution:**

Instead of calling side effects directly, write event messages to an outbox table in the same database transaction as your business data. A separate reliable process (the message relay) reads from this table and publishes messages to RabbitMQ.

**Benefits:**

- **Atomic operations** — Database changes and event publishing succeed or fail together within a single Django transaction.
- **Guaranteed delivery** — At-least-once delivery semantics ensure messages are eventually published.
- **No distributed transactions** — Everything happens within a single database transaction.
- **Reliable retries** — If publishing to RabbitMQ fails, the message relay keeps trying.
- **Django-native** — Uses Django migrations, ORM models for the outbox table, and the Django admin for visibility.

## How it works

```mermaid
flowchart LR
    MA{Your Django app} -->|"publish()"| P[(djoutbox_pending)]
    P ~~~ MR{Message relay}
    MR -->|SELECT FOR UPDATE| P
    MR ~~~ P
    MR --->|publish| ME["Exchange (topic)"]
    P --x|DELETE + INSERT INTO sent| S[(djoutbox_sent)]

    subgraph Database
    P
    S
    end

    subgraph RabbitMQ
    ME -->|binding| Q1[Queue 1]
    end

    Q1 ~~~ W{Worker} --->|"consume()"| Q1
    W ~~~ Q1
```

## Quick start

```python
# 1. Install
pip install djoutbox

# 2. Add to INSTALLED_APPS
INSTALLED_APPS = [
    ...,
    "djoutbox",
]

# 3. Configure
DJOUTBOX = {
    "rmq_url": "amqp://guest:guest@localhost/",
    "exchange_name": "outbox",
}

# 4. Migrate
./manage.py migrate

# 5. Publish
from djoutbox import publish

def create_user(request):
    with transaction.atomic():
        user = User.objects.create(username="johndoe")
        publish("user.created", {"id": user.id, "username": user.username})

# 6. Run the relay (separate process)
import asyncio
from django.conf import settings
from djoutbox import Relay
from djoutbox.conf import build_dsn

asyncio.run(Relay(db_dsn=build_dsn(), **settings.DJOUTBOX).run())

# 7. Write a worker (separate process)
import asyncio
from djoutbox import Worker, consume

@consume("user.created")
async def handle_user_created(user: dict):
    print(f"User created: {user}")

worker = Worker(consumers=[handle_user_created], rmq_url="amqp://guest:guest@localhost/")
asyncio.run(worker.run())
```

## Key features

| Feature | Description |
|---------|-------------|
| Django integration | Migrations, ORM models, admin interface, settings-based configuration |

## What djoutbox is not

- **Not a task queue** — djoutbox implements the outbox pattern for event publishing. It does not replace Celery or similar task queues. Workers are event consumers, not general-purpose task runners.
- **Not a replacement for your existing messaging** — Use it alongside your existing infrastructure. It only handles the outbox → RabbitMQ path.
- **Not a framework** — It's a library. You write the worker scripts and consumer functions.

## Relationship to `outbox`

djoutbox is a port of [outbox](https://github.com/anomalyco/outbox) (an async Python library for PostgreSQL + RabbitMQ) into a Django application. Key differences:

| Feature | `outbox` | `djoutbox` |
|---------|----------|------------|
| Storage | Single `outbox_table` with `sent_at` | Two tables: `djoutbox_pending` + `djoutbox_sent` (partitioned) |
| Table creation | Runtime `ensure_outbox_table()` | Django migrations |
| Configuration | Constructor arguments | `settings.DJOUTBOX` dict |
| Relay entrypoint | User script | Standalone async script |
| Publisher | Async (asyncpg) | Sync (Django ORM) |
| Async publish | Native async | Via `sync_to_async` |