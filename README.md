# djoutbox

Transactional outbox pattern for Django + PostgreSQL + RabbitMQ.

> **Note:** This README only covers running relay and worker processes.
> Full documentation (installation, configuration, publishing, etc.) is TODO.

## Running the relay

The relay moves messages from `djoutbox_pending` to RabbitMQ. It connects to
PostgreSQL (via asyncpg) and RabbitMQ (via aio-pika).

### Option 1 — Copy settings into the script

```python
import asyncio
from djoutbox import Relay

relay = Relay(
    db_dsn="postgresql://user:pass@localhost:5432/mydb",
    rmq_url="amqp://guest:guest@localhost/",
    exchange_name="outbox",
)
asyncio.run(relay.run())
```

No Django dependency. Fast startup. Good for isolated relay pods.

### Option 2 — Env vars

```python
import asyncio
import os
from djoutbox import Relay

relay = Relay(
    db_dsn=os.environ["DB_DSN"],
    rmq_url=os.environ["RMQ_URL"],
    exchange_name=os.environ.get("EXCHANGE_NAME", "outbox"),
)
asyncio.run(relay.run())
```

Use the same env vars in `settings.py` to keep them in sync.

### Option 3 — Import settings.py directly

```python
import os
import sys

sys.path.insert(0, "/path/to/project")
os.environ["DJANGO_SETTINGS_MODULE"] = "myproject.settings"

import asyncio
from django.conf import settings
from djoutbox import Relay
from djoutbox.conf import build_dsn

asyncio.run(Relay(db_dsn=build_dsn(), **settings.DJOUTBOX).run())
```

This is what `examples/relay.py` does. `django.conf.settings` is a lightweight
import (no app registry loading), so startup is fast. `build_dsn()` constructs
the asyncpg DSN from `DATABASES` settings.

### Option 4 — Shared settings file

If `settings.py` is too heavy or cannot import without Django, extract
`DJOUTBOX` to a separate file:

```python
# settings_djoutbox.py
DJOUTBOX = {
    "rmq_url": "amqp://guest:guest@localhost/",
    "exchange_name": "outbox",
    # ...
}
```

```python
# settings.py
from settings_djoutbox import DJOUTBOX  # noqa: F401
```

```python
# relay.py
import asyncio
from settings_djoutbox import DJOUTBOX
from djoutbox import Relay
from djoutbox.conf import build_dsn

relay = Relay(db_dsn=build_dsn(), **DJOUTBOX)
asyncio.run(relay.run())
```

## Running the worker

The worker consumes messages from RabbitMQ and calls your handlers.

### Option 1 — Copy settings into the script

```python
import asyncio
from djoutbox import Worker, consume

@consume("my.routing.key")
async def my_handler(payload: dict) -> None:
    ...

worker = Worker(
    consumers=[my_handler],
    rmq_url="amqp://guest:guest@localhost/",
    exchange_name="outbox",
)
asyncio.run(worker.run())
```

No Django dependency. Fast startup. Good for isolated worker pods.

### Option 2 — Env vars

```python
import asyncio
import os
from djoutbox import Worker, consume

@consume("my.routing.key")
async def my_handler(payload: dict) -> None:
    ...

worker = Worker(
    consumers=[my_handler],
    rmq_url=os.environ["RMQ_URL"],
    exchange_name=os.environ.get("EXCHANGE_NAME", "outbox"),
)
asyncio.run(worker.run())
```

### Option 3 — Import settings.py directly

```python
import os
import sys

sys.path.insert(0, "/path/to/project")
os.environ["DJANGO_SETTINGS_MODULE"] = "myproject.settings"

import asyncio
from django.conf import settings
from djoutbox import Worker, consume

@consume("my.routing.key")
async def my_handler(payload: dict) -> None:
    ...

worker = Worker(consumers=[my_handler], **settings.DJOUTBOX)
asyncio.run(worker.run())
```

This is what `examples/worker.py` does.

### Option 4 — Shared settings file

Same pattern as relay — extract `DJOUTBOX` to a shared file, import it
directly without Django:

```python
import asyncio
from settings_djoutbox import DJOUTBOX
from djoutbox import Worker, consume

@consume("my.routing.key")
async def my_handler(payload: dict) -> None:
    ...

worker = Worker(consumers=[my_handler], **DJOUTBOX)
asyncio.run(worker.run())
```

### Worker in a management command

If you need the full Django environment (ORM, app registry, etc.) inside your
handler, put `Worker.run()` in a management command's `handle()` method:

```python
# myapp/management/commands/run_worker.py
import asyncio
from django.core.management.base import BaseCommand
from djoutbox import Worker, consume

@consume("my.routing.key")
async def my_handler(payload: dict) -> None:
    # Full Django ORM access here
    ...

class Command(BaseCommand):
    def handle(self, *args, **options):
        from django.conf import settings
        worker = Worker(consumers=[my_handler], **settings.DJOUTBOX)
        asyncio.run(worker.run())
```

Run with `./manage.py run_worker`. This pays the full `django.setup()` cost
on startup.