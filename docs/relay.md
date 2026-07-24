# Message Relay

The relay is the process that reads messages from `djoutbox_pending` and publishes them to RabbitMQ. It connects to PostgreSQL via asyncpg and RabbitMQ via aio-pika.

## Running the relay

### Option 1 — Standalone script (recommended)

The relay runs as a standalone async script. It reads `DJOUTBOX` from your Django settings by importing only `django.conf.settings` (not the full app registry), so startup is fast:

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

This is what `examples/relay.py` does. `django.conf.settings` is a lightweight import (no app registry loading), so startup is fast. `build_dsn()` constructs the asyncpg DSN from `DATABASES` settings.

### Option 2 — Env vars (no Django dependency)

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

### Option 3 — Shared settings file

Extract `DJOUTBOX` to a separate file to avoid Django imports entirely:

```python
# settings_djoutbox.py
DJOUTBOX = {
    "rmq_url": "amqp://guest:guest@localhost/",
    "exchange_name": "outbox",
}
```

```python
# relay.py
import asyncio
from settings_djoutbox import DJOUTBOX
from djoutbox import Relay
from djoutbox.conf import build_dsn

asyncio.run(Relay(db_dsn=build_dsn(), **DJOUTBOX).run())
```

## `Relay` API

```python
class Relay:
    def __init__(
        self,
        *,
        db_dsn: str,                                    # required
        rmq_url: str,                                   # required
        exchange_name: str = "outbox",
        batch_size: int = 50,
        notification_timeout: float = 60.0,
        expiration: DateType | None = None,
        sent_archive_enabled: bool = True,
        sent_archive_granularity: str = "1d",
        partition_admin_interval: float = 300.0,
    ) -> None
```

### Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `db_dsn` | *(required)* | Asyncpg DSN: `postgresql://user:password@host:port/dbname` |
| `rmq_url` | *(required)* | RabbitMQ URL: `amqp://user:pass@host:port/vhost` |
| `exchange_name` | `"outbox"` | Topic exchange name |
| `batch_size` | `50` | Messages to fetch per batch. Higher = more throughput, more memory |
| `notification_timeout` | `60.0` | Max seconds to wait for PG NOTIFY before checking for scheduled messages |
| `expiration` | `None` | Default message TTL in RabbitMQ. Overridden by per-message `expiration` |
| `sent_archive_enabled` | `True` | Move sent messages to `djoutbox_sent` partition (instead of deleting) |
| `sent_archive_granularity` | `"1d"` | Partition granularity: `"Nd"` or `"Nm"` |
| `partition_admin_interval` | `300.0` | Seconds between partition admin checks |

### Methods

- `run()` — Start the relay loop. Blocks until SIGINT/SIGTERM.

## How the relay works

```
asyncpg pool (min 1, max 4) on db_dsn
aio_pika.connect_robust(rmq_url) → channel → declare topic exchange (durable)

ensure_partitions() immediately
LISTEN on djoutbox_channel (PG NOTIFY)
partition_admin_loop(): ensure_partitions() every 300s

loop:
    check backlog
    drain batches (SELECT ... FOR UPDATE SKIP LOCKED → publish → move to sent)
    compute next_send or timeout
    wait on notification_event or timeout or shutdown
    on error: sleep 10s, retry
```

### Batch processing

Each batch:

1. `SELECT ... FROM djoutbox_pending WHERE send_after <= now() ORDER BY created_at LIMIT batch_size FOR UPDATE SKIP LOCKED`
2. Publishes all messages concurrently to RabbitMQ via `asyncio.gather`
3. Moves successfully published messages to `djoutbox_sent` (or deletes if archiving disabled)
4. Failed messages stay in `djoutbox_pending` for retry on next poll

### PostgreSQL NOTIFY

When a message is inserted into `djoutbox_pending` with `send_after <= NOW()`, the trigger fires `pg_notify('djoutbox_channel', '')`. The relay listens on this channel and wakes up immediately instead of waiting for the next poll timeout.

Because `pg_notify` is transactional, rolled-back transactions never wake the relay — exactly the right behavior for the outbox pattern.

## Partition management

The relay automatically creates range partitions for `djoutbox_sent`:

- On startup, it creates all needed partitions to cover the horizon
- Every 300 seconds, it checks and creates new partitions as needed
- Partitions are created with `CREATE TABLE ... PARTITION OF djoutbox_sent FOR VALUES FROM ... TO ...`
- Multiple relay pods safely handle `DuplicateTableError` (another pod created it first)

### Granularity

```python
DJOUTBOX = {
    "sent_archive": {
        "enabled": True,
        "granularity": "1d",  # or "3d", "7d", "1m", "3m"
    }
}
```

- `"Nd"` — partitions of N days each
- `"Nm"` — partitions of N calendar months each

### Retention

Partition retention is **your responsibility**. Drop old partitions when you no longer need them:

```sql
DROP TABLE djoutbox_sent_20250101_20250201;
```

There is no automated retention inside djoutbox. This is a deliberate design choice — you control your data lifecycle.

## Multiple relay pods

The relay supports horizontal scaling. Multiple relay instances can run concurrently:

- `FOR UPDATE SKIP LOCKED` ensures each message is claimed by exactly one relay pod
- `DuplicateTableError` during partition creation is safely handled
- At-least-once delivery means occasional duplicates are acceptable (design for idempotency)

## Handling relay failures

If the relay encounters an error (e.g., RabbitMQ unavailable), it logs the error and retries after 10 seconds. Messages remain in `djoutbox_pending` until the relay successfully publishes them. No messages are lost.

If the relay crashes and restarts, it resumes from where it left off — pending messages are still there, and sent messages are already safely archived.