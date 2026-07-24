# djoutbox — Implementation Plan

Transactional outbox pattern for Django + PostgreSQL + RabbitMQ. Port of `../outbox`
(async Python library) into a Django app, with a two-table (pending/sent) partitioned
archive design. Read `DESIGN.md` first; it is authoritative. This plan fills in the
details and points out every decision the implementing LLM should NOT have to guess.

Reference sources of truth, in order:
1. `DESIGN.md` (this repo) — architecture and public API shape.
2. `../outbox/src/outbox/*.py` — exact behavior to port (worker, metrics, retry topology,
   serialization rules, duration parsing). ~1400 lines total; read `worker.py`,
   `message_relay.py`, `publisher.py`, `utils.py`, `metrics.py` before coding.
3. `../outbox/README.md` — user-facing semantics (at-least-once, idempotency caveats).

---

## 1. Scope and deliberate deviations from `../outbox`

Port these features (feature parity per DESIGN.md):

- tracking ids (contextvar chain, `x-outbox-tracking-ids` header)
- topic exchanges
- retry exchanges/queues (delay topology)
- dead-letter exchanges/queues
- prometheus metrics (same 11 metrics, same names — see §9)
- bulk-publish
- graceful worker shutdown
- sync and async consumer functions
- logging via `logging.getLogger("djoutbox")`
- PG LISTEN/NOTIFY wake-up of the relay on insert
- relay-level default `expiration`
- optional pydantic support (never a hard dependency — see §5/§8/§11)

General rule: when porting, keep every good idea from `../outbox` unless DESIGN.md
explicitly replaces it (two-table design, fixed table names, migrations, settings-driven
relay, management-command entrypoint, `queue_name` kwarg). If unsure whether something
was dropped deliberately, check §1 — it lists every intentional deviation.

Deliberate deviations:

| Area | `../outbox` | `djoutbox` | Why |
|---|---|---|---|
| Storage | single `outbox_table` with `sent_at` + `clean_up_after` | two tables: `djoutbox_pending`, `djoutbox_sent` (partitioned) | DESIGN.md; partitioning replaces `clean_up_after` retention |
| Table creation | `ensure_outbox_table_*()` runtime helpers | Django migrations | Django way; DESIGN step 4 |
| `table_name` param | configurable | fixed names | migrations own schema |
| Relay entrypoint | user script | `./manage.py djoutbox_relay` | DESIGN step 7 |
| `consume()` kwarg | `queue` | `queue_name` | DESIGN step 6 |
| Relay/worker settings | constructor args | Django `DJOUTBOX` settings dict (relay); constructor args still supported (worker) | DESIGN steps 3/6 |
| NOTIFY channel/trigger names | `outbox_channel`, `notify_outbox_insert`, `outbox_notify_trigger` | `djoutbox_channel`, `djoutbox_notify_insert`, `djoutbox_notify_trigger` | namespace hygiene |

DESIGN.md relay pseudocode says `WHERE created_at < now() - retry_delay` — interpret as
a sketch. Actual filter is `WHERE send_after <= now()` (required for `eta` support, and
matches `../outbox`). `retry_delay` in the sketch ≈ the poll interval.

---

## 2. Repository layout

```
djoutbox/
├── DESIGN.md
├── PLAN.md
├── pyproject.toml
├── Makefile                      # same targets as ../outbox: lint (ruff+ty+mypy), test
├── src/
│   ├── djoutbox/
│   │   ├── __init__.py           # public API, lazy django imports (see §11)
│   │   ├── apps.py               # DjoutboxConfig
│   │   ├── conf.py               # DJOUTBOX settings access + defaults + validation
│   │   ├── log.py                # logger = logging.getLogger("djoutbox")
│   │   ├── models.py             # PendingMessage, SentMessage (managed=False)
│   │   ├── publisher.py          # OutboxMessage, publish(), bulk_publish()
│   │   ├── partitions.py         # granularity parsing, ensure_partitions()
│   │   ├── relay.py              # Relay (async core, django-agnostic)
│   │   ├── worker.py             # Consumer, consume(), Worker  (NO django imports)
│   │   ├── metrics.py            # 11 prometheus metrics (NO django imports)
│   │   ├── utils.py              # Reject, parse_duration, truncate_body, tracking ctx (NO django imports)
│   │   ├── admin.py              # PendingAdmin, SentAdmin w/ partition filter
│   │   ├── py.typed
│   │   ├── management/
│   │   │   └── commands/
│   │   │       └── djoutbox_relay.py
│   │   └── migrations/
│   │       ├── __init__.py
│   │       └── 0001_initial.py   # RunSQL DDL for both tables
│   └── tests/
│       ├── __init__.py
│       ├── conftest.py           # django settings + testcontainers fixtures
│       ├── test_utils.py
│       ├── test_publisher.py
│       ├── test_partitions.py
│       ├── test_relay.py
│       └── test_worker.py
```

`pyproject.toml`: hatchling, `packages = ["src/djoutbox"]`. Model on `../outbox/pyproject.toml`.

Dependencies:
- runtime: `django>=4.2`, `aio-pika>=9.5,<10`, `asyncpg>=0.30`, `prometheus-client>=0.8,<1`
- extras: `pydantic = ["pydantic>=2,<3"]` (optional support, same pattern as `../outbox`:
  `try: from pydantic import BaseModel except ImportError: BaseModel = type(None)`)
- dev: `pytest`, `pytest-asyncio`, `pytest-django`, `pytest-cov`, `testcontainers[postgres,rabbitmq]`,
  `ruff` (line-length 99, same lint selection as `../outbox`), `mypy --strict`, `ty`, `pydantic`
- `requires-python = ">=3.10"`

---

## 3. Settings schema (`conf.py`)

`settings.DJOUTBOX` is a dict. All keys optional; defaults below. One dict feeds BOTH
entry points: `Relay(db_dsn=..., **settings.DJOUTBOX)` and
`Worker(consumers=[...], **settings.DJOUTBOX)`. `conf.py` exposes:

- `get_setting(key)` / cached-settings accessor
- `validate_settings()` — raises `ImproperlyConfigured` on bad values (bad granularity,
  bad duration strings). Called from `DjoutboxConfig.ready()` (web fails early) AND from
  `build_dsn()` (relay fails early even without app registry, §7).
- `build_dsn(db_alias: str | None = None) -> str` — reads `DATABASES` via
  `django.conf.settings` (works WITHOUT `django.setup()`; settings access does not load
  the app registry) and returns an asyncpg DSN. Requires `ENGINE` containing
  `"postgresql"`, else `ImproperlyConfigured`; build
  `postgresql://user:password@host:port/name` from `USER/PASSWORD/HOST/PORT/NAME`
  (empty HOST → localhost, empty PORT → 5432; URL-quote user/password). `OPTIONS`
  (sslmode etc.) out of scope for v1 — note in docstring.

```python
DJOUTBOX = {
    # shared worker/relay keys — names MUST match Worker kwargs so that
    # Worker(consumers=[...], **settings.DJOUTBOX) works (DESIGN step 6)
    "rmq_url": "amqp://guest:guest@localhost/",   # str, no default -> required at Worker/relay construction time
    "exchange_name": "outbox",
    "default_retry_delays": ("1s", "10s", "1m", "5m"),
    "prefetch_count": 10,
    # relay-only
    "batch_size": 50,
    "notification_timeout": 60.0,  # seconds; max idle wait for PG NOTIFY (backstop)
    "expiration": None,            # relay-level default AMQP expiration (DateType)
    "db_alias": "default",         # which DATABASES entry the relay uses
    # sent archive
    "sent_archive": {
        "enabled": True,
        "granularity": "1d",       # "Nd" (N days) or "Nm" (N calendar months)
    },
}
```

Because `sent_archive`, `batch_size`, etc. are not `Worker` kwargs (and `prefetch_count`,
`default_retry_delays` are not `Relay` kwargs), BOTH `Worker.__init__` and
`Relay.__init__` must accept and ignore unknown kwargs (`**_ignored: object`) so
`**settings.DJOUTBOX` never crashes on either side. Log ignored keys at DEBUG.

Validate `default_retry_delays` and `granularity` in `validate_settings()` using
`parse_duration` / `parse_granularity` so typos fail at startup, not at first retry.

---

## 4. Database schema and migration

`models.py` — both models `managed = False` (schema owned by hand-written migration SQL,
because Django cannot express partitioned tables). Keep model fields in exact sync with SQL.

```python
class PendingMessage(models.Model):
    id = models.BigAutoField(primary_key=True)
    routing_key = models.TextField()
    body = models.BinaryField()
    tracking_ids = models.JSONField()
    created_at = models.DateTimeField()
    send_after = models.DateTimeField()
    expiration = models.DurationField(null=True)

    class Meta:
        db_table = "djoutbox_pending"
        managed = False

class SentMessage(models.Model):
    # DB table has NO primary key constraint (partitioned table; a PK would have to
    # include the partition key). id is unique in practice (copied from pending).
    # Declaring it pk here makes the ORM/admin work. Inserts never go through the ORM.
    id = models.BigIntegerField(primary_key=True)
    routing_key = models.TextField()
    body = models.BinaryField()
    tracking_ids = models.JSONField()
    created_at = models.DateTimeField()
    send_after = models.DateTimeField()
    expiration = models.DurationField(null=True)
    sent_at = models.DateTimeField()

    class Meta:
        db_table = "djoutbox_sent"
        managed = False
```

`migrations/0001_initial.py` — `migrations.RunSQL` with reverse_sql dropping objects.
Exact DDL:

```sql
CREATE TABLE djoutbox_pending (
    id            BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    routing_key   TEXT NOT NULL,
    body          BYTEA NOT NULL,
    tracking_ids  JSONB NOT NULL,
    created_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    send_after    TIMESTAMP WITH TIME ZONE NOT NULL,
    expiration    INTERVAL
);
CREATE INDEX djoutbox_pending_poll_idx
    ON djoutbox_pending (send_after, created_at);

CREATE TABLE djoutbox_sent (
    id            BIGINT NOT NULL,
    routing_key   TEXT NOT NULL,
    body          BYTEA NOT NULL,
    tracking_ids  JSONB NOT NULL,
    created_at    TIMESTAMP WITH TIME ZONE NOT NULL,
    send_after    TIMESTAMP WITH TIME ZONE NOT NULL,
    expiration    INTERVAL,
    sent_at       TIMESTAMP WITH TIME ZONE NOT NULL
) PARTITION BY RANGE (created_at);
-- indexes on the partitioned parent propagate to all future partitions automatically
CREATE INDEX djoutbox_sent_created_at_idx ON djoutbox_sent (created_at);
CREATE INDEX djoutbox_sent_routing_key_idx ON djoutbox_sent (routing_key);

-- LISTEN/NOTIFY wake-up (ported from ../outbox/utils.py DDL_STATEMENTS)
CREATE OR REPLACE FUNCTION djoutbox_notify_insert() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.send_after <= NOW() THEN
        PERFORM pg_notify('djoutbox_channel', '');
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER djoutbox_notify_trigger
    AFTER INSERT ON djoutbox_pending
    FOR EACH ROW
    EXECUTE FUNCTION djoutbox_notify_insert();
```

Critical details:

- `GENERATED BY DEFAULT` (not `ALWAYS`) so the move-to-sent CTE can insert the pending
  row's explicit `id` into `djoutbox_sent`.
- NOTIFY fires only for immediately-sendable rows (`send_after <= NOW()`); eta-scheduled
  rows are covered by the relay's computed wait timeout (§7). Channel name is the
  constant `'djoutbox_channel'`, payload is empty (relay only needs a poke).
- Zero partitions are created by the migration. Partition creation is the relay's job
  (DESIGN.md: "Partition creation is relay's job"). First relay startup creates them.
- Do NOT create the `sent` table conditionally on `sent_archive.enabled` — migrations
  reading settings cause non-reproducible migration state. Always create both tables;
  the setting only changes relay behavior (move vs. delete).
- No `sent_at`-based cleanup index and no `clean_up_after` — retention is the user's
  responsibility via dropping partitions (DESIGN.md).
- Index on `djoutbox_pending` is NOT partial (unlike `../outbox`) because pending has no
  `sent_at` flag — every row is pending.

---

## 5. Publisher (`publisher.py`)

Sync-only (Django ORM is sync; DESIGN note: async users wrap in `sync_to_async`).

```python
@dataclass
class OutboxMessage:
    routing_key: str
    body: Any
    expiration: DateType | None = None   # datetime | timedelta | int | float (seconds/ms per encode_expiration)
    eta: DateType | None = None          # same accepted types

def publish(routing_key: str, body: Any, *, expiration=None, eta=None) -> None
def bulk_publish(messages: Sequence[OutboxMessage]) -> None
```

`publish(routing_key, body, ...)` ≡ `bulk_publish([OutboxMessage(routing_key, body, ...)])`.
Both operate on the default DB connection inside the caller's transaction — that is the
entire point of the outbox pattern; docstrings must say "call inside `transaction.atomic()`
with your business writes".

Serialization rules (port exactly from `../outbox/publisher.py:OutboxMessage.to_sql_params`):

1. body is pydantic `BaseModel` → `body.model_dump_json().encode()`
2. body is `bytes` → as-is
3. otherwise → `json.dumps(body).encode()`; `TypeError/ValueError` → re-raise as `ValueError`
   with routing key in the message
4. `tracking_ids = list(get_tracking_ids() + (str(uuid.uuid4()),))` — appended to contextvar chain
5. `expiration` → reuse `aio_pika.message.encode_expiration` (returns ms str; accepts
   datetime/timedelta/numbers); convert to `datetime.timedelta(milliseconds=...)` for the
   `DurationField`; `None` stays `None`. Invalid → `ValueError`.
6. `eta`: `send_after = now + timedelta(ms=encode_expiration(eta))` if set, else `now`.
   (`encode_expiration` handles absolute datetimes too.)
7. `now = timezone.now()` once per `bulk_publish` call, shared by all messages.

Implementation: build `PendingMessage` instances and `PendingMessage.objects.bulk_create()`.
(`publish` = `bulk_create` with one row; DESIGN's "alias to `Outbox.objects.create`" is
satisfied — bulk_create with a single-element list is equivalent and keeps one code path.)

Pydantic is OPTIONAL everywhere — `djoutbox` must install and fully work without it.
Use the `../outbox` guard pattern in both `publisher.py` and `worker.py`:

```python
try:
    from pydantic import BaseModel
except ImportError:
    if not TYPE_CHECKING:
        BaseModel = type(None)  # type: ignore[misc, assignment]
```

This keeps `isinstance(body, BaseModel)` / `issubclass(body_type, BaseModel)` checks
correctly False when pydantic is absent. Pydantic appears only in the `pydantic` extra
and in dev dependencies. A test (§12) verifies the package works with pydantic blocked.

No publisher-side metrics (parity: `../outbox` has none).

---

## 6. Partitioning (`partitions.py`)

Pure-Python + SQL module, no Django ORM (relay calls it over asyncpg). Unit-testable.

### Granularity parsing

```python
def parse_granularity(s: str) -> tuple[Literal["d", "m"], int]
```
`"Nd"` → `("d", N)`; `"Nm"` → `("m", N)`; N ≥ 1; anything else → `ValueError`.

### Partition naming

`djoutbox_sent_YYYYMMDD_YYYYMMDD` (parent name + `_` + start + `_` + end, DESIGN's
`YYYYMMDD_YYYYMMDD` suffix). Bounds are `[start, end)` — `end` is exclusive and equals
the next partition's `start`.

### Next-partition math

```python
def next_partition(start: date, kind: Literal["d","m"], n: int) -> tuple[date, date]
```

- `"d"`: `end = start + timedelta(days=n)`
- `"m"` (full calendar months, DESIGN example):
  - if `start.day == 1`: `end` = first day of the month `n` months after start's month
    (e.g. start 2026-02-01, n=1 → end 2026-03-01)
  - else: `end` = first day of the month after start's month — i.e. the partition covers
    the rest of the current partial month (start 2026-01-20, n=1 → end 2026-02-01; next
    call with start 2026-02-01 → end 2026-03-01). This yields DESIGN's
    `20260120_20260201`, `20260201_20260301` sequence (DESIGN writes end-inclusive dates
    `20260131`; we store the exclusive bound `20260201` in the name — document this
    naming choice in code comments so nobody "fixes" it).

### `needed_partitions()` algorithm (asyncpg conn)

1. `SELECT child.relname FROM pg_inherits i
   JOIN pg_class child ON child.oid = i.inhrelid
   JOIN pg_class parent ON parent.oid = i.inhparent
   WHERE parent.relname = 'djoutbox_sent'`
2. Parse `YYYYMMDD_YYYYMMDD` suffixes → list of `(start, end)` dates. `max_end = max(end)`
   or `None` if no partitions.
3. `start = max_end` if found, else `min(date.today(), oldest_pending_created_at.date())`
   where the oldest-pending query is
   `SELECT min(created_at) FROM djoutbox_pending` — this covers the edge case of rows
   written before the first relay run / before archiving was enabled, whose `created_at`
   would otherwise fall outside all partitions and make the move CTE fail.
4. Horizon: `days = n` for `"d"`, `31 * n` for `"m"` (DESIGN: "cover at least next N days,
   N = 1 daily, 7 weekly, 31 monthly"). While `start < today + horizon`: compute
   `end = next_partition(start, kind, n)`; emit `(start, end)`; `start = end`.
5. Return list of `(name, start, end)` to create.

### `ensure_partitions(conn, granularity)`

```python
for name, start, end in needed_partitions(conn, granularity):
    try:
        await conn.execute(
            f'CREATE TABLE {name} PARTITION OF djoutbox_sent '
            f"FOR VALUES FROM ('{start.isoformat()}') TO ('{end.isoformat()}')"
        )
    except asyncpg.exceptions.DuplicateTableError:
        pass  # another relay pod created it first
```

Partition names and bounds are validated identifiers/ISO dates generated by our own code,
so f-string interpolation is safe here (never interpolate user input). Add a debug log per
created partition, info log "ensured N partitions".

Also expose `list_partitions() -> list[tuple[str, date, date]]` (sync Django-connection
variant used by the admin filter, §10).

---

## 7. Relay (`relay.py` + entrypoints)

`Relay` is django-agnostic (testable without Django, no django imports in `relay.py`).
Two entrypoints wrap it (see "Entrypoints" below); both just translate
settings → constructor args.

```python
class Relay:   # plain class, NOT a dataclass — must accept **_ignored (see below)
    def __init__(self, *, db_dsn: str,        # asyncpg DSN, e.g. from djoutbox.conf.build_dsn()
                 rmq_url: str,
                 exchange_name: str = "outbox",
                 batch_size: int = 50,
                 notification_timeout: float = 60.0,  # max idle wait for NOTIFY (parity)
                 expiration: DateType | None = None,  # relay-level default AMQP expiration
                 sent_archive_enabled: bool = True,
                 sent_archive_granularity: str = "1d",
                 partition_admin_interval: float = 300.0,  # DESIGN: every 300s
                 **_ignored: object) -> None: ...
    # **_ignored lets Relay(**settings.DJOUTBOX) swallow worker-only keys
    # (prefetch_count, default_retry_delays) and db_alias; log them at DEBUG
```

### Entrypoints

`Relay` itself is constructed from plain args. Django supplies config two ways:

1. **Tiny relay script** (recommended for big codebases — FAST STARTUP). Same philosophy
   as DESIGN step 6's worker file: user writes a file, no auto-discovery:

   ```python
   # relay_runner.py — run: DJANGO_SETTINGS_MODULE=myproject.settings python relay_runner.py
   import asyncio
   from django.conf import settings
   from djoutbox import Relay
   from djoutbox.conf import build_dsn

   asyncio.run(Relay(db_dsn=build_dsn(), **settings.DJOUTBOX).run())
   ```

   Crucially this NEVER calls `django.setup()`: `django.conf.settings` only imports the
   settings module, so startup cost ≈ settings-module import time — no app registry, no
   model imports. `build_dsn()` calls `validate_settings()` internally so misconfig fails
   immediately. (`db_dsn` is the one arg not in `DJOUTBOX` — it derives from `DATABASES`
   to avoid duplicating credentials.)

2. **Management command** (DESIGN step 7, convenience for small projects —
   `./manage.py djoutbox_relay`). Thin wrapper doing exactly the same as the script:

   ```python
   class Command(BaseCommand):
       def handle(self, *args, **options):
           from django.conf import settings
           asyncio.run(Relay(db_dsn=build_dsn(), **settings.DJOUTBOX).run())
   ```

   Pays full `django.setup()` (manage.py does it before `handle`) — fine for small
   projects; document the script as the big-codebase option.

Do NOT use the Django ORM connection in the relay — it is sync; asyncpg talks to the same
database using the DSN from `build_dsn()`.

### Main loop (`run()`)

Port of `../outbox/message_relay.py:run`, adapted. LISTEN/NOTIFY gives near-zero publish
latency; the timeout is only a backstop for eta-scheduled rows and missed notifications.

```
asyncpg pool (min 1, max 4) on db_dsn
aio_pika.connect_robust(rmq_url)  -> channel -> declare topic exchange (durable)
ensure_partitions() immediately
acquire dedicated pool connection for LISTEN; add_listener("djoutbox_channel",
    lambda *_: notification_event.set())
create task partition_admin_loop(): ensure_partitions(); sleep(300); repeat
  - wrap body in try/except: log error, keep looping (partition admin must not kill relay)
install SIGINT/SIGTERM handlers -> shutdown event (same pattern as Worker)
loop until shutdown:
    try:
        backlog = COUNT(*) FROM djoutbox_pending WHERE send_after <= now()
        metrics.table_backlog.set(backlog)
        log WARNING if backlog > 100, DEBUG if > 0          (parity thresholds)
        drain: repeat batches until a batch returns 0 rows
        next_send = SELECT MIN(send_after) FROM djoutbox_pending WHERE send_after > now()
        timeout = min((next_send - now).total_seconds(), notification_timeout) if next_send
                  else notification_timeout
    except Exception: log ERROR, sleep 10s, continue   (parity: RETRY_DELAY_SECONDS=10)
    wait on notification_event with timeout=timeout (asyncio.wait_for; swallow
        TimeoutError); also wake immediately if shutdown event is set; clear event
finally: remove listener, release listen conn, cancel partition task, close channel/pool
```

Racing a NOTIFY against the shutdown signal: simplest correct approach is to create a
task for `notification_event.wait()` and a task for `shutdown_event.wait()` and
`asyncio.wait(..., return_when=FIRST_COMPLETED, timeout=timeout)`.

### Batch (`_consume_batch(conn) -> int`)

Port of `../outbox/message_relay.py:_consume_outbox_batch`, adapted:

```sql
-- inside conn.transaction():
SELECT id, routing_key, body, tracking_ids, created_at, expiration
FROM djoutbox_pending
WHERE send_after <= $1
ORDER BY created_at
LIMIT $2
FOR UPDATE SKIP LOCKED
```

`FOR UPDATE SKIP LOCKED` is mandatory — DESIGN step 7 allows multiple relay pods.

Publish each row concurrently (`asyncio.gather(..., return_exceptions=True)`):

```python
aio_pika.Message(
    body=row["body"],
    content_type="application/json",
    expiration=row["expiration"] or self.expiration,   # row wins; relay default fallback (parity)
    headers={"x-outbox-tracking-ids": (
        json.dumps(row["tracking_ids"]) if isinstance(row["tracking_ids"], list)
        else row["tracking_ids"]
    )},
)
# published with routing_key=row["routing_key"]
```

`expiration` is a `timedelta` from the DB (or the relay-level default); aio-pika encodes
it to the AMQP ms string. `None` → no expiration.

Per result:
- exception → `metrics.publish_failures.labels(exchange_name, failure_type="main",
  error_type=type(exc).__name__).inc()`, log ERROR, row stays in pending (retried next poll)
- success → collect id; `metrics.messages_published.inc()`;
  `metrics.message_age.observe((now - created_at).total_seconds())`

Then ONE statement moves successful rows (DESIGN's CTE, batched — per-message would be N
round trips; same semantics):

```sql
WITH moved AS (
    DELETE FROM djoutbox_pending
    WHERE id = ANY($1::bigint[])
    RETURNING id, routing_key, body, tracking_ids, created_at, send_after, expiration
)
INSERT INTO djoutbox_sent (id, routing_key, body, tracking_ids, created_at, send_after,
                           expiration, sent_at)
SELECT id, routing_key, body, tracking_ids, created_at, send_after, expiration, $2
FROM moved
```

If `sent_archive_enabled` is False: plain `DELETE FROM djoutbox_pending WHERE id = ANY($1)`.

Failure mode to document in code: if the INSERT fails because no partition covers a row's
`created_at`, the whole CTE rolls back atomically → rows stay in pending, messages were
already published → at-least-once duplicates on next poll. Acceptable (at-least-once
semantics), self-heals within 300s when `partition_admin` creates the covering partition
(§6 step 3 guarantees coverage back to the oldest pending row). Log the error loudly.

`metrics.poll_duration.observe(...)` per batch.

The management command lives at `management/commands/djoutbox_relay.py` (see
"Entrypoints" above — it is a thin wrapper; `build_dsn` lives in `conf.py`, §3).

---

## 8. Worker (`worker.py`)

Near-verbatim port of `../outbox/worker.py` (495 lines). NO django imports — worker must
run on machines with no Django installed (DESIGN: "with or without django").

### Public API

```python
def consume(binding_key: str, *, queue_name: str | None = None,
            retry_delays: Sequence[str] | None = None
            ) -> Callable[[Callable], Consumer]

@dataclass
class Consumer:
    binding_key: str
    queue_name: str | None
    callback: Callable
    retry_delays: Sequence[str] | None

class Worker:   # plain class (dataclass + **ignored kwargs doesn't mix well)
    def __init__(self, *, consumers=(), rmq_url=None, rmq_connection=None,
                 exchange_name="outbox", default_retry_delays=("1s","10s","1m","5m"),
                 prefetch_count=10, **_ignored): ...
    async def run(self) -> None: ...
```

DESIGN example passes `**settings.DJOUTBOX`; `rmq_url` matches the settings key
(`../outbox` calls it `rmq_connection_url` — renamed to match DESIGN). `rmq_connection`
(existing aio-pika connection) kept for parity/testability; mutually exclusive with
`rmq_url` → `ValueError`.

### Behavior to port exactly

- `Consumer.__post_init__`: empty/missing `queue_name` → auto-generate
  `f"{callback.__module__}.{callback.__qualname__}"` with `<`/`>` stripped.
  Non-coroutine callback → wrap in `asyncio.to_thread`, preserve `__signature__`.
- `Consumer._handle(message)`:
  - `metrics.messages_received.labels(queue, exchange_name).inc()`
  - signature introspection: exactly one body param; reserved names `routing_key`,
    `message`, `tracking_ids`, `attempt_count` injected only if present in signature
  - body deserialization by type hint of the body param (`get_type_hints` to resolve
    strings): `BaseModel` subclass → `model_validate_json`; `bytes` → raw; else
    `json.loads`. Failure → log ERROR, `nack(requeue=False)` (→ DLQ),
    `messages_processed{status="deserialization_failed"}`, return.
  - tracking ids: `json.loads(message.headers.get("x-outbox-tracking-ids", "[]"))` →
    tuple → set `tracking_ids_contextvar` (reset in `finally`)
  - `attempt_count`: from `x-delivery-count` header (int), default 1; if retry_delays set
    and `attempt_count > len(retry_delays) + 1` → warn, `nack(requeue=False)` (safety net)
  - `routing_key`: `headers["x-original-routing-key"]` (set on retries) else
    `message.routing_key`
  - callback raises `Reject` → warn, `nack(requeue=False)`, `status="rejected"`
  - callback raises other `Exception` → warn (exc_info), `_delayed_retry`,
    `status="failed"`
  - success → `ack()`, `status="success"`
  - `finally`: `message_processing_duration.observe(...)`, reset contextvar
- `_delayed_retry(message, attempt_count, tracking_ids)`:
  - `attempt_count > len(retry_delays)` → `nack(requeue=False)` (→ DLQ)
  - delay 0 ms → `nack(requeue=True)` (instant retry, no topology), `retry_attempts` inc
  - else publish to the per-delay fanout exchange with `routing_key=self.queue_name`
    (NOT the original routing key — dead-letters to default exchange `""` which routes by
    queue name, so the retry returns to the SAME queue), headers = original +
    `x-delivery-count = attempt_count + 1` + `x-original-routing-key` (set once),
    preserve body/content_type; then `ack()` original; `retry_attempts.labels(queue,
    delay_seconds=delay_str).inc()`. Publish failure → `publish_failures{failure_type=
    "retry"}`, re-raise.
- `Worker.run()`:
  - `_set_up_queues()` then consume; `asyncio.Future` shutdown on SIGINT/SIGTERM via
    `loop.add_signal_handler`
  - message dispatch wrapper: if shutdown already requested → `nack(requeue=True)`;
    else `asyncio.create_task(consumer._handle(msg))`, track task set
  - after shutdown future resolves: cancel all consumer tags (`active_consumers.dec()`),
    `await asyncio.wait(tasks)`, cancel dlq-metrics task
- `_update_dlq_metrics()` background task: every 30s, for each consumer
  `channel.get_queue(f"{queue}.dlq")`, passive-declare, set
  `dlq_messages.labels(queue).set(message_count)`; swallow per-queue errors at DEBUG.
- `active_consumers` inc/dec on consume start/cancel.

### RabbitMQ topology (exact names — tests assert these)

All durable; all queues quorum (`"x-queue-type": "quorum"`); default `exchange_name="outbox"`:

| Object | Name | Type | Extra |
|---|---|---|---|
| main exchange | `{exchange_name}` | TOPIC | |
| DLX | `{exchange_name}.dlx` | DIRECT | |
| delay exchange (per unique delay str) | `{exchange_name}.delay_{delay_str}` | FANOUT | e.g. `outbox.delay_10s` |
| delay queue (per unique delay str) | `{exchange_name}.delay_{delay_str}` | — | args: `x-message-ttl=<ms>`, `x-dead-letter-exchange=""` |
| consumer queue | `{queue_name}` | — | args: `x-dead-letter-exchange={exchange_name}.dlx`, `x-dead-letter-routing-key={queue_name}` |
| consumer DLQ | `{queue_name}.dlq` | — | |

Bindings: consumer queue ← main exchange with `binding_key` (topic wildcards OK);
`{queue_name}.dlq` ← DLX with routing key `{queue_name}`; delay queue ← its delay exchange
(fanout). Delay strings validated up front with `parse_duration` BEFORE declaring anything
(let `ValueError` propagate). Delay of 0 → no exchange/queue (instant requeue path).

---

## 9. Metrics (`metrics.py`)

Port `../outbox/metrics.py` verbatim — same names, same labels (parity decision: keep
`outbox_` prefix so parity is mechanically verifiable and dashboards migrate unchanged;
add a module docstring noting this is deliberate):

| Name | Type | Labels |
|---|---|---|
| `outbox_messages_published_total` | Counter | `exchange_name` |
| `outbox_publish_failures_total` | Counter | `exchange_name`, `failure_type` (`main`/`retry`), `error_type` |
| `outbox_message_age_seconds` | Histogram | `exchange_name` |
| `outbox_poll_duration_seconds` | Histogram | `exchange_name` |
| `outbox_table_backlog` | Gauge | `exchange_name` |
| `outbox_messages_received_total` | Counter | `queue`, `exchange_name` |
| `outbox_messages_processed_total` | Counter | `queue`, `exchange_name`, `status` (`success`/`failed`/`rejected`/`deserialization_failed`) |
| `outbox_retry_attempts_total` | Counter | `queue`, `delay_seconds` |
| `outbox_message_processing_duration_seconds` | Histogram | `queue`, `exchange_name` |
| `outbox_dlq_messages` | Gauge | `queue` |
| `outbox_active_consumers` | Gauge | `queue`, `exchange_name` |

Emitting metrics requires no HTTP server in-process; users scrape via their own
`prometheus_client` exposition (Django view or worker-side `start_http_server`).
Document in README later; no code needed.

---

## 10. Admin (`admin.py`)

```python
@admin.register(PendingMessage)
class PendingMessageAdmin(admin.ModelAdmin): ...

@admin.register(SentMessage)
class SentMessageAdmin(admin.ModelAdmin): ...
```

- Both: `list_display = ("id", "routing_key", "created_at", "send_after", "expiration")`
  (+ `"sent_at"` on sent), `list_filter`/`search_fields = ("routing_key",)`,
  `date_hierarchy = "created_at"`, all fields readonly (`has_add_permission` → False,
  `has_change_permission` → False), default ordering `("-created_at",)`.
  `show_full_result_count = False` (avoid COUNT over huge archive).
- `SentMessageAdmin` adds a **partition dropdown filter** (DESIGN requirement):
  custom `admin.SimpleListFilter` subclass `PartitionFilter` with
  `title = "partition"`, `parameter_name = "partition"`:
  - `lookups()` → `list_partitions()` (sync, via Django DB connection — reuse
    `partitions.py` name-parsing; cache result on the filter instance or a short TTL to
    avoid querying `pg_inherits` twice per page load: `lookups` and `queryset` both run
    per request)
  - `queryset(request, qs)` → on selection, `qs.filter(created_at__gte=start,
    created_at__lt=end)` — PostgreSQL constraint exclusion then scans only that partition
  - no selection → no filter (full-table default; acceptable, constraint exclusion still
    applies to `date_hierarchy`/ordering ranges)
- Because `SentMessage` is unmanaged, verify admin delete permission is off
  (`has_delete_permission` → False) — deleting from a partitioned parent is legal PG but
  retention policy is "user drops partitions", keep admin read-only.

---

## 11. Public API & lazy imports (`__init__.py`)

Constraint: `from djoutbox import consume, Worker` must work on a machine with NO Django
installed and no settings configured (DESIGN step 6, worker-only pods). But
`from djoutbox import publish` needs models → needs Django.

Solution: PEP 562 module `__getattr__`:

```python
from djoutbox.worker import Consumer, Worker, consume   # no django imports, eager
from djoutbox.relay import Relay                         # no django imports, eager
from djoutbox.utils import Reject, get_tracking_ids, tracking

def __getattr__(name):
    if name in ("publish", "bulk_publish", "OutboxMessage"):
        from djoutbox import publisher
        return getattr(publisher, name)
    raise AttributeError(...)

__all__ = ["Consumer", "Worker", "consume", "Relay", "Reject", "get_tracking_ids",
           "tracking", "publish", "bulk_publish", "OutboxMessage"]
```

Enforce the import boundary: `worker.py`, `relay.py`, `metrics.py`, `utils.py` must never
import django or `djoutbox.models`. Add a test that asserts
`import djoutbox; djoutbox.Worker` works with `django` blocked in `sys.modules`
(see §12 test plan).

`log.py`: `logger = logging.getLogger("djoutbox")` — single logger used everywhere
(DESIGN requirement). No handlers configured by the library.

`utils.py` ports from `../outbox/utils.py`: `Reject`, `truncate_body` (reprlib config),
`get_rmq_connection` (aio_pika robust connect, ValueError with example URL on failure),
`tracking_ids_contextvar`, `get_tracking_ids()`, `tracking()` context manager, and
`parse_duration` — port EXACTLY, including quirks: pattern
`^([^0]\d*d)?([^0]\d*h)?([^0]\d*m)?([^0]\d*s)?([^0]\d*ms)?$`, zero spellings
`("0","0ms","0s","0m","0h","0d")` → 0, ranges h 1–23, m/s 1–59, ms 1–999, no leading
zeros, `ValueError` on anything else or all-zero. (`../outbox` tests in
`test_utils.py` encode the truth — mirror them.)

---

## 12. Testing plan

Mirror `../outbox/src/tests/` structure. pytest + pytest-asyncio
(`asyncio_default_fixture_loop_scope="session"` like `../outbox`) + pytest-django +
testcontainers (Postgres + RabbitMQ), modeled on `../outbox/src/tests/conftest.py`.

`conftest.py`:
- session-scoped PostgresContainer + RabbitMQContainer fixtures
- configure Django settings programmatically before `django.setup()` (`settings.configure`
  or a `tests/settings.py` + `DJANGO_SETTINGS_MODULE`): `DATABASES` pointed at the PG
  container, `INSTALLED_APPS = ["django.contrib.admin", "django.contrib.auth",
  "django.contrib.contenttypes", "djoutbox"]`, `DJOUTBOX = {...}` test dict,
  `USE_TZ = True`
- run migrations against the container once per session (call `migrate` via
  `call_command` — this exercises 0001_initial DDL for real)
- fixtures: `rmq_url`, `exchange` (test-unique exchange name per test to isolate
  topology), `clean_registry` if metric double-registration becomes an issue (prometheus
  `REGISTRY` is global — `../outbox` gets away with module-level metrics; keep tests in
  one process)

Tests:

- `test_utils.py` — port `../outbox/src/tests/test_utils.py` wholesale: all
  `parse_duration` valid/invalid tables, `truncate_body`, `tracking`/`get_tracking_ids`.
- `test_publisher.py` — serialization matrix (dict/pydantic/bytes; unserializable →
  ValueError); `tracking_ids` chain appended with fresh uuid (set contextvar first, assert
  both ids in DB row); `eta` (timedelta & datetime) → correct `send_after`; `expiration`
  → correct `DurationField`; rollback safety: `publish` inside `transaction.atomic()` +
  raise → row gone (THE core outbox guarantee — assert it); `bulk_publish` writes N rows
  in one call.
- `test_partitions.py` — pure unit tests for `parse_granularity`, `next_partition`
  (day mode; month mode incl. DESIGN's Jan-20 → `20260120_20260201` → `20260201_20260301`
  example; leap-year Feb), and DB tests for `ensure_partitions`: cold start creates
  partitions covering today+horizon; idempotent (second call creates nothing,
  `DuplicateTableError` swallowed); creates backfill partition for old pending rows;
  granularity switch continues from previous end (create `1d` partitions, switch to `1m`,
  assert next starts where last ended — DESIGN: "seamless granularity switch").
- `test_relay.py` (integration, needs both containers):
  - publish → run relay briefly → message lands in RMQ queue bound to test exchange;
    pending row moved to `djoutbox_sent` with `sent_at` set and same `id`
  - NOTIFY wake-up: publish a due message and assert it is relayed well within
    `notification_timeout` (i.e. the trigger+listener path works, not just the timeout)
  - `eta` in the future → not relayed until due (timeout-computation path), then relayed
  - `sent_archive.enabled=False` → row deleted, nothing in sent
  - RMQ down/unroutable publish error → row stays in pending, `publish_failures` inc
  - `expiration` row → AMQP message has expiration set; relay-level default `expiration`
    applies when the row's is NULL; tracking ids header round-trips
  - two relay instances concurrently → no duplicate publishes beyond at-least-once
    tolerance (SKIP LOCKED works)
  - `Relay(db_dsn=..., **settings.DJOUTBOX)` accepts worker-only keys via `**_ignored`
    (guards the one-settings-dict-two-consumers contract)
  - entrypoint test: `build_dsn()` + `Relay(...)` construct successfully from Django
    settings WITHOUT `django.setup()` app loading (subprocess or import blocker —
    guards the fast-startup promise)
- `test_worker.py` — port `../outbox/src/tests/test_worker.py`: signature injection
  (each reserved kwarg), pydantic/bytes/json body deser, deser failure → DLQ,
  `Reject` → DLQ without retries, retry flow with `("0s",)` instant path and with a
  real delay exchange (`x-delivery-count` increments, `x-original-routing-key`
  preserved, ends back in same queue), retries exhausted → `.dlq`, queue-name
  auto-generation, sync callback via to_thread, graceful shutdown (in-flight message
  completes, queued message requeued — `../outbox` tests simulate the shutdown future;
  copy their approach), topology declared with exact names (assert via channel
  passive-declare).
- lazy-import test: subprocess `python -c "import sys; sys.modules['django']=None;
  import djoutbox; djoutbox.Worker"` exits 0 (cheap way: `importlib` with an import
  blocker). Guards §11.
- no-pydantic test: same import-blocker technique with `pydantic` blocked — assert
  `djoutbox.publisher` and `djoutbox.worker` import, `publish` of a dict body works, and
  a consumer with a plain (non-annotated) body param processes a message. Guards the
  optional-dependency contract.

Aim for parity with `../outbox` coverage; `Makefile` target `test` runs
`pytest --cov --cov-report=term-missing`.

Manual smoke script (optional but useful): `tests/` sibling `examples/example.py` like
`../outbox` — a tiny Django project module is overkill; instead document a curl-able
demo in README later. Skip for v1 unless cheap.

---

## 13. Implementation order (milestones)

Each milestone must leave the tree lint-clean (`make lint`) and test-green.

1. **Packaging skeleton** — pyproject, Makefile, src layout, `log.py`, `apps.py`,
   empty `__init__`. `pip install -e .` works.
2. **utils + metrics** — port both files + `test_utils.py`. No django needed yet.
3. **Schema** — `models.py`, `0001_initial.py` with exact §4 DDL, `conf.py` with
   validation. Test: migration applies cleanly to a real PG container.
4. **partitions.py** — pure functions first (TDD the month math against DESIGN's
   example), then `ensure_partitions`. `test_partitions.py`.
5. **publisher.py** — `publish`/`bulk_publish`, `test_publisher.py`.
6. **worker.py** — port `../outbox/worker.py` with renames (`queue_name`,
   `default_retry_delays`, `rmq_url`), `test_worker.py`. This is the biggest file;
   port mechanically, don't redesign.
7. **relay.py + entrypoints** — `Relay` with `**_ignored`, `conf.py::build_dsn` +
   `validate_settings()`, tiny-script pattern, `djoutbox_relay` command wrapper,
   partition-admin task. `test_relay.py`.
8. **admin.py** — both admins + partition filter; smoke-test with Django test client
   (changelist 200s, filter narrows queryset).
9. **Lazy-import polish** — `__init__.py` `__getattr__`, boundary test.
10. **Docs touch-up** — README quickstart mirroring DESIGN steps 1–8 (only if asked).

Recommended commit granularity = milestones (commit only when asked).

---

## 14. Gotchas for the implementer (do not skip)

- **`managed = False` models + migrations**: Django's migration autodetector will want to
  create `CreateModel` operations; write `0001_initial.py` by hand with only `RunSQL`
  (plus `RunPython` no-op reverse if desired). `makemigrations` must produce EMPTY
  migrations afterward — add a test asserting `makemigrations --check --dry-run` passes,
  else unmanaged-model drift crept in.
- **`GENERATED BY DEFAULT AS IDENTITY`** on pending.id — `ALWAYS` breaks the move CTE.
- **Timezone awareness**: all datetimes UTC-aware (`django.utils.timezone.now()` on the
  publish path; asyncpg returns aware datetimes for `timestamptz` — assert in tests).
- **aio-pika expiration encoding**: relay passes the `timedelta` straight to
  `aio_pika.Message(expiration=...)`; aio-pika encodes to ms string. `None` expiration →
  omit.
- **Quorum queues** reject `x-max-priority` etc. — don't add extra queue args beyond the
  ported set; quorum also requires `durable=True`.
- **Delay-queue dead-lettering to the default exchange** routes by *queue name* — that is
  why `_delayed_retry` publishes with `routing_key=self.queue_name`, never the original
  routing key. Getting this wrong silently drops retries.
- **`x-delivery-count` header is a string** (AMQP headers are loosely typed across
  clients; `../outbox` stores `str(count)` and reads `int(...)`) — keep the cast both ways.
- **prometheus-client global registry**: importing `djoutbox.metrics` twice is safe
  (module cached), but tests that reimport under different module names will raise
  `ValueError: Duplicated timeseries`. Import once, everywhere, as `from djoutbox import
  metrics`.
- **`parse_duration` zero**: `"0"`/its spellings → 0 ms → instant-requeue path; ensure
  `_set_up_queues` skips creating delay topology for 0 (parity; easy to regress).
- **DESIGN vs `../outbox` naming**: when they conflict, follow DESIGN
  (`queue_name`, `default_retry_delays`, `rmq_url`, `djoutbox` logger name). When DESIGN
  is silent, follow `../outbox` source, not its README (e.g. source supports `d`/`h` in
  `parse_duration` even though README implies otherwise).
- **Multiple relay pods** are supported by design (SKIP LOCKED + DuplicateTable
  tolerance) — never add leader-election or locking "improvements".
- **Don't add retention/cleanup commands** — DESIGN explicitly excludes them.
- **`attempt_count` off-by-one**: total attempts = `len(retry_delays) + 1`; the header
  stores the count of the *upcoming* delivery. Port the comparisons exactly as in
  `../outbox/worker.py` lines ~105–120 and ~213 — this is the easiest place to introduce
  an off-by-one; tests from `../outbox` catch it, port them.
- **pg_notify is transactional**: delivery happens at COMMIT, so a `publish()` rolled
  back with its business transaction never wakes the relay — this is exactly the outbox
  semantics we want; do not "optimize" by notifying outside the trigger.
- **LISTEN connection is dedicated**: hold one pooled connection for the relay's whole
  lifetime and use it ONLY for `add_listener` (asyncpg delivers notifications on that
  connection; running other queries on it risks missing nothing but complicates
  reasoning). Release it in `finally`.
- **Optional pydantic**: the guarded-import pattern (§5) must appear in BOTH
  `publisher.py` and `worker.py`; nowhere may pydantic be imported unguarded. Same for
  the `type(None)` fallback trick — keep `TYPE_CHECKING` guard so type checkers still see
  the real class.
