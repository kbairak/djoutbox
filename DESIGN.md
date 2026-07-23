# djoutbox — Design

Transactional outbox pattern for Django, PostgreSQL, and RabbitMQ.

Port of https://github.com/kbairak/outbox — adapted to Django idioms.

## Overview

```
                   ┌──────────────────────┐
                   │   Django app          │
                   │   transaction.atomic()│
                   │   OutboxMessage       │
                   │   .objects.publish()  │
                   └──────┬───────────────┘
                          │ INSERT
                          v
                   ┌──────────────────────┐
                   │   outbox_table        │
                   │   (PostgreSQL)        │
                   └──────┬───────────────┘
                          │ NOTIFY / SELECT FOR UPDATE
                          v
                   ┌──────────────────────┐
                   │   message_relay       │  ← management command
                   │   (asyncpg + aio-pika)│
                   └──────┬───────────────┘
                          │ publish
                          v
                   ┌──────────────────────┐
                   │   RabbitMQ            │
                   │   topic exchange      │
                   └──────┬───────────────┘
                          │ consume
                          v
                   ┌──────────────────────┐
                   │   worker              │  ← management command
                   │   (aio-pika)          │
                   │   → handler callbacks │
                   └──────────────────────┘
```

## Components

### 1. Django Model: `OutboxMessage`

`djoutbox/models.py`

| Field | Type | Notes |
|---|---|---|
| `id` | `BigAutoField` | PK |
| `routing_key` | `TextField` | Not null |
| `body` | `BinaryField` | JSON-serialized bytes |
| `tracking_ids` | `JSONField` | List of UUIDs |
| `created_at` | `DateTimeField` | auto `now_add` |
| `expiration` | `DurationField` | Nullable |
| `send_after` | `DateTimeField` | Defaults to `now()` |
| `sent_at` | `DateTimeField` | Nullable, set after relay publishes |

**Manager** provides:
- `publish(routing_key, body, *, expiration=None, eta=None)` — creates one row
- `bulk_publish(messages: list[OutboxMessage])` — bulk insert

Both are sync, safe inside `transaction.atomic()`. No `handle`/`connection` parameter — always uses the current DB alias.

### 2. Migration

`djoutbox/migrations/0001_initial.py`

- `CreateModel` for `outbox_table`
- `AddIndex` — `outbox_pending_idx` (partial: `WHERE sent_at IS NULL` on `(send_after, created_at)`)
- `AddIndex` — `outbox_cleanup_idx` (partial: `WHERE sent_at IS NOT NULL` on `(sent_at)`)
- `RunSQL` — NOTIFY trigger function + trigger on INSERT

### 3. Admin

`djoutbox/admin.py`

- Default queryset: `sent_at__isnull=True` (pending only) — hits `outbox_pending_idx`
- List display: `routing_key`, `created_at`, `send_after`, `sent_at`
- Filters: pending/sent/all, routing_key, date range
- Read-only fields: `sent_at`, `tracking_ids`
- Warning tooltip when "All" is selected on large tables

### 4. Consumers

`djoutbox/consumers.py`

**`Consumer`** dataclass:
- `binding_key: str`
- `queue: str` (auto-generated from callback module+qualname if empty)
- `callback: Callable` (sync or async, auto-wrapped)
- `retry_delays: Sequence[str] | None`

**`consume(binding_key, queue, retry_delays=None)`** decorator:
- Returns a `Consumer` instance
- Sync callbacks auto-wrapped in `asyncio.to_thread`

**`consumer_from_string(path)`**:
- Parses `path.to.module::attr` — imports module, gets attribute
- Supports `Consumer` instance, callable (wraps into `Consumer`)
- Supports `Worker` instance (passes through)

### 5. Message Relay

`djoutbox/relay_core.py` — direct port of original `MessageRelay`

Management command: `./manage.py message_relay`

- Reads `OUTBOX_RMQ_URL` env var (required)
- Reads DB credentials from `settings.DATABASES['default']`
- Opens `asyncpg.Pool` (not Django ORM — needs LISTEN/NOTIFY and `SELECT FOR UPDATE SKIP LOCKED`)
- Opens `aio_pika.connect_robust(OUTBOX_RMQ_URL)`
- Declares topic exchange (`outbox`)
- LISTENs on `outbox_channel`
- Polls: `SELECT ... FOR UPDATE SKIP LOCKED LIMIT batch_size`
- Publishes to RMQ, marks sent or deletes
- Waits on NOTIFY with `notification_timeout` fallback
- Graceful shutdown on SIGINT/SIGTERM

### 6. Worker

`djoutbox/worker_core.py` — direct port of original `Worker`

Management command: `./manage.py worker path.to.module::attr`

- Reads `OUTBOX_RMQ_URL` env var (required)
- Resolves `path.to.module::attr` via `consumer_from_string()`
- Opens `aio_pika.connect_robust(OUTBOX_RMQ_URL)`
- Declares exchange, DLX, delay exchanges/queues, consumer queues
- Starts consuming with `prefetch_count`
- Handles retries via TTL-based delay queues
- Dead-letter queue on exhaustion
- `Reject` exception → skip to DLQ
- Graceful shutdown on SIGINT/SIGTERM

### 7. Settings

No `DJOUTBOX` settings dict for now. Configuration via env vars:

| Env var | Required | Default | Used by |
|---|---|---|---|
| `OUTBOX_RMQ_URL` | Yes | — | relay, worker |
| `OUTBOX_EXCHANGE_NAME` | No | `outbox` | relay, worker |
| `OUTBOX_RETRY_DELAYS` | No | `1s,10s,1m,5m` | worker |
| `OUTBOX_PREFETCH_COUNT` | No | `10` | worker |
| `OUTBOX_BATCH_SIZE` | No | `50` | relay |
| `OUTBOX_NOTIFICATION_TIMEOUT` | No | `60` | relay |

### 8. Utils

`djoutbox/utils.py` — direct port of original:

- `parse_duration(s)` — duration string → milliseconds
- `Reject` — exception class
- `tracking_ids_contextvar`, `get_tracking_ids()`, `tracking()` — tracking ID chain
- `truncate_body(body)` — reprlib-based truncation for logging

### 9. Metrics

`djoutbox/metrics.py` — direct port of original:

- `outbox_messages_published_total`
- `outbox_publish_failures_total`
- `outbox_message_age_seconds`
- `outbox_poll_duration_seconds`
- `outbox_table_backlog`
- `outbox_messages_received_total`
- `outbox_messages_processed_total`
- `outbox_retry_attempts_total`
- `outbox_message_processing_duration_seconds`
- `outbox_dlq_messages`
- `outbox_active_consumers`

## What stays async

| Component | Reason |
|---|---|
| `message_relay` | asyncpg + aio-pika |
| `worker` | aio-pika consumer |
| Consumer callbacks | aio-pika dispatches into coroutines |

## What is sync

| Component | Reason |
|---|---|
| Publisher (`OutboxMessage.objects.publish()`) | Django ORM is sync |
| Admin | Django admin is sync |
| `@consume` sync callbacks | Auto-wrapped in `asyncio.to_thread` |

## Table name

Hardcoded: `outbox_table`

## Archiving / retention

Deferred. Relay deletes sent messages immediately. No archive table yet.

## Deferred (TBD)

- `DJOUTBOX` settings dict
- Archive/retention strategy
- Partitioning support
- Integration with Django REST Framework