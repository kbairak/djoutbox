# Observability

## Logging

djoutbox uses Python's standard `logging` module under the logger name `"djoutbox"`. Logs are enabled by default at `INFO` level.

### Log levels

| Level | What's logged |
|-------|---------------|
| `DEBUG` | Backlog counts, queue bindings, partition creation |
| `INFO` | Messages received, processed successfully, relay started |
| `WARNING` | Retries, rejections, high backlog (>100), messages sent to DLQ |
| `ERROR` | Publish failures, deserialization errors, relay errors |

### Configuration

```python
import logging

# Configure for your application
logging.basicConfig(level=logging.INFO)

# Control djoutbox logger specifically
logging.getLogger("djoutbox").setLevel(logging.WARNING)

# Disable djoutbox logs entirely
logging.getLogger("djoutbox").propagate = False
```

## Prometheus metrics

djoutbox exposes 11 Prometheus metrics that auto-register to `prometheus_client`'s global registry. They integrate seamlessly with your existing Prometheus instrumentation:

```python
from prometheus_client import start_http_server
from djoutbox import Relay, Worker

# Start Prometheus HTTP server (once per process)
start_http_server(9090)

# Outbox metrics are automatically included
relay = Relay(db_dsn="...", rmq_url="...")
worker = Worker(consumers=[...], rmq_url="...")

# Both relays and workers register metrics — serve them from your own endpoint
```

You don't need an HTTP server inside djoutbox — metrics are registered on the global `prometheus_client` registry. Serve them from:

- A Django view (via `django-prometheus` or manually with `prometheus_client.exposition.generate_latest()`)
- A standalone HTTP server in the worker/relay process
- Your existing metrics endpoint

### Metrics reference

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `outbox_messages_published_total` | Counter | `exchange_name` | Messages successfully published from outbox table |
| `outbox_publish_failures_total` | Counter | `exchange_name`, `failure_type`, `error_type` | Failed publish attempts to RabbitMQ |
| `outbox_message_age_seconds` | Histogram | `exchange_name` | Time message spent in outbox table before publishing |
| `outbox_poll_duration_seconds` | Histogram | `exchange_name` | Time to poll DB and publish one batch |
| `outbox_table_backlog` | Gauge | `exchange_name` | Current unsent messages in outbox table |
| `outbox_messages_received_total` | Counter | `queue`, `exchange_name` | Messages received from RabbitMQ queue |
| `outbox_messages_processed_total` | Counter | `queue`, `exchange_name`, `status` | Messages processed (success/failed/rejected/deserialization_failed) |
| `outbox_retry_attempts_total` | Counter | `queue`, `delay_seconds` | Retry attempts by delay tier |
| `outbox_message_processing_duration_seconds` | Histogram | `queue`, `exchange_name` | Handler execution time |
| `outbox_dlq_messages` | Gauge | `queue` | Current messages in dead-letter queue |
| `outbox_active_consumers` | Gauge | `queue`, `exchange_name` | Active consumer connections |

### Disabling metrics

```python
# Not yet supported — metrics are always registered.
# If you want to disable, set the prometheus registry to a no-op.
```

## Health checks

### Relay health

The relay logs startup and error messages. Key health indicators:

- Backlog > 100 → WARNING log
- Publish failures → ERROR log + `outbox_publish_failures_total` counter
- Partition admin errors → ERROR log

### Worker health

The worker logs received/processed messages at INFO level. Key health indicators:

- Deserialization failures → ERROR log + `deserialization_failed` status
- Retries → WARNING log + `outbox_retry_attempts_total` counter
- DLQ messages → `outbox_dlq_messages` gauge

Monitor DLQ message counts — they indicate messages that couldn't be processed after all retries and need manual inspection.