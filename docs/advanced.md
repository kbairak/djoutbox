# Advanced

## Idempotency

djoutbox provides **at-least-once delivery** semantics. Messages may be delivered multiple times due to:

- Retries after failures
- Network issues or worker restarts
- RabbitMQ redeliveries
- `FOR UPDATE SKIP LOCKED` race conditions with multiple relay pods

Your handlers **must be idempotent** to handle duplicate deliveries correctly:

```python
@consume("order.created")
async def handle_order(order_id: int):
    # ✅ Check if already processed
    if await is_order_processed(order_id):
        return

    await process_order(order_id)
    await mark_order_processed(order_id)
```

## Pre-provisioning RabbitMQ resources

By default, the relay and worker create all required RabbitMQ exchanges, queues, and bindings on startup. While convenient for development, this can cause issues in production:

- **Orphaned resources** when code is removed
- **Lack of oversight** into what resources exist
- **Security concerns** — applications shouldn't have permission to create infrastructure

### Exchanges

| Name Pattern | Type | Purpose |
|--------------|------|---------|
| `{exchange_name}` | TOPIC | Main exchange for routing messages from relay to consumers |
| `{exchange_name}.dlx` | DIRECT | Dead-letter exchange for failed messages |
| `{exchange_name}.delay_{duration}` | FANOUT | Delay exchange for retry backoff (one per unique delay value) |

### Queues

| Name Pattern | Purpose |
|--------------|---------|
| `{consumer.queue}` | Consumer's main queue |
| `{consumer.queue}.dlq` | Dead-letter queue |
| `{exchange_name}.delay_{duration}` | Delay queue (one per unique delay value) |

### Bindings

| Exchange | Routing Key | Queue |
|----------|-------------|-------|
| `{exchange_name}` | `{consumer.binding_key}` | `{consumer.queue}` |
| `{exchange_name}.dlx` | `{consumer.queue}` | `{consumer.queue}.dlq` |
| `{exchange_name}.delay_{duration}` | (fanout) | `{exchange_name}.delay_{duration}` |

### Queue arguments

Consumer queues:

- `x-dead-letter-exchange`: `{exchange_name}.dlx`
- `x-dead-letter-routing-key`: `{consumer.queue}`
- `x-queue-type`: `quorum`

Delay queues:

- `x-message-ttl`: milliseconds (e.g., 1000 for "1s")
- `x-dead-letter-exchange`: `""` (default exchange — routes by queue name)
- `x-queue-type`: `quorum`

### Terraform example

<details>
<summary>Configuration variables</summary>

```hcl
terraform {
  required_providers {
    rabbitmq = {
      source  = "cyrilgdn/rabbitmq"
      version = "~> 1.8"
    }
  }
}

provider "rabbitmq" {
  endpoint = "http://localhost:15672"
  username = "admin"
  password = "admin"
}

locals {
  exchange_name = "outbox"

  retry_delays = {
    "1s"  = 1000
    "10s" = 10000
    "1m"  = 60000
    "5m"  = 300000
  }

  consumers = [
    {
      queue       = "myapp.handle_user_created"
      binding_key = "user.created"
    },
    {
      queue       = "myapp.handle_user_updated"
      binding_key = "user.updated"
    },
  ]
}
```

</details>

<details>
<summary>Exchanges, queues, and bindings</summary>

```hcl
# Main topic exchange
resource "rabbitmq_exchange" "main" {
  name  = local.exchange_name
  vhost = "/"

  settings {
    type    = "topic"
    durable = true
  }
}

# Dead letter exchange
resource "rabbitmq_exchange" "dlx" {
  name  = "${local.exchange_name}.dlx"
  vhost = "/"

  settings {
    type    = "direct"
    durable = true
  }
}

# Delay exchanges
resource "rabbitmq_exchange" "delay" {
  for_each = local.retry_delays

  name  = "${local.exchange_name}.delay_${each.key}"
  vhost = "/"

  settings {
    type    = "fanout"
    durable = true
  }
}

# Delay queues
resource "rabbitmq_queue" "delay" {
  for_each = local.retry_delays

  name  = "${local.exchange_name}.delay_${each.key}"
  vhost = "/"

  settings {
    durable = true
    arguments = {
      "x-message-ttl"          = each.value
      "x-dead-letter-exchange" = ""
      "x-queue-type"           = "quorum"
    }
  }
}

# Consumer queues
resource "rabbitmq_queue" "consumer" {
  for_each = { for c in local.consumers : c.queue => c }

  name  = each.value.queue
  vhost = "/"

  settings {
    durable = true
    arguments = {
      "x-dead-letter-exchange"    = "${local.exchange_name}.dlx"
      "x-dead-letter-routing-key" = each.value.queue
      "x-queue-type"              = "quorum"
    }
  }
}

# Dead letter queues
resource "rabbitmq_queue" "dlq" {
  for_each = { for c in local.consumers : c.queue => c }

  name  = "${each.value.queue}.dlq"
  vhost = "/"

  settings {
    durable = true
    arguments = {
      "x-queue-type" = "quorum"
    }
  }
}

# Bind consumer queues to main exchange
resource "rabbitmq_binding" "consumer" {
  for_each = { for c in local.consumers : c.queue => c }

  source           = local.exchange_name
  vhost            = "/"
  destination      = each.value.queue
  destination_type = "queue"
  routing_key      = each.value.binding_key
}

# Bind DLQs to dead letter exchange
resource "rabbitmq_binding" "dlq" {
  for_each = { for c in local.consumers : c.queue => c }

  source           = "${local.exchange_name}.dlx"
  vhost            = "/"
  destination      = "${each.value.queue}.dlq"
  destination_type = "queue"
  routing_key      = each.value.queue
}

# Bind delay queues to delay exchanges
resource "rabbitmq_binding" "delay" {
  for_each = local.retry_delays

  source           = "${local.exchange_name}.delay_${each.key}"
  vhost            = "/"
  destination      = "${local.exchange_name}.delay_${each.key}"
  destination_type = "queue"
  routing_key      = ""
}
```

</details>

<details>
<summary>User and permissions</summary>

```hcl
# Create restricted application user
resource "rabbitmq_user" "app" {
  name     = "myapp"
  password = "secure_password"
  tags     = []
}

# Grant limited permissions
resource "rabbitmq_permissions" "app" {
  user  = rabbitmq_user.app.name
  vhost = "/"

  permissions {
    configure = "^$"                                              # Cannot create/delete resources
    write     = "^(${local.exchange_name}|${local.exchange_name}\\.delay_.*)$"  # Can publish to main and delay exchanges
    read      = ".*"                                              # Can consume from all queues
  }
}
```

</details>

## Partition lifecycle

### Creation

Partitions are created automatically by the relay:

1. On startup (`ensure_partitions()` runs immediately)
2. Every 300 seconds thereafter (`partition_admin_loop()`)

The relay creates partitions covering the horizon:

- For `"1d"` granularity: covers the next 1 day
- For `"1m"` granularity: covers the next 31 days (one full month minimum)

### Retention

Partition retention is **your responsibility**. djoutbox never drops partitions automatically.

Manual retention:

```sql
-- List partitions
SELECT child.relname, pg_catalog.pg_size_pretty(pg_total_relation_size(child.oid))
FROM pg_inherits i
JOIN pg_class child ON child.oid = i.inhrelid
JOIN pg_class parent ON parent.oid = i.inhparent
WHERE parent.relname = 'djoutbox_sent';

-- Drop an old partition
DROP TABLE djoutbox_sent_20240101_20240102;
```

Automated retention (e.g., cron job or pg_cron):

```sql
-- Keep 90 days of history
SELECT 'DROP TABLE ' || child.relname || ';'
FROM pg_inherits i
JOIN pg_class child ON child.oid = i.inhrelid
JOIN pg_class parent ON parent.oid = i.inhparent
WHERE parent.relname = 'djoutbox_sent'
  AND split_part(child.relname, '_', 3)::date < CURRENT_DATE - 90;
```

### Safety

Before dropping a partition, ensure all messages have been processed. The `djoutbox_sent` table only contains messages that have already been relayed — it's safe to drop partitions as long as you don't need the historical data.

## Production checklist

- [ ] **Idempotent handlers** — All consumers handle duplicate deliveries
- [ ] **RabbitMQ resources pre-provisioned** — Use Terraform or similar IaC
- [ ] **Partition retention policy** — Schedule regular partition cleanup
- [ ] **Monitoring** — Set up alerts on `outbox_dlq_messages`, `outbox_table_backlog`, and `outbox_publish_failures_total`
- [ ] **Multiple relay pods** — Run at least 2 relay instances for high availability
- [ ] **Multiple worker pods** — Run at least 2 worker instances per queue
- [ ] **Logging** — Configure log aggregation for the `djoutbox` logger
- [ ] **Django settings validation** — `DjoutboxConfig.ready()` validates settings on startup; verify no `ImproperlyConfigured` errors
- [ ] **Database connection limits** — Relay uses asyncpg pool (min 1, max 4 connections); account for this in your connection budget