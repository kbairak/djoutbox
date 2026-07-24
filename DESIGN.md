# djoutbox — Design

Transactional outbox pattern for Django, PostgreSQL, and RabbitMQ

## Overview

1. uv/pip install djoutbox

2. Add djoutbox to INSTALLED_APPS

3. Configure outbox in settings (partitioning, RMQ connection string, RMQ exchange name, expiration, etc)

4. `./manage.py migrate`

5. In some view or whatever:

   ```python
   from djoutbox import publish

   publish("routing_key", payload)  # also expiration and/or eta as kwargs
   ```

6. Write a worker file (no mgmt command, no auto-discovery)

   ```python
   import asyncio
   from djoutbox import consume, Worker

   @consume("binding_key")  # also queue_name and/or retry_delays as kwargs
   async def work(payload):  # Also routing_key, message, tracking_ids, attempt_count, will be added by the framework if present in the signature
      ...

   from django.conf import settings
   worker = Worker(consumers=[work, ...], **settings.DJOUTBOX)  # Or pass rmq_url, exchange_name, default_retry_delays, prefetch_count
   asyncio.run(worker.run())
   ```

   - Worker is not django-specific, it could work on other machines entirely
   - You can still put `worker.run` in the `handle` method of a management command; this way you have access to django stuff like settings and ORM

7. Start one or more pods with `./manage.py djoutbox_relay`, connects to PG and RMQ based on django settings

8. Start one or more pods that run one or more workers (with or without django)

## Notes

- 'publish' is sync because django transactions are sync; if user is writing async, they want to put publish in an async_to_sync block
- 'publish' could be an alias or thin wrapper to `Outbox.objects.create`
- if publish payload is pydantic model, it is `.model_dump_json`ed
- if consumer payload has a pydantic model type hint, it is `.model_validate_json`ed
- otherwise, it is `json.load`ed
- otherwise, it is bytes
- We want feature-parity with ../outbox
  - tracking ids
  - topic exchanges
  - retry exchanges/queues
  - dead-letter exchanges/queues
  - prometheus metrics
  - bulk-publish
  - graceful worker shutdown
- sync and async consumer functions
- configure logging with getLogger('djoutbox')
- relay wakes on PG LISTEN/NOTIFY (trigger on insert when send_after <= now()), timeout as backstop
- pydantic support is optional (extra), package works without it installed

## Partitioning

Two tables: `djoutbox_pending` (regular) and `djoutbox_sent` (range-partitioned by `created_at`).

### Relay flow

```
relay_loop():                               # hot path, no partition awareness
    SELECT * FROM pending
    WHERE created_at < now() - retry_delay
    LIMIT batch_size

    for each message:
        publish to RMQ
        CTE: DELETE FROM pending WHERE id = $1 RETURNING *;
             INSERT INTO sent SELECT * FROM $1;

partition_admin():                          # runs every 300s in same event loop
    for partition in needed_partitions():
        try:
            CREATE TABLE YYYYMMDD_YYYYMMDD PARTITION OF sent
            FOR VALUES FROM (start) TO (end)
        except DuplicateTable:
            pass
```

`needed_partitions()`:

1. Query `pg_partitions` for max `upper_bound` of `sent_*` partitions
2. If none → start from today
3. Create partitions to cover up to at least the most recent `created_at` + 1 day
4. Each partition starts where previous ended → seamless granularity switch

On relay startup, `partition_admin` runs immediately once, then every 300s.

### Granularity config

```python
DJOUTBOX = {
    "SENT_ARCHIVE": {
        "ENABLED": True,
        "GRANULARITY": "1d",  # "Nd" (N days) or "Nm" (N months, captures full calendar months)
    }
}
```

Partition names: `YYYYMMDD_YYYYMMDD` (no granularity prefix).

Month granularity captures full calendar months. Example with `1m`:

- Existing partitions cover up to 20 Jan → next partition `20260120_20260131`
- Next one `20260201_20260228`, then `20260301_20260331`, etc.

### Partition lifecycle

- Migrations create `djoutbox_sent` as partitioned parent table, zero partitions
- Partition creation is relay's job (auto-create on startup + every 300s)
- Partition retention is **user's responsibility** (`DROP TABLE` via psql, metabase, cron, etc.)
- No retention or lifecycle management inside djoutbox
- No management commands for partitions

### Admin panel

- `djoutbox_pending` exposed normally in admin (small table, no concerns)
- `djoutbox_sent` exposed in admin with a **partition dropdown filter** so queries hit only the relevant partition(s)
