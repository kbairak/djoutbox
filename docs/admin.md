# Django Admin

djoutbox registers both `djoutbox_pending` and `djoutbox_sent` tables in the Django admin for visibility into message flow.

## PendingMessageAdmin

Shows messages waiting to be relayed. Read-only.

List display: `id`, `routing_key`, `created_at`, `send_after`, `expiration`

Search: `routing_key`

Date hierarchy: `created_at`

## SentMessageAdmin

Shows archived messages that have been relayed. Read-only, with a partition filter for efficient browsing.

List display: `id`, `routing_key`, `created_at`, `send_after`, `expiration`, `sent_at`

Search: `routing_key`

Date hierarchy: `created_at`

Filter: **Partition** — dropdown showing all existing partitions of `djoutbox_sent`. Selecting a partition filters the queryset to only show rows within that partition's date range, enabling constraint exclusion.

### Partition filter

The `PartitionFilter` reads existing partitions from `pg_inherits` and displays them as a dropdown. Selecting a partition adds `created_at__gte` and `created_at__lt` to the queryset, which PostgreSQL's constraint exclusion uses to scan only the relevant partition.

This is essential for large archives — querying the full `djoutbox_sent` table (which is partitioned) without a partition filter would scan all partitions, potentially causing slow queries.

## Permissions

Both admin views are read-only:

- `has_add_permission` → `False`
- `has_change_permission` → `False`
- `has_delete_permission` → `False`

Removal of sent messages is handled by dropping partitions (see [Advanced: Partition lifecycle](advanced.md#partition-lifecycle)).

## Disabling the admin

If you don't want the admin views, unregister the models:

```python
# your_app/admin.py
from django.contrib import admin
from djoutbox.models import PendingMessage, SentMessage

admin.site.unregister(PendingMessage)
admin.site.unregister(SentMessage)
```