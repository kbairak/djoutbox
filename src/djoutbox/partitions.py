from __future__ import annotations

import logging
import re
from datetime import date, timedelta
from typing import Any, Literal, cast

import asyncpg

logger = logging.getLogger("djoutbox")

GRANULARITY_RE = re.compile(r"^(\d+)([dm])$")
PARTITION_NAME_RE = re.compile(r"^djoutbox_sent_(\d{8})_(\d{8})$")


def parse_granularity(s: str) -> tuple[Literal["d", "m"], int]:
    match = GRANULARITY_RE.match(s)
    if not match:
        raise ValueError(f"Invalid granularity: {s!r} (expected Nd or Nm)")
    n = int(match.group(1))
    if n < 1:
        raise ValueError(f"Granularity N must be >= 1, got {n}")
    return (cast(Literal["d", "m"], match.group(2)), n)


def next_partition(start: date, kind: Literal["d", "m"], n: int) -> tuple[date, date]:
    if kind == "d":
        end = start + timedelta(days=n)
    else:
        if start.day == 1:
            month = start.month + n
            year = start.year + (month - 1) // 12
            month = ((month - 1) % 12) + 1
            end = date(year, month, 1)
        else:
            month = start.month + 1
            year = start.year + (month - 1) // 12
            month = ((month - 1) % 12) + 1
            end = date(year, month, 1)
    return (start, end)


def _parse_partition_name(name: str) -> tuple[date, date] | None:
    match = PARTITION_NAME_RE.match(name)
    if not match:
        return None
    start = date(int(match.group(1)[:4]), int(match.group(1)[4:6]), int(match.group(1)[6:]))
    end = date(int(match.group(2)[:4]), int(match.group(2)[4:6]), int(match.group(2)[6:]))
    return (start, end)


async def needed_partitions(
    conn: asyncpg.Connection[Any], granularity: str
) -> list[tuple[str, date, date]]:
    kind, n = parse_granularity(granularity)
    rows = await conn.fetch(
        """SELECT child.relname FROM pg_inherits i
JOIN pg_class child ON child.oid = i.inhrelid
JOIN pg_class parent ON parent.oid = i.inhparent
WHERE parent.relname = 'djoutbox_sent'"""
    )
    max_end: date | None = None
    for row in rows:
        parsed = _parse_partition_name(row["relname"])
        if parsed and (max_end is None or parsed[1] > max_end):
            max_end = parsed[1]
    today = date.today()
    if max_end is None:
        oldest = await conn.fetchval("SELECT min(created_at) FROM djoutbox_pending")
        oldest_date = oldest.date() if oldest is not None else today
        start = min(today, oldest_date)
    else:
        start = max_end
    horizon = n if kind == "d" else 31 * n
    end_date = today + timedelta(days=horizon)
    result: list[tuple[str, date, date]] = []
    while start < end_date:
        part_start, part_end = next_partition(start, kind, n)
        name = f"djoutbox_sent_{part_start.strftime('%Y%m%d')}_{part_end.strftime('%Y%m%d')}"
        result.append((name, part_start, part_end))
        start = part_end
    return result


async def ensure_partitions(conn: asyncpg.Connection[Any], granularity: str) -> None:
    partitions = await needed_partitions(conn, granularity)
    created = 0
    for name, start, end in partitions:
        try:
            await conn.execute(
                f"CREATE TABLE {name} PARTITION OF djoutbox_sent "
                f"FOR VALUES FROM ('{start.isoformat()}') TO ('{end.isoformat()}')"
            )
            logger.debug("Created partition %s", name)
            created += 1
        except asyncpg.exceptions.DuplicateTableError:
            pass
    if created:
        logger.info("Ensured %d partitions", created)


def list_partitions() -> list[tuple[str, date, date]]:
    from django.db import connection

    with connection.cursor() as cursor:
        cursor.execute(
            """SELECT child.relname FROM pg_inherits i
JOIN pg_class child ON child.oid = i.inhrelid
JOIN pg_class parent ON parent.oid = i.inhparent
WHERE parent.relname = 'djoutbox_sent'"""
        )
        rows = cursor.fetchall()
    result: list[tuple[str, date, date]] = []
    for (name,) in rows:
        parsed = _parse_partition_name(name)
        if parsed:
            result.append((name, parsed[0], parsed[1]))
    return result
