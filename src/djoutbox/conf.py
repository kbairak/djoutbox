from __future__ import annotations

import re
from typing import Any

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

from djoutbox.log import logger

DEFAULTS: dict[str, Any] = {
    "rmq_url": "amqp://guest:guest@localhost/",
    "exchange_name": "outbox",
    "default_retry_delays": ("1s", "10s", "1m", "5m"),
    "prefetch_count": 10,
    "batch_size": 50,
    "notification_timeout": 60.0,
    "expiration": None,
    "db_alias": "default",
    "sent_archive": {
        "enabled": True,
        "granularity": "1d",
    },
}

DURATION_RE = re.compile(r"^([^0]\d*d)?([^0]\d*h)?([^0]\d*m)?([^0]\d*s)?([^0]\d*ms)?$")

ZERO_DURATIONS = {"0", "0ms", "0s", "0m", "0h", "0d"}


def parse_duration(s: str) -> int:
    if s in ZERO_DURATIONS:
        return 0
    match = DURATION_RE.search(s)
    if match is None:
        raise ValueError(f"Invalid duration string: {s!r}")
    days_s, hours_s, minutes_s, seconds_s, ms_s = match.groups()
    result = 0
    if days_s:
        days = int(days_s[:-1])
        result += days * 86400000
    if hours_s:
        hours = int(hours_s[:-1])
        if not 1 <= hours <= 23:
            raise ValueError(f"Invalid hours value {hours}, must be 1-23")
        result += hours * 3600000
    if minutes_s:
        minutes = int(minutes_s[:-1])
        if not 1 <= minutes <= 59:
            raise ValueError(f"Invalid minutes value {minutes}, must be 1-59")
        result += minutes * 60000
    if seconds_s:
        seconds = int(seconds_s[:-1])
        if not 1 <= seconds <= 59:
            raise ValueError(f"Invalid seconds value {seconds}, must be 1-59")
        result += seconds * 1000
    if ms_s:
        ms = int(ms_s[:-2])
        if not 1 <= ms <= 999:
            raise ValueError(f"Invalid milliseconds value {ms}, must be 1-999")
        result += ms
    if result == 0:
        raise ValueError(f"Invalid duration string: {s!r}")
    return result


GRANULARITY_RE = re.compile(r"^(\d+)([dm])$")


def parse_granularity(s: str) -> tuple[str, int]:
    match = GRANULARITY_RE.match(s)
    if not match:
        raise ValueError(f"Invalid granularity: {s!r} (expected Nd or Nm)")
    n = int(match.group(1))
    if n < 1:
        raise ValueError(f"Granularity N must be >= 1, got {n}")
    return (match.group(2), n)


def get_setting(key: str) -> Any:
    raw = getattr(settings, "DJOUTBOX", {})
    return raw.get(key, DEFAULTS[key])


def validate_settings() -> None:
    raw = getattr(settings, "DJOUTBOX", {})
    for key in ("default_retry_delays",):
        value = raw.get(key, DEFAULTS[key])
        if isinstance(value, (list, tuple)):
            for s in value:
                parse_duration(s)
    archive = raw.get("sent_archive", DEFAULTS["sent_archive"])
    if isinstance(archive, dict):
        granularity = archive.get("granularity", DEFAULTS["sent_archive"]["granularity"])
        try:
            parse_granularity(granularity)
        except ValueError as exc:
            raise ImproperlyConfigured(str(exc)) from exc
    logger.debug("djoutbox settings validated")


def build_dsn(db_alias: str | None = None) -> str:
    from urllib.parse import quote

    validate_settings()
    alias = db_alias or get_setting("db_alias")
    db = settings.DATABASES[alias]
    engine = db["ENGINE"]
    if "postgresql" not in engine:
        raise ImproperlyConfigured(
            f"djoutbox requires a PostgreSQL database, got {engine}"
        )
    user = quote(db.get("USER", "") or "")
    password = quote(db.get("PASSWORD", "") or "")
    host = db.get("HOST") or "localhost"
    port = db.get("PORT") or "5432"
    name = db.get("NAME", "")
    return f"postgresql://{user}:{password}@{host}:{port}/{name}"
