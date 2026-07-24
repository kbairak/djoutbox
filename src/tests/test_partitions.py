from datetime import date

import pytest

from djoutbox.partitions import next_partition, parse_granularity


def test_parse_granularity_days():
    assert parse_granularity("1d") == ("d", 1)
    assert parse_granularity("7d") == ("d", 7)
    assert parse_granularity("30d") == ("d", 30)


def test_parse_granularity_months():
    assert parse_granularity("1m") == ("m", 1)
    assert parse_granularity("3m") == ("m", 3)


def test_parse_granularity_invalid():
    with pytest.raises(ValueError, match="N must be >= 1"):
        parse_granularity("0d")
    with pytest.raises(ValueError, match="Invalid granularity"):
        parse_granularity("1x")
    with pytest.raises(ValueError, match="Invalid granularity"):
        parse_granularity("foo")


def test_next_partition_day():
    start = date(2026, 1, 1)
    s, e = next_partition(start, "d", 1)
    assert s == date(2026, 1, 1)
    assert e == date(2026, 1, 2)

    s2, e2 = next_partition(e, "d", 1)
    assert s2 == date(2026, 1, 2)
    assert e2 == date(2026, 1, 3)


def test_next_partition_7_days():
    start = date(2026, 1, 1)
    s, e = next_partition(start, "d", 7)
    assert s == date(2026, 1, 1)
    assert e == date(2026, 1, 8)


def test_next_partition_month_start():
    start = date(2026, 2, 1)
    s, e = next_partition(start, "m", 1)
    assert s == date(2026, 2, 1)
    assert e == date(2026, 3, 1)


def test_next_partition_month_partial():
    start = date(2026, 1, 20)
    s, e = next_partition(start, "m", 1)
    assert s == date(2026, 1, 20)
    assert e == date(2026, 2, 1)

    s2, e2 = next_partition(e, "m", 1)
    assert s2 == date(2026, 2, 1)
    assert e2 == date(2026, 3, 1)


def test_next_partition_month_leap_year():
    start = date(2024, 2, 1)
    s, e = next_partition(start, "m", 1)
    assert s == date(2024, 2, 1)
    assert e == date(2024, 3, 1)


def test_next_partition_month_multi():
    start = date(2026, 1, 1)
    s, e = next_partition(start, "m", 3)
    assert s == date(2026, 1, 1)
    assert e == date(2026, 4, 1)


def test_next_partition_year_boundary():
    start = date(2026, 12, 1)
    s, e = next_partition(start, "m", 1)
    assert s == date(2026, 12, 1)
    assert e == date(2027, 1, 1)


def test_design_example():
    start = date(2026, 1, 20)
    s1, e1 = next_partition(start, "m", 1)
    assert s1 == date(2026, 1, 20)
    assert e1 == date(2026, 2, 1)

    s2, e2 = next_partition(e1, "m", 1)
    assert s2 == date(2026, 2, 1)
    assert e2 == date(2026, 3, 1)
