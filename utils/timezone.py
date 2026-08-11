"""Helpers for converting between the practice's business-local wall clock and
true UTC instants. Mirrors winnonah/python/utils/timezone.py; kept as a
separate copy since this repo doesn't share code with winnonah, only its
config (via load_config()'s business_timezone field).
"""

from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo


def business_to_utc(naive_business_dt: datetime, business_timezone: str) -> datetime:
    """Localize a naive business-local wall-clock datetime to UTC.

    Raises if given a datetime that already carries tzinfo, since that means
    the caller has an already-aware value and doesn't need this conversion.
    """
    if naive_business_dt.tzinfo is not None:
        raise ValueError(
            "business_to_utc expects a naive datetime, got one with tzinfo "
            f"{naive_business_dt.tzinfo!r}"
        )
    return naive_business_dt.replace(tzinfo=ZoneInfo(business_timezone)).astimezone(UTC)


def business_date_to_utc(business_date: date, business_timezone: str) -> datetime:
    """Convert a business-local calendar date (midnight) to its UTC instant."""
    return business_to_utc(
        datetime.combine(business_date, datetime.min.time()), business_timezone
    )


def now_utc() -> datetime:
    """The current instant, as a genuine UTC-aware datetime."""
    return datetime.now(UTC)


def now_business(business_timezone: str) -> datetime:
    """The current instant, as business-local wall-clock time."""
    return datetime.now(ZoneInfo(business_timezone))
