"""Normalize any date string or timestamp to 'YYYY-MM-DD'."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Optional


_FORMATS = [
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
    "%d %B %Y",          # e.g. "12 January 2024"
    "%B %d, %Y",         # e.g. "January 12, 2024"
    "%b %d, %Y",         # e.g. "Jan 12, 2024"
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%d-%m-%Y",
    "%B %Y",             # e.g. "January 2024" → first of month
    "%b %Y",             # e.g. "Jan 2024"
]


def normalize_date(value: Optional[str | int | float]) -> Optional[str]:
    """Return 'YYYY-MM-DD' or None if the input cannot be parsed.

    Accepts:
    - ISO 8601 strings
    - UNIX timestamps (int/float or numeric strings)
    - Common date format strings
    """
    if value is None:
        return None

    # Numeric timestamp (int/float) — detect milliseconds vs seconds
    if isinstance(value, (int, float)):
        ts = float(value)
        if ts > 1e10:   # milliseconds
            ts /= 1000
        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
        except (OSError, OverflowError, ValueError):
            return None

    value = str(value).strip()
    if not value:
        return None

    # Numeric string timestamp
    if re.fullmatch(r"\d{9,13}", value):
        ts = float(value)
        if ts > 1e10:   # milliseconds
            ts /= 1000
        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
        except (OSError, OverflowError, ValueError):
            return None

    # Already "YYYY-MM-DD"
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        return value

    # Try all format strings
    for fmt in _FORMATS:
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    # Fallback: try dateutil if available
    try:
        from dateutil import parser as du_parser  # type: ignore

        return du_parser.parse(value, fuzzy=True).strftime("%Y-%m-%d")
    except Exception:
        pass

    return None
