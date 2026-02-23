"""Write per-source JSON output files with freshness checking."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Sequence

from scraper.schema import FeedbackItem


def is_fresh(path: Path, freshness_hours: float) -> bool:
    """Return True if the file exists and was modified within `freshness_hours`."""
    if not path.exists():
        return False
    age_seconds = time.time() - path.stat().st_mtime
    return age_seconds < freshness_hours * 3600


def write_output(
    items: Sequence[FeedbackItem],
    output_dir: str,
    product_slug: str,
    source_id: str,
    strip_raw: bool = True,
) -> Path:
    """Serialize items to ``output/{product_slug}/{source_id}.json`` and return the path."""
    dest_dir = Path(output_dir) / product_slug
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"{source_id}.json"

    records = []
    for item in items:
        data = item.model_dump(mode="json")
        if strip_raw:
            data.pop("raw", None)
        records.append(data)

    dest.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
    return dest
