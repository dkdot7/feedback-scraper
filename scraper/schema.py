"""Unified FeedbackItem schema for all scrapers."""

from __future__ import annotations

from typing import Optional
from datetime import datetime, timezone

from pydantic import BaseModel, field_validator

from scraper.utils.hashing import make_id


class FeedbackItem(BaseModel):
    id: str
    source: str
    product: str
    author: Optional[str] = None
    rating: Optional[float] = None          # normalized to 0.0–5.0
    title: Optional[str] = None
    body: str
    date: Optional[str] = None              # "YYYY-MM-DD"
    url: Optional[str] = None
    scraped_at: str                          # ISO 8601 with Z
    helpful_votes: Optional[int] = None
    verified_purchase: Optional[bool] = None
    language: Optional[str] = None
    sentiment: Optional[str] = None         # reserved for future NLP
    tags: list[str] = []
    raw: Optional[dict] = None              # debug only

    @field_validator("rating")
    @classmethod
    def clamp_rating(cls, v: Optional[float]) -> Optional[float]:
        if v is None:
            return v
        return max(0.0, min(5.0, v))

    @field_validator("body")
    @classmethod
    def body_must_not_be_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("body must not be empty")
        return v.strip()

    model_config = {"extra": "forbid"}


def make_feedback_id(source: str, url: Optional[str], author: Optional[str], body: str) -> str:
    """Generate a stable SHA-256 ID for a feedback item."""
    if url:
        key = f"{source}::{url}"
    elif author:
        key = f"{source}::{author}::{body[:100]}"
    else:
        key = f"{source}::{body[:200]}"
    return make_id(key)


def now_iso() -> str:
    """Return current UTC time as ISO 8601 string with Z suffix."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
