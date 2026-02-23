"""SHA-256 ID generation for FeedbackItem deduplication."""

import hashlib


def make_id(key: str) -> str:
    """Return a hex SHA-256 digest of the given key string."""
    return hashlib.sha256(key.encode("utf-8")).hexdigest()
