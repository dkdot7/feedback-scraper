"""Abstract base class for all scrapers."""

from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Iterator

from scraper.schema import FeedbackItem
from scraper.utils.rate_limiter import SimpleDelayLimiter, make_rate_limiter

logger = logging.getLogger(__name__)


class ConfigError(ValueError):
    """Raised when a scraper's configuration is invalid or missing required keys."""


@dataclass
class ScraperConfig:
    """Holds the merged per-source + global configuration for a single scraper."""

    product_name: str
    product_slug: str
    source_params: dict
    output_dir: str = "output"
    max_items: int = 200
    freshness_hours: float = 24.0
    rate_limit_delay: float = 1.0
    rate_limit_jitter: float = 0.3
    debug: bool = False
    env: dict = field(default_factory=dict)

    @classmethod
    def from_raw(
        cls,
        product_name: str,
        product_slug: str,
        source_params: dict,
        global_cfg: dict,
        env: dict | None = None,
    ) -> "ScraperConfig":
        return cls(
            product_name=product_name,
            product_slug=product_slug,
            source_params=source_params,
            output_dir=global_cfg.get("output_dir", "output"),
            max_items=source_params.get(
                "max_items", global_cfg.get("default_max_items", 200)
            ),
            freshness_hours=source_params.get(
                "freshness_hours", global_cfg.get("default_freshness_hours", 24.0)
            ),
            rate_limit_delay=source_params.get(
                "rate_limit_delay",
                global_cfg.get("default_rate_limit_delay", 1.0),
            ),
            debug=global_cfg.get("debug", False),
            env=env or dict(os.environ),
        )


class BaseScraper(ABC):
    """Abstract base for all source scrapers.

    Subclasses must define:
        SOURCE_ID  – snake_case identifier used as the output filename
        TIER       – "tier1" | "tier2" | "tier3" | "optional"
        REQUIRES_KEYS – list of environment variable names needed

    They must implement:
        scrape() – yield FeedbackItem, never raise
    """

    SOURCE_ID: str = ""
    TIER: str = ""
    REQUIRES_KEYS: list[str] = []

    def __init__(self, config: ScraperConfig) -> None:
        self.config = config
        self.logger = logging.getLogger(f"scraper.{self.SOURCE_ID}")
        self.rate_limiter: SimpleDelayLimiter = make_rate_limiter(  # type: ignore[assignment]
            delay=config.rate_limit_delay,
            jitter=config.rate_limit_jitter,
        )

    # ── Subclass interface ────────────────────────────────────────────────────

    def validate_config(self) -> None:
        """Raise ConfigError if required env keys or source params are missing."""
        missing = [k for k in self.REQUIRES_KEYS if not self.config.env.get(k)]
        if missing:
            raise ConfigError(
                f"[{self.SOURCE_ID}] Missing required environment variables: "
                + ", ".join(missing)
            )

    @abstractmethod
    def scrape(self) -> Iterator[FeedbackItem]:
        """Yield FeedbackItem objects.  Must not raise — catch internally."""
        ...

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _get_env(self, key: str, default: str | None = None) -> str | None:
        return self.config.env.get(key, default)

    def _param(self, key: str, default=None):
        return self.config.source_params.get(key, default)

    @property
    def max_items(self) -> int:
        return self.config.max_items
