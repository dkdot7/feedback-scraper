"""Apple App Store scraper using app-store-scraper."""

from __future__ import annotations

import logging
from typing import Iterator

from scraper.base import BaseScraper, ScraperConfig
from scraper.schema import FeedbackItem, make_feedback_id, now_iso
from scraper.utils.date_parser import normalize_date

logger = logging.getLogger(__name__)


class AppStoreScraper(BaseScraper):
    SOURCE_ID = "app_store"
    TIER = "tier1"
    REQUIRES_KEYS: list[str] = []

    def scrape(self) -> Iterator[FeedbackItem]:
        try:
            from app_store_scraper import AppStore
        except ImportError:
            logger.error("[app_store] app-store-scraper not installed")
            return

        app_name = self._param("app_name", "app")
        app_id = self._param("app_id")
        country = self._param("country", "us")

        if not app_id:
            logger.error("[app_store] app_id not configured")
            return

        logger.info("[app_store] Fetching up to %d reviews for %s", self.max_items, app_id)

        try:
            self.rate_limiter.wait()
            app = AppStore(country=country, app_name=app_name, app_id=app_id)
            app.review(how_many=self.max_items)
        except Exception as exc:
            logger.error("[app_store] Failed to fetch reviews: %s", exc)
            return

        for r in app.reviews:
            try:
                body = (r.get("review") or "").strip()
                if not body:
                    continue

                rating_raw = r.get("rating")
                rating = float(rating_raw) if rating_raw is not None else None

                item = FeedbackItem(
                    id=make_feedback_id(
                        self.SOURCE_ID,
                        None,
                        r.get("userName"),
                        body,
                    ),
                    source=self.SOURCE_ID,
                    product=self.config.product_name,
                    author=r.get("userName"),
                    rating=rating,
                    title=r.get("title"),
                    body=body,
                    date=normalize_date(r.get("date")),
                    url=f"https://apps.apple.com/{country}/app/{app_name}/id{app_id}",
                    scraped_at=now_iso(),
                    language=country,
                    tags=["app_store", "mobile", "ios"],
                    raw=r if self.config.debug else None,
                )
                yield item
            except Exception as exc:
                logger.warning("[app_store] Skipping malformed review: %s", exc)
