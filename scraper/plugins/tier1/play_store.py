"""Google Play Store scraper using google-play-scraper."""

from __future__ import annotations

import logging
from typing import Iterator

from scraper.base import BaseScraper, ScraperConfig
from scraper.schema import FeedbackItem, make_feedback_id, now_iso
from scraper.utils.date_parser import normalize_date

logger = logging.getLogger(__name__)


class PlayStoreScraper(BaseScraper):
    SOURCE_ID = "play_store"
    TIER = "tier1"
    REQUIRES_KEYS: list[str] = []

    def scrape(self) -> Iterator[FeedbackItem]:
        try:
            from google_play_scraper import reviews, Sort
        except ImportError:
            logger.error("[play_store] google-play-scraper not installed")
            return

        app_id = self._param("app_id")
        if not app_id:
            logger.error("[play_store] app_id not configured")
            return

        lang = self._param("lang", "en")
        country = self._param("country", "us")
        count = self.max_items

        logger.info("[play_store] Fetching up to %d reviews for %s", count, app_id)

        try:
            self.rate_limiter.wait()
            result, _ = reviews(
                app_id,
                lang=lang,
                country=country,
                sort=Sort.NEWEST,
                count=count,
            )
        except Exception as exc:
            logger.error("[play_store] Failed to fetch reviews: %s", exc)
            return

        for r in result:
            try:
                body = (r.get("content") or "").strip()
                if not body:
                    continue

                rating_raw = r.get("score")
                rating = float(rating_raw) if rating_raw is not None else None

                item = FeedbackItem(
                    id=make_feedback_id(
                        self.SOURCE_ID,
                        r.get("reviewId"),
                        r.get("userName"),
                        body,
                    ),
                    source=self.SOURCE_ID,
                    product=self.config.product_name,
                    author=r.get("userName"),
                    rating=rating,
                    title=r.get("title"),
                    body=body,
                    date=normalize_date(r.get("at")),
                    url=f"https://play.google.com/store/apps/details?id={app_id}",
                    scraped_at=now_iso(),
                    helpful_votes=r.get("thumbsUpCount"),
                    language=lang,
                    tags=["play_store", "mobile"],
                    raw=r if self.config.debug else None,
                )
                yield item
            except Exception as exc:
                logger.warning("[play_store] Skipping malformed review: %s", exc)
