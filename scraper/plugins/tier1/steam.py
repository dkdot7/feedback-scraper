"""Steam Store review scraper using the free Steam API."""

from __future__ import annotations

import logging
from typing import Iterator

from scraper.base import BaseScraper
from scraper.schema import FeedbackItem, make_feedback_id, now_iso
from scraper.utils.date_parser import normalize_date
from scraper.utils.http_client import make_session

logger = logging.getLogger(__name__)

_REVIEW_URL = "https://store.steampowered.com/appreviews/{app_id}"


class SteamScraper(BaseScraper):
    SOURCE_ID = "steam"
    TIER = "tier1"
    REQUIRES_KEYS: list[str] = []

    def scrape(self) -> Iterator[FeedbackItem]:
        app_id: str = self._param("app_id", "")
        if not app_id or app_id == "0":
            logger.error("[steam] app_id not configured or set to 0")
            return

        language: str = self._param("language", "english")
        review_type: str = self._param("review_type", "all")
        session = make_session()
        cursor = "*"
        yielded = 0

        logger.info("[steam] Fetching reviews for app %s", app_id)

        while yielded < self.max_items:
            self.rate_limiter.wait()
            try:
                resp = session.get(
                    _REVIEW_URL.format(app_id=app_id),
                    params={
                        "json": 1,
                        "language": language,
                        "review_type": review_type,
                        "purchase_type": "all",
                        "num_per_page": min(100, self.max_items - yielded),
                        "cursor": cursor,
                        "filter": "recent",
                    },
                )
                resp.raise_for_status()
                data = resp.json()
            except Exception as exc:
                logger.error("[steam] Request failed: %s", exc)
                break

            if data.get("success") != 1:
                logger.warning("[steam] API returned non-success: %s", data)
                break

            reviews = data.get("reviews", [])
            if not reviews:
                break

            for r in reviews:
                if yielded >= self.max_items:
                    break
                try:
                    body = (r.get("review") or "").strip()
                    if not body:
                        continue

                    # Steam uses thumbs up/down — normalize to 5.0 / 1.0
                    voted_up = r.get("voted_up", None)
                    rating: float | None = None
                    if voted_up is True:
                        rating = 5.0
                    elif voted_up is False:
                        rating = 1.0

                    steam_id = r.get("recommendationid", "")
                    url = f"https://store.steampowered.com/app/{app_id}/#app_reviews_hash"

                    item = FeedbackItem(
                        id=make_feedback_id(
                            self.SOURCE_ID,
                            f"steam::{steam_id}",
                            str(r.get("author", {}).get("steamid", "")),
                            body,
                        ),
                        source=self.SOURCE_ID,
                        product=self.config.product_name,
                        author=str(r.get("author", {}).get("steamid", "")),
                        rating=rating,
                        body=body,
                        date=normalize_date(r.get("timestamp_created")),
                        url=url,
                        scraped_at=now_iso(),
                        helpful_votes=r.get("votes_up"),
                        verified_purchase=r.get("steam_purchase", False),
                        language=language,
                        tags=["steam", "game_review"],
                        raw=r if self.config.debug else None,
                    )
                    yield item
                    yielded += 1
                except Exception as exc:
                    logger.warning("[steam] Skipping review: %s", exc)

            # Pagination
            new_cursor = data.get("cursor", "")
            if not new_cursor or new_cursor == cursor:
                break
            cursor = new_cursor

        logger.info("[steam] Yielded %d items", yielded)
