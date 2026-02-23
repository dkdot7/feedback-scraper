"""MouthShut scraper — CSS: div.review-article.

⚠  ToS is ambiguous for automated scraping.
   Use for internal research only. Requires --tos-aware flag to run.
"""

from __future__ import annotations

import logging
from typing import Iterator

from bs4 import BeautifulSoup

from scraper.base import BaseScraper
from scraper.schema import FeedbackItem, make_feedback_id, now_iso
from scraper.utils.date_parser import normalize_date
from scraper.utils.http_client import make_session

logger = logging.getLogger(__name__)


class MouthShutScraper(BaseScraper):
    SOURCE_ID = "mouthshut"
    TIER = "tier2"
    REQUIRES_KEYS: list[str] = []

    def scrape(self) -> Iterator[FeedbackItem]:
        base_url: str = self._param("url", "")
        if not base_url:
            logger.error("[mouthshut] 'url' not configured")
            return

        session = make_session()
        page = 1
        yielded = 0

        logger.info("[mouthshut] Scraping %s", base_url)

        while yielded < self.max_items:
            self.rate_limiter.wait()
            url = base_url if page == 1 else f"{base_url}?page={page}"
            try:
                resp = session.get(url)
                if resp.status_code == 429:
                    logger.warning("[mouthshut] 429 — stopping")
                    break
                resp.raise_for_status()
            except Exception as exc:
                logger.error("[mouthshut] Request failed (page %d): %s", page, exc)
                break

            soup = BeautifulSoup(resp.text, "lxml")
            articles = soup.select("div.review-article, div.reviewBox, div.review-cnt")

            if not articles:
                logger.info("[mouthshut] No reviews on page %d — stopping", page)
                break

            for article in articles:
                if yielded >= self.max_items:
                    break
                try:
                    body_el = article.select_one(
                        ".review-desc, .review-body, p.review-text, .reviewtxt"
                    )
                    body_text = body_el.get_text(separator=" ", strip=True) if body_el else ""
                    if not body_text:
                        continue

                    title_el = article.select_one(".review-title, h2, h3")
                    title = title_el.get_text(strip=True) if title_el else None

                    rating_el = article.select_one("[class*='rating'], [data-rating]")
                    rating: float | None = None
                    if rating_el:
                        # Try data attribute first
                        for attr in ("data-rating", "data-score"):
                            val = rating_el.get(attr)
                            if val:
                                try:
                                    rating = float(val)
                                except ValueError:
                                    pass
                                break
                        # Fallback: count filled stars
                        if rating is None:
                            filled = len(article.select(".star-full, .star-filled, .icon-star"))
                            if filled:
                                rating = float(filled)

                    author_el = article.select_one(".username, .reviewer-name, .author-name")
                    author = author_el.get_text(strip=True) if author_el else None

                    date_el = article.select_one("time, .review-date, .post-date")
                    date_str = (
                        date_el.get("datetime") or date_el.get_text(strip=True)
                        if date_el
                        else None
                    )

                    item = FeedbackItem(
                        id=make_feedback_id(self.SOURCE_ID, None, author, body_text),
                        source=self.SOURCE_ID,
                        product=self.config.product_name,
                        author=author,
                        rating=rating,
                        title=title,
                        body=body_text,
                        date=normalize_date(date_str),
                        url=url,
                        scraped_at=now_iso(),
                        tags=["mouthshut"],
                        raw=None,
                    )
                    yield item
                    yielded += 1
                except Exception as exc:
                    logger.warning("[mouthshut] Skipping article: %s", exc)

            page += 1

        logger.info("[mouthshut] Yielded %d items", yielded)
