"""GetApp scraper — same Gartner platform as Capterra.

⚠  WARNING: GetApp's ToS prohibits automated scraping.
   Use for internal research only. Never redistribute scraped data.
   Requires --tos-aware flag to run.
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

_BASE = "https://www.getapp.com/content-management-software/a/{slug}/reviews/"


class GetAppScraper(BaseScraper):
    SOURCE_ID = "getapp"
    TIER = "tier2"
    REQUIRES_KEYS: list[str] = []

    def scrape(self) -> Iterator[FeedbackItem]:
        slug: str = self._param("slug", "")
        if not slug:
            logger.error("[getapp] 'slug' not configured")
            return

        session = make_session()
        page = 1
        yielded = 0

        logger.info("[getapp] Scraping %s", slug)

        while yielded < self.max_items:
            self.rate_limiter.wait()
            url = _BASE.format(slug=slug) + f"?page={page}"
            try:
                resp = session.get(url)
                if resp.status_code == 429:
                    logger.warning("[getapp] 429 — stopping")
                    break
                resp.raise_for_status()
            except Exception as exc:
                logger.error("[getapp] Request failed (page %d): %s", page, exc)
                break

            soup = BeautifulSoup(resp.text, "lxml")
            # GetApp uses same Gartner selectors as Capterra
            cards = soup.select('div[data-testid="review-card"]')
            if not cards:
                cards = soup.select("div.review-card, article.review")

            if not cards:
                logger.info("[getapp] No cards on page %d — stopping", page)
                break

            for card in cards:
                if yielded >= self.max_items:
                    break
                try:
                    body_el = card.select_one('[data-testid="review-body"], .review-body')
                    body_text = body_el.get_text(separator=" ", strip=True) if body_el else ""
                    if not body_text:
                        pros = card.select_one('[data-testid="pros"], .pros')
                        cons = card.select_one('[data-testid="cons"], .cons')
                        parts = [p.get_text(strip=True) for p in [pros, cons] if p]
                        body_text = " | ".join(parts)
                    if not body_text:
                        continue

                    title_el = card.select_one('[data-testid="review-title"], .review-title')
                    title = title_el.get_text(strip=True) if title_el else None

                    rating_el = card.select_one('[data-rating]')
                    rating: float | None = None
                    if rating_el:
                        try:
                            rating = float(rating_el.get("data-rating", ""))
                        except (ValueError, TypeError):
                            pass

                    author_el = card.select_one('[data-testid="reviewer-name"], .reviewer-name')
                    author = author_el.get_text(strip=True) if author_el else None

                    date_el = card.select_one("time")
                    date_str = date_el.get("datetime") if date_el else None

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
                        tags=["getapp"],
                        raw=None,
                    )
                    yield item
                    yielded += 1
                except Exception as exc:
                    logger.warning("[getapp] Skipping card: %s", exc)

            page += 1

        logger.info("[getapp] Yielded %d items", yielded)
