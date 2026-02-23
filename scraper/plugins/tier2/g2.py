"""G2 scraper — stealth Playwright + __NEXT_DATA__ JSON fallback.

⚠  WARNING: G2's ToS prohibits automated scraping.
   Use for internal research only. Never redistribute scraped data.
   Requires --tos-aware flag to run.
"""

from __future__ import annotations

import json
import logging
from typing import Iterator

from bs4 import BeautifulSoup

from scraper.base import BaseScraper
from scraper.schema import FeedbackItem, make_feedback_id, now_iso
from scraper.utils.date_parser import normalize_date
from scraper.utils.stealth_browser import stealth_page

logger = logging.getLogger(__name__)

_BASE = "https://www.g2.com/products/{slug}/reviews"


class G2Scraper(BaseScraper):
    SOURCE_ID = "g2"
    TIER = "tier2"
    REQUIRES_KEYS: list[str] = []

    def scrape(self) -> Iterator[FeedbackItem]:
        slug: str = self._param("slug", "")
        if not slug:
            logger.error("[g2] 'slug' not configured")
            return

        headless: bool = self._param("headless", True)
        page_num = 1
        yielded = 0

        logger.info("[g2] Scraping %s (stealth Playwright)", slug)

        try:
            with stealth_page(headless=headless) as page:
                while yielded < self.max_items:
                    self.rate_limiter.wait()
                    url = _BASE.format(slug=slug) + f"?page={page_num}"
                    try:
                        page.goto(url, wait_until="domcontentloaded", timeout=30000)
                        page.wait_for_timeout(2000)
                    except Exception as exc:
                        logger.error("[g2] Navigation failed (page %d): %s", page_num, exc)
                        break

                    html = page.content()
                    soup = BeautifulSoup(html, "lxml")
                    items_found = 0

                    # Try __NEXT_DATA__
                    next_data_tag = soup.find("script", id="__NEXT_DATA__")
                    if next_data_tag:
                        try:
                            next_data = json.loads(next_data_tag.string or "{}")
                            reviews = _find_reviews_in_next_data(next_data)
                            for r in reviews:
                                if yielded >= self.max_items:
                                    break
                                body_text = (r.get("body") or r.get("comment") or r.get("pros") or "").strip()
                                if not body_text:
                                    continue

                                rating_raw = r.get("star_rating") or r.get("rating")
                                try:
                                    rating = float(rating_raw) if rating_raw else None
                                except (TypeError, ValueError):
                                    rating = None

                                item = FeedbackItem(
                                    id=make_feedback_id(
                                        self.SOURCE_ID, None,
                                        r.get("reviewer_name") or r.get("author"),
                                        body_text,
                                    ),
                                    source=self.SOURCE_ID,
                                    product=self.config.product_name,
                                    author=r.get("reviewer_name") or r.get("author"),
                                    rating=rating,
                                    title=r.get("title"),
                                    body=body_text,
                                    date=normalize_date(r.get("submitted_at") or r.get("date")),
                                    url=url,
                                    scraped_at=now_iso(),
                                    tags=["g2"],
                                    raw=r if self.config.debug else None,
                                )
                                yield item
                                yielded += 1
                                items_found += 1
                        except Exception as exc:
                            logger.debug("[g2] __NEXT_DATA__ parse error: %s", exc)

                    # CSS fallback
                    if items_found == 0:
                        cards = soup.select("div.paper--box")
                        for card in cards:
                            if yielded >= self.max_items:
                                break
                            try:
                                body_el = card.select_one(".formatted-text")
                                body_text = body_el.get_text(separator=" ", strip=True) if body_el else ""
                                if not body_text:
                                    continue

                                title_el = card.select_one(".review-title")
                                title = title_el.get_text(strip=True) if title_el else None

                                rating_el = card.select_one("[data-rating]")
                                try:
                                    rating = float(rating_el.get("data-rating")) if rating_el else None
                                except (TypeError, ValueError):
                                    rating = None

                                author_el = card.select_one(".reviewer-name")
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
                                    tags=["g2"],
                                    raw=None,
                                )
                                yield item
                                yielded += 1
                                items_found += 1
                            except Exception as exc:
                                logger.warning("[g2] Skipping card: %s", exc)

                    if items_found == 0:
                        logger.info("[g2] No items on page %d — stopping", page_num)
                        break

                    page_num += 1

        except Exception as exc:
            logger.error("[g2] Stealth browser error: %s", exc)

        logger.info("[g2] Yielded %d items", yielded)


def _find_reviews_in_next_data(data, depth: int = 0) -> list[dict]:
    if depth > 10:
        return []
    if isinstance(data, list):
        if data and isinstance(data[0], dict) and (
            "body" in data[0] or "star_rating" in data[0] or "reviewer_name" in data[0]
        ):
            return data
        results = []
        for item in data:
            results.extend(_find_reviews_in_next_data(item, depth + 1))
        return results
    if isinstance(data, dict):
        for key in ("reviews", "reviewList", "review_data"):
            if key in data:
                return _find_reviews_in_next_data(data[key], depth + 1)
        results = []
        for v in data.values():
            results.extend(_find_reviews_in_next_data(v, depth + 1))
        return results
    return []
