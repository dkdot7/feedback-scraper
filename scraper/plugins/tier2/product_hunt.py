"""Product Hunt scraper — stealth Playwright + __NEXT_DATA__ JSON.

⚠  ToS is ambiguous — use for internal research only.
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

_BASE = "https://www.producthunt.com/products/{slug}/reviews"


class ProductHuntScraper(BaseScraper):
    SOURCE_ID = "product_hunt"
    TIER = "tier2"
    REQUIRES_KEYS: list[str] = []

    def scrape(self) -> Iterator[FeedbackItem]:
        slug: str = self._param("slug", "")
        if not slug:
            logger.error("[product_hunt] 'slug' not configured")
            return

        headless: bool = self._param("headless", True)
        page_num = 1
        yielded = 0

        logger.info("[product_hunt] Scraping %s (stealth Playwright)", slug)

        try:
            with stealth_page(headless=headless) as page:
                while yielded < self.max_items:
                    self.rate_limiter.wait()
                    url = _BASE.format(slug=slug) + f"?page={page_num}"
                    try:
                        page.goto(url, wait_until="domcontentloaded", timeout=30000)
                        page.wait_for_timeout(2000)
                    except Exception as exc:
                        logger.error("[product_hunt] Navigation failed (page %d): %s", page_num, exc)
                        break

                    html = page.content()
                    soup = BeautifulSoup(html, "lxml")
                    items_found = 0

                    # Try __NEXT_DATA__
                    next_data_tag = soup.find("script", id="__NEXT_DATA__")
                    if next_data_tag:
                        try:
                            next_data = json.loads(next_data_tag.string or "{}")
                            reviews = _extract_reviews(next_data)
                            for r in reviews:
                                if yielded >= self.max_items:
                                    break
                                body_text = (r.get("body") or r.get("comment") or "").strip()
                                if not body_text:
                                    continue

                                rating_raw = r.get("rating") or r.get("score")
                                try:
                                    rating = float(rating_raw) if rating_raw is not None else None
                                except (TypeError, ValueError):
                                    rating = None

                                user = r.get("user") or {}
                                author = user.get("name") or user.get("username") if isinstance(user, dict) else None

                                item = FeedbackItem(
                                    id=make_feedback_id(self.SOURCE_ID, None, author, body_text),
                                    source=self.SOURCE_ID,
                                    product=self.config.product_name,
                                    author=author,
                                    rating=rating,
                                    title=r.get("title"),
                                    body=body_text,
                                    date=normalize_date(r.get("createdAt") or r.get("created_at")),
                                    url=url,
                                    scraped_at=now_iso(),
                                    helpful_votes=r.get("votesCount"),
                                    tags=["product_hunt"],
                                    raw=r if self.config.debug else None,
                                )
                                yield item
                                yielded += 1
                                items_found += 1
                        except Exception as exc:
                            logger.debug("[product_hunt] __NEXT_DATA__ parse error: %s", exc)

                    # CSS fallback
                    if items_found == 0:
                        cards = soup.select("div.review-card, section.review, [data-test='review']")
                        for card in cards:
                            if yielded >= self.max_items:
                                break
                            try:
                                body_el = card.select_one("p, .review-body")
                                body_text = body_el.get_text(strip=True) if body_el else ""
                                if not body_text:
                                    continue

                                author_el = card.select_one(".username, .author-name")
                                author = author_el.get_text(strip=True) if author_el else None

                                date_el = card.select_one("time")
                                date_str = date_el.get("datetime") if date_el else None

                                item = FeedbackItem(
                                    id=make_feedback_id(self.SOURCE_ID, None, author, body_text),
                                    source=self.SOURCE_ID,
                                    product=self.config.product_name,
                                    author=author,
                                    body=body_text,
                                    date=normalize_date(date_str),
                                    url=url,
                                    scraped_at=now_iso(),
                                    tags=["product_hunt"],
                                    raw=None,
                                )
                                yield item
                                yielded += 1
                                items_found += 1
                            except Exception as exc:
                                logger.warning("[product_hunt] Skipping CSS card: %s", exc)

                    if items_found == 0:
                        logger.info("[product_hunt] No items on page %d — stopping", page_num)
                        break

                    page_num += 1

        except Exception as exc:
            logger.error("[product_hunt] Stealth browser error: %s", exc)

        logger.info("[product_hunt] Yielded %d items", yielded)


def _extract_reviews(data, depth: int = 0) -> list[dict]:
    if depth > 8:
        return []
    if isinstance(data, dict):
        for key in ("reviews", "reviewsList", "productReviews"):
            if key in data:
                val = data[key]
                if isinstance(val, list) and val and isinstance(val[0], dict):
                    return val
                return _extract_reviews(val, depth + 1)
        results = []
        for v in data.values():
            results.extend(_extract_reviews(v, depth + 1))
        return results
    if isinstance(data, list):
        if data and isinstance(data[0], dict) and "body" in data[0]:
            return data
        results = []
        for item in data:
            results.extend(_extract_reviews(item, depth + 1))
        return results
    return []
