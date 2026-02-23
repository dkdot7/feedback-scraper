"""GetApp scraper — stealth Playwright (same Gartner platform as Capterra).

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
from scraper.utils.stealth_browser import stealth_page

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

        headless: bool = self._param("headless", True)
        page_num = 1
        yielded = 0

        logger.info("[getapp] Scraping %s (stealth Playwright)", slug)

        try:
            with stealth_page(headless=headless) as page:
                while yielded < self.max_items:
                    self.rate_limiter.wait()
                    url = _BASE.format(slug=slug) + f"?page={page_num}"
                    try:
                        page.goto(url, wait_until="domcontentloaded", timeout=30000)
                        page.wait_for_timeout(2000)
                    except Exception as exc:
                        logger.error("[getapp] Navigation failed (page %d): %s", page_num, exc)
                        break

                    html = page.content()
                    soup = BeautifulSoup(html, "lxml")
                    cards = soup.select('div[data-testid="review-card"]')
                    if not cards:
                        cards = soup.select("div.review-card, article.review")

                    if not cards:
                        logger.info("[getapp] No cards on page %d — stopping", page_num)
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
                            try:
                                rating = float(rating_el.get("data-rating")) if rating_el else None
                            except (TypeError, ValueError):
                                rating = None

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

                    page_num += 1

        except Exception as exc:
            logger.error("[getapp] Stealth browser error: %s", exc)

        logger.info("[getapp] Yielded %d items", yielded)
