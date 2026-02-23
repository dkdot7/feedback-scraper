"""Sitejabber scraper — stealth Playwright + CSS: article.review.

⚠  WARNING: Review scraping may conflict with Sitejabber ToS.
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

_BASE = "https://www.sitejabber.com/reviews/{slug}"


class SitejabberScraper(BaseScraper):
    SOURCE_ID = "sitejabber"
    TIER = "tier2"
    REQUIRES_KEYS: list[str] = []

    def scrape(self) -> Iterator[FeedbackItem]:
        slug: str = self._param("slug", "")
        if not slug:
            logger.error("[sitejabber] 'slug' not configured")
            return

        headless: bool = self._param("headless", True)
        page_num = 1
        yielded = 0

        logger.info("[sitejabber] Scraping %s (stealth Playwright)", slug)

        try:
            with stealth_page(headless=headless) as page:
                while yielded < self.max_items:
                    self.rate_limiter.wait()
                    url = _BASE.format(slug=slug) + f"?page={page_num}"
                    try:
                        page.goto(url, wait_until="domcontentloaded", timeout=30000)
                        page.wait_for_timeout(2000)
                    except Exception as exc:
                        logger.error("[sitejabber] Navigation failed (page %d): %s", page_num, exc)
                        break

                    html = page.content()
                    soup = BeautifulSoup(html, "lxml")
                    articles = soup.select("article.review, div.review-item")

                    if not articles:
                        logger.info("[sitejabber] No reviews on page %d — stopping", page_num)
                        break

                    for article in articles:
                        if yielded >= self.max_items:
                            break
                        try:
                            body_el = article.select_one(".review-content, .review-body, p.review-text")
                            body_text = body_el.get_text(separator=" ", strip=True) if body_el else ""
                            if not body_text:
                                continue

                            title_el = article.select_one(".review-title, h3")
                            title = title_el.get_text(strip=True) if title_el else None

                            rating_el = article.select_one("[data-rating], .rating")
                            rating: float | None = None
                            if rating_el:
                                for attr in ("data-rating", "data-score"):
                                    val = rating_el.get(attr)
                                    if val:
                                        try:
                                            rating = float(val)
                                        except ValueError:
                                            pass
                                        break

                            author_el = article.select_one(".reviewer-name, .author-name, .username")
                            author = author_el.get_text(strip=True) if author_el else None

                            date_el = article.select_one("time, .review-date")
                            date_str = (
                                date_el.get("datetime") or date_el.get_text(strip=True)
                                if date_el else None
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
                                tags=["sitejabber"],
                                raw=None,
                            )
                            yield item
                            yielded += 1
                        except Exception as exc:
                            logger.warning("[sitejabber] Skipping article: %s", exc)

                    page_num += 1

        except Exception as exc:
            logger.error("[sitejabber] Stealth browser error: %s", exc)

        logger.info("[sitejabber] Yielded %d items", yielded)
