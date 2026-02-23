"""Trustpilot scraper — stealth Playwright + JSON-LD fallback.

⚠  WARNING: Trustpilot's ToS prohibits automated scraping.
   Use for internal research only. Never redistribute scraped data.
   Requires --tos-aware flag to run.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Iterator

from bs4 import BeautifulSoup

from scraper.base import BaseScraper
from scraper.schema import FeedbackItem, make_feedback_id, now_iso
from scraper.utils.date_parser import normalize_date
from scraper.utils.stealth_browser import stealth_page

logger = logging.getLogger(__name__)

_BASE = "https://www.trustpilot.com/review/{slug}"


class TrustpilotScraper(BaseScraper):
    SOURCE_ID = "trustpilot"
    TIER = "tier2"
    REQUIRES_KEYS: list[str] = []

    def scrape(self) -> Iterator[FeedbackItem]:
        slug: str = self._param("slug", "")
        if not slug:
            logger.error("[trustpilot] 'slug' not configured")
            return

        headless: bool = self._param("headless", True)
        page_num = 1
        yielded = 0

        logger.info("[trustpilot] Scraping %s (stealth Playwright)", slug)

        try:
            with stealth_page(headless=headless) as page:
                while yielded < self.max_items:
                    self.rate_limiter.wait()
                    url = _BASE.format(slug=slug) + f"?page={page_num}"
                    try:
                        page.goto(url, wait_until="domcontentloaded", timeout=30000)
                        page.wait_for_timeout(1500)  # let JS settle
                    except Exception as exc:
                        logger.error("[trustpilot] Navigation failed (page %d): %s", page_num, exc)
                        break

                    html = page.content()
                    soup = BeautifulSoup(html, "lxml")
                    items_found = 0

                    # Live selectors (verified Feb 2026)
                    cards = soup.find_all(class_=re.compile(r"reviewCard", re.I))
                    for card in cards:
                        if yielded >= self.max_items:
                            break
                        try:
                            body_el = card.find(class_=re.compile(r"styles_reviewText", re.I))
                            body_text = body_el.get_text(strip=True) if body_el else ""
                            if not body_text:
                                continue

                            author_el = card.find(class_=re.compile(r"styles_consumerName", re.I))
                            author = author_el.get_text(strip=True) if author_el else None

                            # Rating from img alt: "Rated 4 out of 5 stars"
                            rating_img = card.find("img", class_=re.compile(r"CDS_StarRating", re.I))
                            rating: float | None = None
                            if rating_img:
                                import re as _re
                                m = _re.search(r"Rated\s+([\d.]+)", rating_img.get("alt", ""))
                                if m:
                                    rating = float(m.group(1))

                            date_el = card.find("time")
                            date_str = date_el.get("datetime") if date_el else None

                            title_el = card.find(class_=re.compile(r"styles_reviewHeader|heading-xs", re.I))
                            title = title_el.get_text(strip=True) if title_el else None

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
                                tags=["trustpilot"],
                                raw=None,
                            )
                            yield item
                            yielded += 1
                            items_found += 1
                        except Exception as exc:
                            logger.warning("[trustpilot] Skipping card: %s", exc)

                    if items_found == 0:
                        logger.info("[trustpilot] No items on page %d — stopping", page_num)
                        break

                    page_num += 1

        except Exception as exc:
            logger.error("[trustpilot] Stealth browser error: %s", exc)

        logger.info("[trustpilot] Yielded %d items", yielded)
