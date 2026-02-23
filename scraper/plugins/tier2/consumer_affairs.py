"""ConsumerAffairs scraper — CSS: div.rvw-cnt.

⚠  WARNING: ConsumerAffairs ToS explicitly prohibits scraping.
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

_BASE = "https://www.consumeraffairs.com/software/{slug}.html"


class ConsumerAffairsScraper(BaseScraper):
    SOURCE_ID = "consumer_affairs"
    TIER = "tier2"
    REQUIRES_KEYS: list[str] = []

    def scrape(self) -> Iterator[FeedbackItem]:
        slug: str = self._param("slug", "")
        if not slug:
            logger.error("[consumer_affairs] 'slug' not configured")
            return

        session = make_session()
        page = 1
        yielded = 0

        logger.info("[consumer_affairs] Scraping %s", slug)

        while yielded < self.max_items:
            self.rate_limiter.wait()
            url = _BASE.format(slug=slug)
            params = {"page": page} if page > 1 else {}
            try:
                resp = session.get(url, params=params)
                if resp.status_code == 429:
                    logger.warning("[consumer_affairs] 429 — stopping")
                    break
                resp.raise_for_status()
            except Exception as exc:
                logger.error("[consumer_affairs] Request failed (page %d): %s", page, exc)
                break

            soup = BeautifulSoup(resp.text, "lxml")
            containers = soup.select("div.rvw-cnt, div.review-container, article.review")

            if not containers:
                logger.info("[consumer_affairs] No reviews on page %d — stopping", page)
                break

            for container in containers:
                if yielded >= self.max_items:
                    break
                try:
                    body_el = container.select_one(".rvw-body, .review-body, p")
                    body_text = body_el.get_text(separator=" ", strip=True) if body_el else ""
                    if not body_text:
                        continue

                    title_el = container.select_one(".rvw-title, .review-title, h3")
                    title = title_el.get_text(strip=True) if title_el else None

                    rating_el = container.select_one("[data-rating], .rating-stars")
                    rating: float | None = None
                    if rating_el:
                        val = rating_el.get("data-rating") or rating_el.get("data-score")
                        if val:
                            try:
                                rating = float(val)
                            except ValueError:
                                pass

                    author_el = container.select_one(".rvw-author, .reviewer-name, .author")
                    author = author_el.get_text(strip=True) if author_el else None

                    date_el = container.select_one("time, .rvw-date, .review-date")
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
                        tags=["consumer_affairs"],
                        raw=None,
                    )
                    yield item
                    yielded += 1
                except Exception as exc:
                    logger.warning("[consumer_affairs] Skipping item: %s", exc)

            page += 1

        logger.info("[consumer_affairs] Yielded %d items", yielded)
