"""Amazon product review scraper — CSS: div[data-hook="review"].

⚠  WARNING: Amazon ToS explicitly prohibits scraping. High bot-detection risk.
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

_REVIEWS_URL = "https://www.amazon.com/product-reviews/{asin}"


class AmazonScraper(BaseScraper):
    SOURCE_ID = "amazon"
    TIER = "tier2"
    REQUIRES_KEYS: list[str] = []

    def scrape(self) -> Iterator[FeedbackItem]:
        asin: str = self._param("asin", "")
        if not asin:
            logger.error("[amazon] 'asin' not configured")
            return

        session = make_session()
        # Amazon requires a more browser-like Accept header
        session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        })

        page = 1
        yielded = 0

        logger.info("[amazon] Scraping ASIN %s (high bot risk)", asin)

        while yielded < self.max_items:
            self.rate_limiter.wait()
            url = _REVIEWS_URL.format(asin=asin)
            try:
                resp = session.get(
                    url,
                    params={
                        "ie": "UTF8",
                        "reviewerType": "all_reviews",
                        "pageNumber": page,
                        "sortBy": "recent",
                    },
                )
                if resp.status_code == 429:
                    logger.warning("[amazon] 429 — stopping")
                    break
                if resp.status_code == 503 or "robot" in resp.text.lower()[:500]:
                    logger.warning("[amazon] Bot check triggered — stopping")
                    break
                resp.raise_for_status()
            except Exception as exc:
                logger.error("[amazon] Request failed (page %d): %s", page, exc)
                break

            soup = BeautifulSoup(resp.text, "lxml")
            reviews = soup.select('div[data-hook="review"]')

            if not reviews:
                logger.info("[amazon] No reviews on page %d — stopping", page)
                break

            for review in reviews:
                if yielded >= self.max_items:
                    break
                try:
                    body_el = review.select_one('[data-hook="review-body"] span')
                    body_text = body_el.get_text(separator=" ", strip=True) if body_el else ""
                    if not body_text:
                        continue

                    title_el = review.select_one('[data-hook="review-title"] span:not([class])')
                    title = title_el.get_text(strip=True) if title_el else None

                    rating_el = review.select_one('[data-hook="review-star-rating"] span.a-icon-alt')
                    rating: float | None = None
                    if rating_el:
                        # Format: "4.0 out of 5 stars"
                        import re
                        m = re.search(r"([\d.]+)\s+out of", rating_el.get_text())
                        if m:
                            rating = float(m.group(1))

                    author_el = review.select_one(".a-profile-name")
                    author = author_el.get_text(strip=True) if author_el else None

                    date_el = review.select_one('[data-hook="review-date"]')
                    date_text = date_el.get_text(strip=True) if date_el else None
                    # "Reviewed in the United States on January 15, 2024"
                    if date_text:
                        import re
                        m = re.search(r"on (.+)$", date_text)
                        if m:
                            date_text = m.group(1)

                    helpful_el = review.select_one('[data-hook="helpful-vote-statement"]')
                    helpful: int | None = None
                    if helpful_el:
                        import re
                        m = re.search(r"(\d+)", helpful_el.get_text())
                        if m:
                            helpful = int(m.group(1))

                    verified_el = review.select_one('[data-hook="avp-badge"]')
                    verified = verified_el is not None

                    item = FeedbackItem(
                        id=make_feedback_id(self.SOURCE_ID, None, author, body_text),
                        source=self.SOURCE_ID,
                        product=self.config.product_name,
                        author=author,
                        rating=rating,
                        title=title,
                        body=body_text,
                        date=normalize_date(date_text),
                        url=f"https://www.amazon.com/dp/{asin}",
                        scraped_at=now_iso(),
                        helpful_votes=helpful,
                        verified_purchase=verified,
                        language="en",
                        tags=["amazon"],
                        raw=None,
                    )
                    yield item
                    yielded += 1
                except Exception as exc:
                    logger.warning("[amazon] Skipping review: %s", exc)

            page += 1

        logger.info("[amazon] Yielded %d items", yielded)
