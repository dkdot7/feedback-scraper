"""Flipkart review scraper — 2-step: search for PID, then fetch reviews.

⚠  WARNING: Flipkart ToS prohibits scraping.
   Use for internal research only. Never redistribute scraped data.
   Requires --tos-aware flag to run.
"""

from __future__ import annotations

import logging
import re
from typing import Iterator

from bs4 import BeautifulSoup

from scraper.base import BaseScraper
from scraper.schema import FeedbackItem, make_feedback_id, now_iso
from scraper.utils.date_parser import normalize_date
from scraper.utils.http_client import make_session

logger = logging.getLogger(__name__)

_SEARCH_URL = "https://www.flipkart.com/search"
_REVIEW_URL_TMPL = "https://www.flipkart.com/{slug}/product-reviews/{pid}"


class FlipkartScraper(BaseScraper):
    SOURCE_ID = "flipkart"
    TIER = "tier2"
    REQUIRES_KEYS: list[str] = []

    def scrape(self) -> Iterator[FeedbackItem]:
        search_query: str = self._param("search_query", self.config.product_name)
        session = make_session()
        session.headers.update({
            "Accept-Language": "en-IN,en;q=0.9",
        })

        logger.info("[flipkart] Step 1: searching for '%s'", search_query)

        # Step 1: Find product PID and slug
        self.rate_limiter.wait()
        try:
            resp = session.get(
                _SEARCH_URL,
                params={"q": search_query, "marketplace": "FLIPKART"},
            )
            if resp.status_code == 429:
                logger.warning("[flipkart] 429 on search — stopping")
                return
            resp.raise_for_status()
        except Exception as exc:
            logger.error("[flipkart] Search request failed: %s", exc)
            return

        soup = BeautifulSoup(resp.text, "lxml")
        product_links = soup.select("a._1fQZEK, a.s1Q9rs, a._2rpwqI")

        if not product_links:
            logger.warning("[flipkart] No product links found in search results")
            return

        # Extract PID from first matching product href
        first_href = product_links[0].get("href", "")
        pid_match = re.search(r"/p/(itm[a-zA-Z0-9]+)", first_href)
        if not pid_match:
            logger.warning("[flipkart] Could not extract PID from %s", first_href)
            return

        pid = pid_match.group(1)
        slug_match = re.search(r"/([\w-]+)/p/", first_href)
        slug = slug_match.group(1) if slug_match else "product"

        logger.info("[flipkart] Step 2: fetching reviews for PID=%s", pid)

        page = 1
        yielded = 0

        while yielded < self.max_items:
            self.rate_limiter.wait()
            review_url = _REVIEW_URL_TMPL.format(slug=slug, pid=pid) + f"?page={page}"
            try:
                resp = session.get(review_url)
                if resp.status_code == 429:
                    logger.warning("[flipkart] 429 on reviews — stopping")
                    break
                resp.raise_for_status()
            except Exception as exc:
                logger.error("[flipkart] Review request failed (page %d): %s", page, exc)
                break

            soup = BeautifulSoup(resp.text, "lxml")
            containers = soup.select("div._16PBlm, div.review-container, div._27M-vq")

            if not containers:
                logger.info("[flipkart] No reviews on page %d — stopping", page)
                break

            for container in containers:
                if yielded >= self.max_items:
                    break
                try:
                    body_el = container.select_one("div.t-ZTKy, .review-text, p._2-N8zT")
                    body_text = body_el.get_text(separator=" ", strip=True) if body_el else ""
                    if not body_text:
                        continue

                    title_el = container.select_one("p._2-N8zT, .review-title")
                    title = title_el.get_text(strip=True) if title_el else None

                    rating_el = container.select_one("div._3LWZlK, [data-rating]")
                    rating: float | None = None
                    if rating_el:
                        try:
                            rating = float(rating_el.get_text(strip=True))
                        except ValueError:
                            pass

                    author_el = container.select_one("p._2sc7ZR, .reviewer-name")
                    author = author_el.get_text(strip=True) if author_el else None

                    date_el = container.select_one("p._2sc7ZR+p, time, .review-date")
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
                        url=review_url,
                        scraped_at=now_iso(),
                        tags=["flipkart"],
                        raw=None,
                    )
                    yield item
                    yielded += 1
                except Exception as exc:
                    logger.warning("[flipkart] Skipping review: %s", exc)

            page += 1

        logger.info("[flipkart] Yielded %d items", yielded)
