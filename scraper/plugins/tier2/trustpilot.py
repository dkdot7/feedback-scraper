"""Trustpilot scraper — HTML + JSON-LD fallback.

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
from scraper.utils.http_client import make_session

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

        session = make_session()
        page = 1
        yielded = 0

        logger.info("[trustpilot] Scraping %s", slug)

        while yielded < self.max_items:
            self.rate_limiter.wait()
            url = _BASE.format(slug=slug) + f"?page={page}"
            try:
                resp = session.get(url)
                if resp.status_code == 429:
                    logger.warning("[trustpilot] 429 Too Many Requests — stopping")
                    break
                resp.raise_for_status()
            except Exception as exc:
                logger.error("[trustpilot] Request failed (page %d): %s", page, exc)
                break

            soup = BeautifulSoup(resp.text, "lxml")

            # Try JSON-LD first
            items_found = 0
            for script in soup.find_all("script", type="application/ld+json"):
                try:
                    data = json.loads(script.string or "")
                    reviews = []
                    if isinstance(data, dict) and data.get("@type") == "Product":
                        reviews = data.get("review", [])
                    elif isinstance(data, list):
                        for d in data:
                            if isinstance(d, dict) and d.get("@type") in ("Review", "UserReview"):
                                reviews.append(d)

                    for r in reviews:
                        if yielded >= self.max_items:
                            break
                        body_text = (
                            r.get("reviewBody")
                            or r.get("description")
                            or ""
                        ).strip()
                        if not body_text:
                            continue

                        rating_raw = (
                            r.get("reviewRating", {}).get("ratingValue")
                            if isinstance(r.get("reviewRating"), dict)
                            else r.get("reviewRating")
                        )
                        try:
                            rating = float(rating_raw) if rating_raw else None
                        except (TypeError, ValueError):
                            rating = None

                        author_name = None
                        author = r.get("author")
                        if isinstance(author, dict):
                            author_name = author.get("name")
                        elif isinstance(author, str):
                            author_name = author

                        item = FeedbackItem(
                            id=make_feedback_id(self.SOURCE_ID, None, author_name, body_text),
                            source=self.SOURCE_ID,
                            product=self.config.product_name,
                            author=author_name,
                            rating=rating,
                            title=r.get("name"),
                            body=body_text,
                            date=normalize_date(r.get("datePublished")),
                            url=url,
                            scraped_at=now_iso(),
                            tags=["trustpilot"],
                            raw=r if self.config.debug else None,
                        )
                        yield item
                        yielded += 1
                        items_found += 1
                except Exception as exc:
                    logger.debug("[trustpilot] JSON-LD parse error: %s", exc)

            # CSS fallback if JSON-LD gave nothing
            if items_found == 0:
                cards = soup.select("div[data-service-review-card-paper]")
                for card in cards:
                    if yielded >= self.max_items:
                        break
                    try:
                        body_el = card.select_one("p[data-service-review-text-typography]")
                        body_text = body_el.get_text(strip=True) if body_el else ""
                        if not body_text:
                            continue

                        title_el = card.select_one("h2[data-service-review-title-typography]")
                        title = title_el.get_text(strip=True) if title_el else None

                        rating_el = card.select_one("div[data-service-review-rating]")
                        rating_str = rating_el.get("data-service-review-rating", "") if rating_el else ""
                        try:
                            rating = float(rating_str) if rating_str else None
                        except ValueError:
                            rating = None

                        author_el = card.select_one("span[data-consumer-name-typography]")
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
                            tags=["trustpilot"],
                            raw=None,
                        )
                        yield item
                        yielded += 1
                        items_found += 1
                    except Exception as exc:
                        logger.warning("[trustpilot] Skipping card: %s", exc)

            if items_found == 0:
                logger.info("[trustpilot] No items on page %d — stopping", page)
                break

            page += 1

        logger.info("[trustpilot] Yielded %d items", yielded)
