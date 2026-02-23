"""Hacker News scraper using Algolia HN API (free, no key required)."""

from __future__ import annotations

import logging
from typing import Iterator

from scraper.base import BaseScraper
from scraper.schema import FeedbackItem, make_feedback_id, now_iso
from scraper.utils.date_parser import normalize_date
from scraper.utils.http_client import make_session

logger = logging.getLogger(__name__)

_ALGOLIA_URL = "https://hn.algolia.com/api/v1/search_by_date"
_HN_ITEM_URL = "https://news.ycombinator.com/item?id={}"


class HackerNewsScraper(BaseScraper):
    SOURCE_ID = "hacker_news"
    TIER = "tier1"
    REQUIRES_KEYS: list[str] = []

    def scrape(self) -> Iterator[FeedbackItem]:
        query: str = self._param("search_query", self.config.product_name)
        tags: str = self._param("tags", "story")
        session = make_session()
        page = 0
        yielded = 0

        logger.info("[hacker_news] Searching for '%s' (tags=%s)", query, tags)

        while yielded < self.max_items:
            self.rate_limiter.wait()
            try:
                resp = session.get(
                    _ALGOLIA_URL,
                    params={
                        "query": query,
                        "tags": tags,
                        "hitsPerPage": min(100, self.max_items - yielded),
                        "page": page,
                    },
                )
                resp.raise_for_status()
                data = resp.json()
            except Exception as exc:
                logger.error("[hacker_news] Request failed (page %d): %s", page, exc)
                break

            hits = data.get("hits", [])
            if not hits:
                break

            for hit in hits:
                if yielded >= self.max_items:
                    break
                try:
                    body = (
                        hit.get("story_text")
                        or hit.get("comment_text")
                        or hit.get("title")
                        or ""
                    ).strip()
                    if not body:
                        continue

                    object_id = hit.get("objectID", "")
                    url = hit.get("url") or _HN_ITEM_URL.format(object_id)

                    item = FeedbackItem(
                        id=make_feedback_id(
                            self.SOURCE_ID,
                            _HN_ITEM_URL.format(object_id),
                            hit.get("author"),
                            body,
                        ),
                        source=self.SOURCE_ID,
                        product=self.config.product_name,
                        author=hit.get("author"),
                        rating=None,
                        title=hit.get("title"),
                        body=body,
                        date=normalize_date(hit.get("created_at")),
                        url=url,
                        scraped_at=now_iso(),
                        helpful_votes=hit.get("points"),
                        language="en",
                        tags=["hacker_news"],
                        raw=hit if self.config.debug else None,
                    )
                    yield item
                    yielded += 1
                except Exception as exc:
                    logger.warning("[hacker_news] Skipping hit: %s", exc)

            # Check if more pages
            nb_pages = data.get("nbPages", 1)
            page += 1
            if page >= nb_pages:
                break

        logger.info("[hacker_news] Yielded %d items", yielded)
