"""Stack Exchange / Stack Overflow scraper using the public API."""

from __future__ import annotations

import logging
from typing import Iterator

from scraper.base import BaseScraper
from scraper.schema import FeedbackItem, make_feedback_id, now_iso
from scraper.utils.date_parser import normalize_date
from scraper.utils.http_client import make_session

logger = logging.getLogger(__name__)

_API_URL = "https://api.stackexchange.com/2.3/search/advanced"
_ANSWER_URL = "https://api.stackexchange.com/2.3/questions/{ids}/answers"


class StackOverflowScraper(BaseScraper):
    SOURCE_ID = "stack_overflow"
    TIER = "tier1"
    REQUIRES_KEYS: list[str] = []   # API key is optional but increases quota

    def scrape(self) -> Iterator[FeedbackItem]:
        query: str = self._param("search_query", self.config.product_name)
        tags: list[str] = self._param("tags", [])
        api_key = self._get_env("STACKOVERFLOW_API_KEY")

        session = make_session()
        page = 1
        yielded = 0

        logger.info("[stack_overflow] Searching for '%s'", query)

        while yielded < self.max_items:
            self.rate_limiter.wait()
            params: dict = {
                "q": query,
                "site": "stackoverflow",
                "order": "desc",
                "sort": "relevance",
                "pagesize": min(100, self.max_items - yielded),
                "page": page,
                "filter": "withbody",
            }
            if tags:
                params["tagged"] = ";".join(tags)
            if api_key:
                params["key"] = api_key

            try:
                resp = session.get(_API_URL, params=params)
                resp.raise_for_status()
                data = resp.json()
            except Exception as exc:
                logger.error("[stack_overflow] Request failed (page %d): %s", page, exc)
                break

            items = data.get("items", [])
            if not items:
                break

            for q in items:
                if yielded >= self.max_items:
                    break
                try:
                    body = (q.get("body") or q.get("title") or "").strip()
                    if not body:
                        continue

                    # Strip HTML tags simply
                    import re
                    body = re.sub(r"<[^>]+>", " ", body).strip()
                    if not body:
                        continue

                    url = q.get("link", "")
                    item = FeedbackItem(
                        id=make_feedback_id(
                            self.SOURCE_ID,
                            url,
                            q.get("owner", {}).get("display_name"),
                            body,
                        ),
                        source=self.SOURCE_ID,
                        product=self.config.product_name,
                        author=q.get("owner", {}).get("display_name"),
                        rating=None,
                        title=q.get("title"),
                        body=body,
                        date=normalize_date(q.get("creation_date")),
                        url=url,
                        scraped_at=now_iso(),
                        helpful_votes=q.get("score"),
                        language="en",
                        tags=["stack_overflow"] + (q.get("tags") or []),
                        raw=q if self.config.debug else None,
                    )
                    yield item
                    yielded += 1
                except Exception as exc:
                    logger.warning("[stack_overflow] Skipping item: %s", exc)

            if not data.get("has_more"):
                break
            page += 1

        logger.info("[stack_overflow] Yielded %d items", yielded)
