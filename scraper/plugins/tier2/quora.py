"""Quora scraper — basic requests with session cookie (JS-heavy, limited).

⚠  WARNING: Quora ToS explicitly prohibits scraping.
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

_SEARCH_URL = "https://www.quora.com/search"


class QuoraScraper(BaseScraper):
    SOURCE_ID = "quora"
    TIER = "tier2"
    REQUIRES_KEYS: list[str] = []

    def scrape(self) -> Iterator[FeedbackItem]:
        search_query: str = self._param(
            "search_query", f"{self.config.product_name} review"
        )
        session_cookie = self._get_env("QUORA_SESSION")

        session = make_session()
        if session_cookie:
            session.cookies.set("m-b", session_cookie, domain=".quora.com")

        logger.info(
            "[quora] Searching Quora for '%s' (limited without Playwright)", search_query
        )

        self.rate_limiter.wait()
        try:
            resp = session.get(
                _SEARCH_URL,
                params={"q": search_query, "type": "answer"},
            )
            if resp.status_code in (403, 429):
                logger.warning("[quora] Access blocked (%d) — Quora is JS-heavy", resp.status_code)
                return
            resp.raise_for_status()
        except Exception as exc:
            logger.error("[quora] Request failed: %s", exc)
            return

        soup = BeautifulSoup(resp.text, "lxml")
        answers = soup.select(
            "div.q-box.spacing_log_answer_content, .answer-content, .AnswerBase"
        )

        if not answers:
            logger.info(
                "[quora] No answers parsed — Quora heavily uses client-side rendering. "
                "Consider using the Playwright-based quora scraper instead."
            )
            return

        yielded = 0
        for ans in answers:
            if yielded >= self.max_items:
                break
            try:
                body_text = ans.get_text(separator=" ", strip=True)
                if not body_text or len(body_text) < 20:
                    continue

                author_el = ans.find_previous(
                    "a", class_=lambda c: c and "author" in c.lower()
                )
                author = author_el.get_text(strip=True) if author_el else None

                item = FeedbackItem(
                    id=make_feedback_id(self.SOURCE_ID, None, author, body_text),
                    source=self.SOURCE_ID,
                    product=self.config.product_name,
                    author=author,
                    body=body_text,
                    date=None,
                    url=_SEARCH_URL,
                    scraped_at=now_iso(),
                    language="en",
                    tags=["quora"],
                    raw=None,
                )
                yield item
                yielded += 1
            except Exception as exc:
                logger.warning("[quora] Skipping answer: %s", exc)

        logger.info("[quora] Yielded %d items", yielded)
