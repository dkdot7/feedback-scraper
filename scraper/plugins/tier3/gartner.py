"""Gartner Peer Insights scraper — Playwright (JS-heavy).

⚠  WARNING: Gartner ToS explicitly prohibits scraping.
   Use for internal research only. Never redistribute scraped data.
   Requires --tos-aware flag to run.
"""

from __future__ import annotations

import logging
from typing import Iterator

from scraper.base import BaseScraper
from scraper.schema import FeedbackItem, make_feedback_id, now_iso
from scraper.utils.date_parser import normalize_date

logger = logging.getLogger(__name__)


class GartnerScraper(BaseScraper):
    SOURCE_ID = "gartner"
    TIER = "tier3"
    REQUIRES_KEYS: list[str] = []

    def scrape(self) -> Iterator[FeedbackItem]:
        try:
            from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
        except ImportError:
            logger.error("[gartner] playwright not installed — run: playwright install chromium")
            return

        url: str = self._param("url", "")
        if not url:
            logger.error("[gartner] 'url' not configured")
            return

        session_cookie = self._get_env("GARTNER_SESSION_COOKIE")
        headless: bool = self.config.source_params.get("headless", True)

        logger.info("[gartner] Launching Playwright for %s", url)
        yielded = 0

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/121.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 900},
            )
            if session_cookie:
                context.add_cookies([{
                    "name": "GSESSIONID",
                    "value": session_cookie,
                    "domain": ".gartner.com",
                    "path": "/",
                }])

            page = context.new_page()
            page_num = 1

            try:
                while yielded < self.max_items:
                    self.rate_limiter.wait()
                    page_url = url + f"?page={page_num}" if page_num > 1 else url

                    try:
                        page.goto(page_url, wait_until="networkidle", timeout=30000)
                    except PWTimeout:
                        logger.warning("[gartner] Timeout loading page %d", page_num)
                        break

                    # Wait for review cards to appear
                    try:
                        page.wait_for_selector(
                            ".review-panel, article.review, [data-testid='review-card']",
                            timeout=10000,
                        )
                    except PWTimeout:
                        logger.info("[gartner] No review cards on page %d", page_num)
                        break

                    cards = page.query_selector_all(
                        ".review-panel, article.review, [data-testid='review-card']"
                    )
                    if not cards:
                        break

                    found = 0
                    for card in cards:
                        if yielded >= self.max_items:
                            break
                        try:
                            body_el = card.query_selector(".review-text, .review-body, p")
                            body_text = (body_el.inner_text() if body_el else "").strip()
                            if not body_text:
                                continue

                            title_el = card.query_selector(".review-title, h3, h4")
                            title = title_el.inner_text().strip() if title_el else None

                            rating_el = card.query_selector("[data-rating], .rating")
                            rating: float | None = None
                            if rating_el:
                                for attr in ("data-rating", "data-score"):
                                    val = rating_el.get_attribute(attr)
                                    if val:
                                        try:
                                            rating = float(val)
                                        except ValueError:
                                            pass
                                        break

                            author_el = card.query_selector(".reviewer-name, .author")
                            author = author_el.inner_text().strip() if author_el else None

                            date_el = card.query_selector("time, .review-date")
                            date_str: str | None = None
                            if date_el:
                                date_str = date_el.get_attribute("datetime") or date_el.inner_text().strip()

                            item = FeedbackItem(
                                id=make_feedback_id(self.SOURCE_ID, None, author, body_text),
                                source=self.SOURCE_ID,
                                product=self.config.product_name,
                                author=author,
                                rating=rating,
                                title=title,
                                body=body_text,
                                date=normalize_date(date_str),
                                url=page_url,
                                scraped_at=now_iso(),
                                tags=["gartner"],
                                raw=None,
                            )
                            yield item
                            yielded += 1
                            found += 1
                        except Exception as exc:
                            logger.warning("[gartner] Skipping card: %s", exc)

                    if found == 0:
                        break

                    # Check for next page button
                    next_btn = page.query_selector(
                        "a[aria-label='Next page'], button.next-page, [data-testid='next-page']"
                    )
                    if not next_btn:
                        break
                    page_num += 1

            except Exception as exc:
                logger.error("[gartner] Playwright error: %s", exc)
            finally:
                browser.close()

        logger.info("[gartner] Yielded %d items", yielded)
