"""Microsoft Store review scraper — Playwright (click reviews tab, wait networkidle).

⚠  ToS is ambiguous. Use for internal research only.
   Requires --tos-aware flag to run.
"""

from __future__ import annotations

import logging
from typing import Iterator

from scraper.base import BaseScraper
from scraper.schema import FeedbackItem, make_feedback_id, now_iso
from scraper.utils.date_parser import normalize_date

logger = logging.getLogger(__name__)

_APP_URL = "https://apps.microsoft.com/detail/{app_id}"


class MicrosoftStoreScraper(BaseScraper):
    SOURCE_ID = "microsoft_store"
    TIER = "tier3"
    REQUIRES_KEYS: list[str] = []

    def scrape(self) -> Iterator[FeedbackItem]:
        try:
            from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
        except ImportError:
            logger.error("[microsoft_store] playwright not installed")
            return

        app_id: str = self._param("app_id", "")
        if not app_id:
            logger.error("[microsoft_store] 'app_id' not configured")
            return

        headless: bool = self.config.source_params.get("headless", True)
        base_url = _APP_URL.format(app_id=app_id)

        logger.info("[microsoft_store] Launching Playwright for %s", base_url)
        yielded = 0

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/121.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1366, "height": 768},
            )
            page = context.new_page()

            try:
                self.rate_limiter.wait()
                page.goto(base_url, wait_until="networkidle", timeout=30000)

                # Click the "Ratings and reviews" tab
                try:
                    tab = page.get_by_role("tab", name_or_text="Ratings and reviews")
                    if not tab:
                        tab = page.query_selector(
                            "[data-testid='reviews-tab'], button:has-text('Reviews'), "
                            "a:has-text('Ratings')"
                        )
                    if tab:
                        tab.click()
                        page.wait_for_load_state("networkidle", timeout=10000)
                except Exception:
                    logger.debug("[microsoft_store] Could not find/click reviews tab")

                # Scroll to load more reviews
                for _ in range(5):
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    page.wait_for_timeout(1500)

                review_cards = page.query_selector_all(
                    "[data-testid='review-card'], div.review, article.review, .c-review"
                )

                if not review_cards:
                    logger.info("[microsoft_store] No review cards found")
                    browser.close()
                    return

                for card in review_cards:
                    if yielded >= self.max_items:
                        break
                    try:
                        body_el = card.query_selector(
                            ".review-body, .review-text, p, [data-testid='review-body']"
                        )
                        body_text = (body_el.inner_text() if body_el else "").strip()
                        if not body_text:
                            continue

                        title_el = card.query_selector(
                            ".review-title, h3, h4, [data-testid='review-title']"
                        )
                        title = title_el.inner_text().strip() if title_el else None

                        rating_el = card.query_selector("[data-rating], .rating, [aria-label*='star']")
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

                        author_el = card.query_selector(".reviewer-name, .author, [data-testid='reviewer']")
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
                            url=base_url,
                            scraped_at=now_iso(),
                            tags=["microsoft_store"],
                            raw=None,
                        )
                        yield item
                        yielded += 1
                    except Exception as exc:
                        logger.warning("[microsoft_store] Skipping card: %s", exc)

            except Exception as exc:
                logger.error("[microsoft_store] Playwright error: %s", exc)
            finally:
                browser.close()

        logger.info("[microsoft_store] Yielded %d items", yielded)
