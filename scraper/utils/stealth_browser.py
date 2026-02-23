"""Shared stealth Playwright browser context.

Patches the Chromium browser to remove all automation fingerprints before
any page loads, making it indistinguishable from a real user's Chrome.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Iterator

logger = logging.getLogger(__name__)


@contextmanager
def stealth_page(headless: bool = True) -> Iterator:
    """Context manager that yields a stealth-patched Playwright page.

    Usage:
        with stealth_page() as page:
            page.goto("https://example.com")
            html = page.content()
    """
    try:
        from playwright.sync_api import sync_playwright
        from playwright_stealth import Stealth
    except ImportError as e:
        raise ImportError(
            "playwright and playwright-stealth are required. "
            "Run: pip install playwright playwright-stealth && playwright install chromium"
        ) from e

    stealth = Stealth(navigator_webdriver=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=headless,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars",
                "--disable-dev-shm-usage",
            ],
        )
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/121.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
            locale="en-US",
            timezone_id="America/New_York",
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            },
        )
        page = context.new_page()
        stealth.apply_stealth_sync(page)
        try:
            yield page
        finally:
            browser.close()


def fetch_html(url: str, headless: bool = True, wait_until: str = "domcontentloaded", timeout: int = 30000) -> str:
    """Fetch a URL with stealth Playwright and return the page HTML."""
    with stealth_page(headless=headless) as page:
        page.goto(url, wait_until=wait_until, timeout=timeout)
        return page.content()
