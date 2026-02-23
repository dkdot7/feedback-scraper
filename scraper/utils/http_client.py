"""Shared requests.Session with retry logic and a rotating User-Agent."""

from __future__ import annotations

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]

_ua_index = 0


def _next_user_agent() -> str:
    global _ua_index
    agent = _USER_AGENTS[_ua_index % len(_USER_AGENTS)]
    _ua_index += 1
    return agent


def make_session(
    total_retries: int = 3,
    backoff_factor: float = 2.0,
    status_forcelist: tuple[int, ...] = (429, 500, 502, 503, 504),
    timeout: int = 30,
) -> requests.Session:
    """Create a requests.Session with automatic retries and a desktop User-Agent."""
    session = requests.Session()

    retry = Retry(
        total=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=list(status_forcelist),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    session.headers.update(
        {
            "User-Agent": _next_user_agent(),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )
    session.request = _with_timeout(session.request, timeout)
    return session


def _with_timeout(original_request, default_timeout: int):
    """Wrap session.request to inject a default timeout when none is given."""

    def request_with_timeout(method, url, **kwargs):
        kwargs.setdefault("timeout", default_timeout)
        return original_request(method, url, **kwargs)

    return request_with_timeout
