"""Reddit scraper using PRAW."""

from __future__ import annotations

import logging
from typing import Iterator

from scraper.base import BaseScraper, ConfigError
from scraper.schema import FeedbackItem, make_feedback_id, now_iso
from scraper.utils.date_parser import normalize_date

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.reddit.com"


class RedditScraper(BaseScraper):
    SOURCE_ID = "reddit"
    TIER = "tier1"
    REQUIRES_KEYS = ["REDDIT_CLIENT_ID", "REDDIT_CLIENT_SECRET"]

    def validate_config(self) -> None:
        super().validate_config()

    def scrape(self) -> Iterator[FeedbackItem]:
        try:
            import praw
        except ImportError:
            logger.error("[reddit] praw not installed")
            return

        client_id = self._get_env("REDDIT_CLIENT_ID")
        client_secret = self._get_env("REDDIT_CLIENT_SECRET")
        user_agent = self._get_env("REDDIT_USER_AGENT", "FeedbackScraper/1.0")

        try:
            reddit = praw.Reddit(
                client_id=client_id,
                client_secret=client_secret,
                user_agent=user_agent,
                check_for_async=False,
            )
        except Exception as exc:
            logger.error("[reddit] Failed to init PRAW: %s", exc)
            return

        subreddits: list[str] = self._param("subreddits", [])
        search_query: str = self._param("search_query", self.config.product_name)
        time_filter: str = self._param("time_filter", "month")
        yielded = 0

        for sub_name in subreddits:
            if yielded >= self.max_items:
                break
            logger.info("[reddit] Searching r/%s for '%s'", sub_name, search_query)
            try:
                self.rate_limiter.wait()
                subreddit = reddit.subreddit(sub_name)
                for submission in subreddit.search(
                    search_query, time_filter=time_filter, limit=None
                ):
                    if yielded >= self.max_items:
                        break
                    try:
                        body = (submission.selftext or submission.title or "").strip()
                        if not body or body == "[deleted]" or body == "[removed]":
                            # Use title as body for link posts
                            body = submission.title.strip()
                        if not body:
                            continue

                        url = _BASE_URL + submission.permalink
                        item = FeedbackItem(
                            id=make_feedback_id(
                                self.SOURCE_ID, url, submission.author.name if submission.author else None, body
                            ),
                            source=self.SOURCE_ID,
                            product=self.config.product_name,
                            author=submission.author.name if submission.author else None,
                            rating=None,
                            title=submission.title,
                            body=body,
                            date=normalize_date(submission.created_utc),
                            url=url,
                            scraped_at=now_iso(),
                            helpful_votes=submission.score,
                            language="en",
                            tags=["reddit", f"r/{sub_name}", "post"],
                            raw=None,
                        )
                        yield item
                        yielded += 1

                        # Also yield top-level comments
                        self.rate_limiter.wait()
                        submission.comments.replace_more(limit=0)
                        for comment in submission.comments.list():
                            if yielded >= self.max_items:
                                break
                            try:
                                cbody = (comment.body or "").strip()
                                if not cbody or cbody in ("[deleted]", "[removed]"):
                                    continue
                                curl = _BASE_URL + comment.permalink
                                citem = FeedbackItem(
                                    id=make_feedback_id(
                                        self.SOURCE_ID,
                                        curl,
                                        comment.author.name if comment.author else None,
                                        cbody,
                                    ),
                                    source=self.SOURCE_ID,
                                    product=self.config.product_name,
                                    author=comment.author.name if comment.author else None,
                                    rating=None,
                                    body=cbody,
                                    date=normalize_date(comment.created_utc),
                                    url=curl,
                                    scraped_at=now_iso(),
                                    helpful_votes=comment.score,
                                    language="en",
                                    tags=["reddit", f"r/{sub_name}", "comment"],
                                    raw=None,
                                )
                                yield citem
                                yielded += 1
                            except Exception as exc:
                                logger.warning("[reddit] Skipping comment: %s", exc)

                    except Exception as exc:
                        logger.warning("[reddit] Skipping submission: %s", exc)

            except Exception as exc:
                logger.error("[reddit] Error searching r/%s: %s", sub_name, exc)
