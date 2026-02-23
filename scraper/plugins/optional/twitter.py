"""Twitter / X scraper using Tweepy v2 (requires paid Basic API tier).

Requires TWITTER_BEARER_TOKEN in .env.
~$100/month for Basic access tier as of 2024.
"""

from __future__ import annotations

import logging
from typing import Iterator

from scraper.base import BaseScraper
from scraper.schema import FeedbackItem, make_feedback_id, now_iso
from scraper.utils.date_parser import normalize_date

logger = logging.getLogger(__name__)


class TwitterScraper(BaseScraper):
    SOURCE_ID = "twitter"
    TIER = "optional"
    REQUIRES_KEYS = ["TWITTER_BEARER_TOKEN"]

    def scrape(self) -> Iterator[FeedbackItem]:
        try:
            import tweepy
        except ImportError:
            logger.error("[twitter] tweepy not installed")
            return

        bearer_token = self._get_env("TWITTER_BEARER_TOKEN")
        if not bearer_token:
            logger.error("[twitter] TWITTER_BEARER_TOKEN not set")
            return

        search_query: str = self._param(
            "search_query",
            f"{self.config.product_name} -is:retweet lang:en",
        )

        client = tweepy.Client(bearer_token=bearer_token, wait_on_rate_limit=True)

        logger.info("[twitter] Searching for: %s", search_query)

        yielded = 0
        next_token = None

        while yielded < self.max_items:
            self.rate_limiter.wait()
            try:
                response = client.search_recent_tweets(
                    query=search_query,
                    max_results=min(100, self.max_items - yielded),
                    tweet_fields=["created_at", "author_id", "public_metrics", "lang"],
                    expansions=["author_id"],
                    user_fields=["name", "username"],
                    next_token=next_token,
                )
            except tweepy.TooManyRequests:
                logger.warning("[twitter] Rate limit exceeded — stopping")
                break
            except tweepy.Forbidden as exc:
                logger.error("[twitter] API access denied — check your plan: %s", exc)
                break
            except Exception as exc:
                logger.error("[twitter] API error: %s", exc)
                break

            if not response.data:
                break

            # Build user lookup dict
            users: dict[str, str] = {}
            if response.includes and response.includes.get("users"):
                for u in response.includes["users"]:
                    users[str(u.id)] = u.name or u.username

            for tweet in response.data:
                if yielded >= self.max_items:
                    break
                try:
                    body = tweet.text.strip()
                    if not body:
                        continue

                    author = users.get(str(tweet.author_id))
                    tweet_url = f"https://twitter.com/i/web/status/{tweet.id}"
                    metrics = tweet.public_metrics or {}

                    item = FeedbackItem(
                        id=make_feedback_id(self.SOURCE_ID, tweet_url, author, body),
                        source=self.SOURCE_ID,
                        product=self.config.product_name,
                        author=author,
                        rating=None,
                        body=body,
                        date=normalize_date(
                            tweet.created_at.isoformat() if tweet.created_at else None
                        ),
                        url=tweet_url,
                        scraped_at=now_iso(),
                        helpful_votes=metrics.get("like_count"),
                        language=tweet.lang,
                        tags=["twitter"],
                        raw=None,
                    )
                    yield item
                    yielded += 1
                except Exception as exc:
                    logger.warning("[twitter] Skipping tweet: %s", exc)

            meta = response.meta or {}
            next_token = meta.get("next_token")
            if not next_token:
                break

        logger.info("[twitter] Yielded %d items", yielded)
