"""YouTube scraper using YouTube Data API v3."""

from __future__ import annotations

import logging
from typing import Iterator

from scraper.base import BaseScraper, ConfigError
from scraper.schema import FeedbackItem, make_feedback_id, now_iso
from scraper.utils.date_parser import normalize_date

logger = logging.getLogger(__name__)


class YouTubeScraper(BaseScraper):
    SOURCE_ID = "youtube"
    TIER = "tier1"
    REQUIRES_KEYS = ["YOUTUBE_API_KEY"]

    def scrape(self) -> Iterator[FeedbackItem]:
        try:
            from googleapiclient.discovery import build
            from googleapiclient.errors import HttpError
        except ImportError:
            logger.error("[youtube] google-api-python-client not installed")
            return

        api_key = self._get_env("YOUTUBE_API_KEY")
        if not api_key:
            logger.error("[youtube] YOUTUBE_API_KEY not set")
            return

        search_query: str = self._param("search_query", self.config.product_name)
        max_videos: int = self._param("max_videos", 10)
        max_comments: int = self._param("max_comments_per_video", 50)
        order: str = self._param("order", "relevance")

        try:
            youtube = build("youtube", "v3", developerKey=api_key)
        except Exception as exc:
            logger.error("[youtube] Failed to build API client: %s", exc)
            return

        logger.info("[youtube] Searching for '%s' (max_videos=%d)", search_query, max_videos)

        # 1. Search for videos
        try:
            self.rate_limiter.wait()
            search_resp = (
                youtube.search()
                .list(
                    q=search_query,
                    part="id,snippet",
                    type="video",
                    order=order,
                    maxResults=min(max_videos, 50),
                )
                .execute()
            )
        except Exception as exc:
            logger.error("[youtube] Search failed: %s", exc)
            return

        video_ids = [
            item["id"]["videoId"]
            for item in search_resp.get("items", [])
            if item.get("id", {}).get("videoId")
        ]

        yielded = 0

        for video_id in video_ids:
            if yielded >= self.max_items:
                break

            video_url = f"https://www.youtube.com/watch?v={video_id}"
            logger.debug("[youtube] Fetching comments for %s", video_url)

            page_token = None
            fetched = 0

            while fetched < max_comments and yielded < self.max_items:
                self.rate_limiter.wait()
                try:
                    params: dict = {
                        "part": "snippet",
                        "videoId": video_id,
                        "maxResults": min(100, max_comments - fetched),
                        "order": "relevance",
                        "textFormat": "plainText",
                    }
                    if page_token:
                        params["pageToken"] = page_token

                    thread_resp = youtube.commentThreads().list(**params).execute()
                except Exception as exc:
                    logger.warning("[youtube] Comment fetch failed for %s: %s", video_id, exc)
                    break

                for thread in thread_resp.get("items", []):
                    if yielded >= self.max_items or fetched >= max_comments:
                        break
                    try:
                        snippet = thread["snippet"]["topLevelComment"]["snippet"]
                        body = (snippet.get("textDisplay") or "").strip()
                        if not body:
                            continue

                        item = FeedbackItem(
                            id=make_feedback_id(
                                self.SOURCE_ID,
                                f"{video_url}#comment-{thread['id']}",
                                snippet.get("authorDisplayName"),
                                body,
                            ),
                            source=self.SOURCE_ID,
                            product=self.config.product_name,
                            author=snippet.get("authorDisplayName"),
                            rating=None,
                            body=body,
                            date=normalize_date(snippet.get("publishedAt")),
                            url=video_url,
                            scraped_at=now_iso(),
                            helpful_votes=snippet.get("likeCount"),
                            language="en",
                            tags=["youtube", "comment"],
                            raw=snippet if self.config.debug else None,
                        )
                        yield item
                        yielded += 1
                        fetched += 1
                    except Exception as exc:
                        logger.warning("[youtube] Skipping comment: %s", exc)

                page_token = thread_resp.get("nextPageToken")
                if not page_token:
                    break

        logger.info("[youtube] Yielded %d items", yielded)
