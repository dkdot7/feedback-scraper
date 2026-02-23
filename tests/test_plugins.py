"""Integration-style tests for scrapers using mocked HTTP responses."""

import json
import os
from unittest.mock import MagicMock, patch

import pytest
import responses as resp_mock

from scraper.base import BaseScraper, ConfigError, ScraperConfig
from scraper.schema import FeedbackItem, now_iso


def _make_config(source_params: dict = None, env: dict = None) -> ScraperConfig:
    return ScraperConfig(
        product_name="TestProduct",
        product_slug="test-product",
        source_params=source_params or {},
        output_dir="/tmp",
        max_items=10,
        freshness_hours=24,
        rate_limit_delay=0.0,   # No delay in tests
        rate_limit_jitter=0.0,
        debug=False,
        env=env or {},
    )


# ── Registry ──────────────────────────────────────────────────────────────────

class TestRegistry:
    def test_list_sources_returns_list(self):
        from scraper.registry import list_sources
        sources = list_sources()
        assert isinstance(sources, list)
        assert len(sources) > 0

    def test_all_sources_have_source_id(self):
        from scraper.registry import get_all
        for source_id, cls in get_all().items():
            assert source_id == cls.SOURCE_ID
            assert cls.TIER in ("tier1", "tier2", "tier3", "optional")

    def test_get_known_source(self):
        from scraper.registry import get
        cls = get("hacker_news")
        assert cls is not None
        assert cls.SOURCE_ID == "hacker_news"

    def test_get_unknown_source_returns_none(self):
        from scraper.registry import get
        assert get("nonexistent_source_xyz") is None


# ── BaseScraper validation ────────────────────────────────────────────────────

class TestBaseScraperValidation:
    def test_missing_required_key_raises_config_error(self):
        from scraper.plugins.tier1.reddit import RedditScraper
        config = _make_config(env={})  # no keys set
        scraper = RedditScraper(config)
        with pytest.raises(ConfigError):
            scraper.validate_config()

    def test_present_required_keys_passes(self):
        from scraper.plugins.tier1.reddit import RedditScraper
        config = _make_config(env={
            "REDDIT_CLIENT_ID": "fake_id",
            "REDDIT_CLIENT_SECRET": "fake_secret",
        })
        scraper = RedditScraper(config)
        scraper.validate_config()  # should not raise


# ── Hacker News (no auth, uses HTTP) ─────────────────────────────────────────

class TestHackerNewsScraper:
    @resp_mock.activate
    def test_yields_items_from_api(self):
        from scraper.plugins.tier1.hacker_news import HackerNewsScraper

        fake_response = {
            "hits": [
                {
                    "objectID": "12345",
                    "title": "Notion is great",
                    "story_text": "I love using Notion for notes.",
                    "author": "testuser",
                    "created_at": "2024-01-15T10:00:00Z",
                    "points": 42,
                    "url": "https://example.com",
                }
            ],
            "nbPages": 1,
        }

        resp_mock.add(
            resp_mock.GET,
            "https://hn.algolia.com/api/v1/search_by_date",
            json=fake_response,
            status=200,
        )

        config = _make_config(
            source_params={"search_query": "Notion", "tags": "story"},
        )
        scraper = HackerNewsScraper(config)
        items = list(scraper.scrape())

        assert len(items) == 1
        assert items[0].source == "hacker_news"
        assert items[0].author == "testuser"
        assert items[0].helpful_votes == 42
        assert items[0].date == "2024-01-15"

    @resp_mock.activate
    def test_handles_empty_response(self):
        from scraper.plugins.tier1.hacker_news import HackerNewsScraper

        resp_mock.add(
            resp_mock.GET,
            "https://hn.algolia.com/api/v1/search_by_date",
            json={"hits": [], "nbPages": 0},
            status=200,
        )

        config = _make_config(source_params={"search_query": "Notion"})
        scraper = HackerNewsScraper(config)
        items = list(scraper.scrape())
        assert items == []

    @resp_mock.activate
    def test_handles_api_error_gracefully(self):
        from scraper.plugins.tier1.hacker_news import HackerNewsScraper

        resp_mock.add(
            resp_mock.GET,
            "https://hn.algolia.com/api/v1/search_by_date",
            status=500,
        )

        config = _make_config(source_params={"search_query": "Notion"})
        scraper = HackerNewsScraper(config)
        items = list(scraper.scrape())  # Should not raise
        assert items == []


# ── Steam (no auth, uses HTTP) ────────────────────────────────────────────────

class TestSteamScraper:
    @resp_mock.activate
    def test_yields_items(self):
        from scraper.plugins.tier1.steam import SteamScraper

        fake_response = {
            "success": 1,
            "reviews": [
                {
                    "recommendationid": "rec001",
                    "review": "Amazing game, totally worth it.",
                    "voted_up": True,
                    "timestamp_created": 1705276800,
                    "votes_up": 15,
                    "author": {"steamid": "76561198012345678"},
                    "steam_purchase": True,
                }
            ],
            "cursor": "",
        }

        resp_mock.add(
            resp_mock.GET,
            "https://store.steampowered.com/appreviews/12345",
            json=fake_response,
            status=200,
        )

        config = _make_config(
            source_params={"app_id": "12345", "language": "english", "review_type": "all"}
        )
        scraper = SteamScraper(config)
        items = list(scraper.scrape())

        assert len(items) == 1
        assert items[0].rating == 5.0  # voted_up=True → 5.0
        assert items[0].verified_purchase is True
        assert items[0].date == "2024-01-15"

    def test_missing_app_id_yields_nothing(self):
        from scraper.plugins.tier1.steam import SteamScraper
        config = _make_config(source_params={"app_id": "0"})
        scraper = SteamScraper(config)
        items = list(scraper.scrape())
        assert items == []


# ── Schema deduplication (orchestrator logic) ─────────────────────────────────

class TestDeduplication:
    def test_same_id_deduplicated(self):
        from scraper.schema import make_feedback_id

        items = [
            FeedbackItem(
                id=make_feedback_id("test", "http://x.com/1", None, "body"),
                source="test",
                product="Test",
                body="body",
                scraped_at=now_iso(),
            )
            for _ in range(3)
        ]

        seen: set[str] = set()
        unique = []
        for item in items:
            if item.id not in seen:
                seen.add(item.id)
                unique.append(item)

        assert len(unique) == 1
