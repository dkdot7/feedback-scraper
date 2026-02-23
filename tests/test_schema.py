"""Unit tests for FeedbackItem schema and utilities."""

import pytest
from pydantic import ValidationError

from scraper.schema import FeedbackItem, make_feedback_id, now_iso
from scraper.utils.hashing import make_id
from scraper.utils.date_parser import normalize_date


class TestMakeId:
    def test_returns_64_char_hex(self):
        result = make_id("test_key")
        assert len(result) == 64
        assert all(c in "0123456789abcdef" for c in result)

    def test_deterministic(self):
        assert make_id("same") == make_id("same")

    def test_different_inputs_different_outputs(self):
        assert make_id("a") != make_id("b")


class TestMakeFeedbackId:
    def test_uses_url_when_available(self):
        id1 = make_feedback_id("play_store", "http://example.com/1", "user", "body")
        id2 = make_feedback_id("play_store", "http://example.com/2", "user", "body")
        assert id1 != id2

    def test_falls_back_to_author_body(self):
        id1 = make_feedback_id("reddit", None, "user1", "some body text")
        id2 = make_feedback_id("reddit", None, "user2", "some body text")
        assert id1 != id2

    def test_falls_back_to_body_only(self):
        result = make_feedback_id("hn", None, None, "a" * 300)
        assert len(result) == 64


class TestNormalizeDate:
    def test_iso_string(self):
        assert normalize_date("2024-01-15T10:30:00Z") == "2024-01-15"

    def test_unix_timestamp_int(self):
        assert normalize_date(1705276800) == "2024-01-15"

    def test_already_yyyy_mm_dd(self):
        assert normalize_date("2024-03-20") == "2024-03-20"

    def test_human_readable(self):
        assert normalize_date("January 15, 2024") == "2024-01-15"

    def test_none_input(self):
        assert normalize_date(None) is None

    def test_empty_string(self):
        assert normalize_date("") is None

    def test_unix_timestamp_float(self):
        result = normalize_date(1705276800.0)
        assert result == "2024-01-15"

    def test_millisecond_timestamp(self):
        result = normalize_date(1705276800000)
        assert result == "2024-01-15"


class TestFeedbackItem:
    def _valid(self, **kwargs):
        defaults = {
            "id": "a" * 64,
            "source": "play_store",
            "product": "Notion",
            "body": "Great app!",
            "scraped_at": now_iso(),
        }
        defaults.update(kwargs)
        return FeedbackItem(**defaults)

    def test_valid_minimal(self):
        item = self._valid()
        assert item.source == "play_store"
        assert item.body == "Great app!"

    def test_body_stripped(self):
        item = self._valid(body="  padded  ")
        assert item.body == "padded"

    def test_empty_body_raises(self):
        with pytest.raises(ValidationError):
            self._valid(body="   ")

    def test_rating_clamped_high(self):
        item = self._valid(rating=10.0)
        assert item.rating == 5.0

    def test_rating_clamped_low(self):
        item = self._valid(rating=-1.0)
        assert item.rating == 0.0

    def test_rating_none_allowed(self):
        item = self._valid(rating=None)
        assert item.rating is None

    def test_optional_fields_default_none(self):
        item = self._valid()
        assert item.author is None
        assert item.url is None
        assert item.title is None

    def test_tags_default_empty_list(self):
        item = self._valid()
        assert item.tags == []

    def test_extra_fields_forbidden(self):
        with pytest.raises(ValidationError):
            self._valid(nonexistent_field="value")

    def test_json_serialization(self):
        item = self._valid(rating=4.5, tags=["test"])
        data = item.model_dump(mode="json")
        assert data["rating"] == 4.5
        assert "raw" in data
