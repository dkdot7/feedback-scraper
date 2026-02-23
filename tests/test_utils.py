"""Unit tests for scraper utilities."""

import time
import threading
import pytest

from scraper.utils.rate_limiter import SimpleDelayLimiter, SlidingWindowLimiter, make_rate_limiter
from scraper.utils.output_writer import is_fresh, write_output
from scraper.schema import FeedbackItem, now_iso


class TestSimpleDelayLimiter:
    def test_waits_approximately_delay(self):
        limiter = SimpleDelayLimiter(delay=0.05, jitter=0.0)
        start = time.monotonic()
        limiter.wait()
        elapsed = time.monotonic() - start
        assert 0.04 <= elapsed <= 0.15

    def test_jitter_does_not_make_negative_sleep(self):
        limiter = SimpleDelayLimiter(delay=0.01, jitter=5.0)
        # Should not raise or hang
        limiter.wait()


class TestSlidingWindowLimiter:
    def test_enforces_rpm_limit(self):
        limiter = SlidingWindowLimiter(max_calls=3, window_seconds=1.0)
        times = []
        for _ in range(4):
            limiter.wait()
            times.append(time.monotonic())
        # The 4th call must wait at least window after the 1st
        assert times[3] - times[0] >= 0.9

    def test_thread_safe(self):
        limiter = SlidingWindowLimiter(max_calls=10, window_seconds=1.0)
        errors = []

        def worker():
            try:
                limiter.wait()
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors


class TestMakeRateLimiter:
    def test_returns_simple_delay_by_default(self):
        limiter = make_rate_limiter(delay=1.0)
        assert isinstance(limiter, SimpleDelayLimiter)

    def test_returns_sliding_window_when_rpm_set(self):
        limiter = make_rate_limiter(rpm=60)
        assert isinstance(limiter, SlidingWindowLimiter)


class TestIsFresh:
    def test_missing_file_is_not_fresh(self, tmp_path):
        path = tmp_path / "nonexistent.json"
        assert not is_fresh(path, freshness_hours=24)

    def test_new_file_is_fresh(self, tmp_path):
        path = tmp_path / "fresh.json"
        path.write_text("[]")
        assert is_fresh(path, freshness_hours=24)

    def test_old_file_is_not_fresh(self, tmp_path):
        import os
        path = tmp_path / "old.json"
        path.write_text("[]")
        # Set mtime to 25 hours ago
        old_time = time.time() - 25 * 3600
        os.utime(path, (old_time, old_time))
        assert not is_fresh(path, freshness_hours=24)


class TestWriteOutput:
    def _make_item(self, body: str = "test body") -> FeedbackItem:
        return FeedbackItem(
            id="a" * 64,
            source="test_source",
            product="Test",
            body=body,
            scraped_at=now_iso(),
            tags=[],
        )

    def test_creates_file(self, tmp_path):
        items = [self._make_item("body one"), self._make_item("body two")]
        path = write_output(items, str(tmp_path), "my-product", "test_source")
        assert path.exists()

    def test_output_is_valid_json(self, tmp_path):
        import json
        items = [self._make_item("hello world")]
        path = write_output(items, str(tmp_path), "product", "source")
        data = json.loads(path.read_text())
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["body"] == "hello world"

    def test_raw_stripped_by_default(self, tmp_path):
        import json
        item = FeedbackItem(
            id="a" * 64,
            source="s",
            product="P",
            body="body",
            scraped_at=now_iso(),
            raw={"debug": "data"},
        )
        path = write_output([item], str(tmp_path), "p", "s", strip_raw=True)
        data = json.loads(path.read_text())
        assert "raw" not in data[0]

    def test_raw_preserved_when_keep_raw(self, tmp_path):
        import json
        item = FeedbackItem(
            id="a" * 64,
            source="s",
            product="P",
            body="body",
            scraped_at=now_iso(),
            raw={"key": "value"},
        )
        path = write_output([item], str(tmp_path), "p", "s", strip_raw=False)
        data = json.loads(path.read_text())
        assert data[0]["raw"] == {"key": "value"}
