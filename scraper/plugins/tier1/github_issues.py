"""GitHub Issues scraper using PyGitHub."""

from __future__ import annotations

import logging
from typing import Iterator

from scraper.base import BaseScraper
from scraper.schema import FeedbackItem, make_feedback_id, now_iso
from scraper.utils.date_parser import normalize_date

logger = logging.getLogger(__name__)


class GitHubIssuesScraper(BaseScraper):
    SOURCE_ID = "github_issues"
    TIER = "tier1"
    REQUIRES_KEYS: list[str] = []   # GITHUB_TOKEN optional, raises rate limit

    def scrape(self) -> Iterator[FeedbackItem]:
        try:
            from github import Github, GithubException
        except ImportError:
            logger.error("[github_issues] PyGithub not installed")
            return

        token = self._get_env("GITHUB_TOKEN")
        repo_name: str = self._param("repo", "")
        state: str = self._param("state", "open")

        if not repo_name:
            logger.error("[github_issues] 'repo' not configured (e.g. 'owner/repo')")
            return

        g = Github(login_or_token=token) if token else Github()

        logger.info("[github_issues] Fetching %s issues from %s", state, repo_name)

        try:
            self.rate_limiter.wait()
            repo = g.get_repo(repo_name)
            issues = repo.get_issues(state=state, sort="updated", direction="desc")
        except Exception as exc:
            logger.error("[github_issues] Failed to access repo %s: %s", repo_name, exc)
            return

        yielded = 0
        for issue in issues:
            if yielded >= self.max_items:
                break
            try:
                body = (issue.body or "").strip()
                if not body:
                    body = issue.title

                if not body:
                    continue

                labels = [lbl.name for lbl in issue.labels]

                item = FeedbackItem(
                    id=make_feedback_id(
                        self.SOURCE_ID,
                        issue.html_url,
                        issue.user.login if issue.user else None,
                        body,
                    ),
                    source=self.SOURCE_ID,
                    product=self.config.product_name,
                    author=issue.user.login if issue.user else None,
                    rating=None,
                    title=issue.title,
                    body=body,
                    date=normalize_date(issue.created_at.isoformat() if issue.created_at else None),
                    url=issue.html_url,
                    scraped_at=now_iso(),
                    helpful_votes=issue.reactions.get("+1", 0) if hasattr(issue, "reactions") else None,
                    language="en",
                    tags=["github", "issue"] + labels,
                    raw=None,
                )
                yield item
                yielded += 1
                self.rate_limiter.wait()
            except Exception as exc:
                logger.warning("[github_issues] Skipping issue #%s: %s", getattr(issue, "number", "?"), exc)

        logger.info("[github_issues] Yielded %d items", yielded)
