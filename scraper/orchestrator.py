"""Orchestrator: runs scrapers with tiered concurrency and Rich live output."""

from __future__ import annotations

import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Sequence

from rich.console import Console
from rich.live import Live
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

from scraper.base import BaseScraper, ConfigError, ScraperConfig
from scraper.registry import get_all
from scraper.utils.output_writer import is_fresh, write_output

logger = logging.getLogger("scraper.orchestrator")
console = Console()

_TIER_WORKERS = {
    "tier1": 4,
    "tier2": 2,
    "tier3": 1,
    "optional": 1,
}

_TIER_ORDER = ["tier1", "tier2", "tier3", "optional"]


@dataclass
class ScrapeResult:
    source: str
    tier: str
    items_scraped: int = 0
    skipped: bool = False
    error: Optional[str] = None
    duration_seconds: float = 0.0

    @property
    def status_icon(self) -> str:
        if self.error:
            return "[red]✗[/red]"
        if self.skipped:
            return "[yellow]~[/yellow]"
        return "[green]✓[/green]"

    @property
    def status_label(self) -> str:
        if self.error:
            return f"Error: {self.error[:60]}"
        if self.skipped:
            return "Skipped (fresh)"
        return f"{self.items_scraped} items"


def _build_scraper(
    source_id: str,
    cls,
    product_name: str,
    product_slug: str,
    source_cfg: dict,
    global_cfg: dict,
    env: dict,
) -> BaseScraper:
    config = ScraperConfig.from_raw(
        product_name=product_name,
        product_slug=product_slug,
        source_params=source_cfg,
        global_cfg=global_cfg,
        env=env,
    )
    return cls(config)


def _run_single(
    scraper: BaseScraper,
    output_dir: str,
    product_slug: str,
    force: bool,
    dry_run: bool,
    strip_raw: bool,
) -> ScrapeResult:
    source_id = scraper.SOURCE_ID
    result = ScrapeResult(source=source_id, tier=scraper.TIER)
    start = time.perf_counter()

    try:
        # 1. Validate config
        scraper.validate_config()

        # 2. Freshness check
        output_path = Path(output_dir) / product_slug / f"{source_id}.json"
        if not force and is_fresh(output_path, scraper.config.freshness_hours):
            result.skipped = True
            result.duration_seconds = time.perf_counter() - start
            return result

        # 3. Dry run — skip actual scraping
        if dry_run:
            result.skipped = True
            result.duration_seconds = time.perf_counter() - start
            return result

        # 4. Scrape
        items = list(scraper.scrape())

        # 5. Deduplicate by id
        seen: set[str] = set()
        unique = []
        for item in items:
            if item.id not in seen:
                seen.add(item.id)
                unique.append(item)

        # 6. Write output
        write_output(unique, output_dir, product_slug, source_id, strip_raw=strip_raw)
        result.items_scraped = len(unique)

    except ConfigError as exc:
        result.error = str(exc)
        logger.error("[%s] ConfigError: %s", source_id, exc)
    except Exception as exc:
        result.error = str(exc)
        logger.exception("[%s] Unexpected error", source_id)

    result.duration_seconds = time.perf_counter() - start
    return result


def run_scrapers(
    product_name: str,
    product_slug: str,
    source_ids: list[str] | None,
    config_sources: dict,
    global_cfg: dict,
    output_dir: str,
    force: bool = False,
    dry_run: bool = False,
    tos_aware: bool = False,
    strip_raw: bool = True,
) -> list[ScrapeResult]:
    """Run all requested scrapers and return results."""
    env = dict(os.environ)
    all_scrapers = get_all()

    # Filter to requested source IDs
    if source_ids:
        candidates = {sid: all_scrapers[sid] for sid in source_ids if sid in all_scrapers}
        unknown = [sid for sid in source_ids if sid not in all_scrapers]
        if unknown:
            logger.warning("Unknown sources (ignored): %s", unknown)
    else:
        candidates = all_scrapers

    # Filter to only enabled sources from config
    enabled: dict[str, tuple] = {}
    for source_id, cls in candidates.items():
        src_cfg = config_sources.get(source_id, {})
        if not src_cfg.get("enabled", True):
            continue
        tier = cls.TIER
        if tier in ("tier2", "tier3") and not tos_aware:
            logger.info(
                "[%s] Skipped — requires --tos-aware flag (Tier 2/3)", source_id
            )
            continue
        enabled[source_id] = (cls, src_cfg, tier)

    # Group by tier
    by_tier: dict[str, list[tuple]] = {t: [] for t in _TIER_ORDER}
    for source_id, (cls, src_cfg, tier) in enabled.items():
        by_tier.setdefault(tier, []).append((source_id, cls, src_cfg))

    results: list[ScrapeResult] = []

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[bold]{task.description:<22}"),
        BarColumn(bar_width=20),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TextColumn("{task.fields[status]}"),
        console=console,
        transient=False,
    )

    with Live(progress, console=console, refresh_per_second=10):
        for tier in _TIER_ORDER:
            tier_items = by_tier.get(tier, [])
            if not tier_items:
                continue

            max_workers = _TIER_WORKERS.get(tier, 1)

            # Build scrapers
            scraper_map: dict[str, BaseScraper] = {}
            task_ids: dict[str, TaskID] = {}
            for source_id, cls, src_cfg in tier_items:
                try:
                    s = _build_scraper(
                        source_id, cls, product_name, product_slug,
                        src_cfg, global_cfg, env,
                    )
                    scraper_map[source_id] = s
                    task_ids[source_id] = progress.add_task(
                        source_id, total=1, status="[dim]queued[/dim]"
                    )
                except Exception as exc:
                    logger.error("[%s] Failed to instantiate: %s", source_id, exc)

            if max_workers == 1:
                # Sequential execution
                for source_id, scraper in scraper_map.items():
                    tid = task_ids[source_id]
                    progress.update(tid, status="[cyan]running…[/cyan]")
                    res = _run_single(
                        scraper, output_dir, product_slug, force, dry_run, strip_raw
                    )
                    results.append(res)
                    progress.update(tid, advance=1, status=res.status_label)
            else:
                futures = {}
                with ThreadPoolExecutor(max_workers=max_workers) as pool:
                    for source_id, scraper in scraper_map.items():
                        tid = task_ids[source_id]
                        progress.update(tid, status="[cyan]running…[/cyan]")
                        fut = pool.submit(
                            _run_single,
                            scraper, output_dir, product_slug, force, dry_run, strip_raw,
                        )
                        futures[fut] = (source_id, tid)

                    for fut in as_completed(futures):
                        source_id, tid = futures[fut]
                        try:
                            res = fut.result()
                        except Exception as exc:
                            res = ScrapeResult(source=source_id, tier=tier, error=str(exc))
                        results.append(res)
                        progress.update(tid, advance=1, status=res.status_label)

    return results


def print_summary(results: list[ScrapeResult]) -> None:
    """Print a Rich summary table after all scrapers complete."""
    table = Table(title="Scrape Summary", show_lines=False)
    table.add_column("Source", style="bold")
    table.add_column("Tier", justify="center")
    table.add_column("Status", justify="center")
    table.add_column("Items", justify="right")
    table.add_column("Duration", justify="right")
    table.add_column("Detail")

    total_items = 0
    for r in sorted(results, key=lambda x: (x.tier, x.source)):
        table.add_row(
            r.source,
            r.tier,
            r.status_icon,
            str(r.items_scraped) if not r.skipped else "—",
            f"{r.duration_seconds:.1f}s",
            r.status_label,
        )
        total_items += r.items_scraped

    console.print(table)
    console.print(
        f"\n[bold]Total:[/bold] {len(results)} sources, "
        f"[green]{total_items}[/green] items scraped."
    )
