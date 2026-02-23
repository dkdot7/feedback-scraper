#!/usr/bin/env python3
"""Feedback Intelligence Scraper — CLI entry point."""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import click
import yaml
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table

load_dotenv()

console = Console()


def _setup_logging(log_dir: str = "logs") -> None:
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = Path(log_dir) / f"scrape_{timestamp}.log"

    fmt = "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stderr),
        ],
    )
    # Silence noisy third-party loggers
    for noisy in ("urllib3", "prawcore", "googleapiclient", "playwright"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def _load_config(config_path: str) -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


@click.group(invoke_without_command=True)
@click.pass_context
def cli(ctx: click.Context) -> None:
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())


@cli.command("run")
@click.option("--product", "-p", default=None, help="Product name (overrides config.yaml)")
@click.option("--all", "run_all", is_flag=True, default=False, help="Run all enabled sources")
@click.option("--sources", "-s", default=None, help="Comma-separated list of source IDs")
@click.option("--force", "-f", is_flag=True, default=False, help="Ignore freshness cache")
@click.option("--dry-run", is_flag=True, default=False, help="Validate config without scraping")
@click.option(
    "--tos-aware",
    is_flag=True,
    default=False,
    help=(
        "Acknowledge ToS restrictions and allow Tier 2/3 scrapers. "
        "These scrapers may violate the Terms of Service of their targets. "
        "Use for internal research only; never redistribute scraped data."
    ),
)
@click.option("--config", "config_path", default="config.yaml", help="Path to config.yaml")
@click.option("--strip-raw/--keep-raw", default=True, help="Strip raw field from output JSON")
def run_command(
    product: str | None,
    run_all: bool,
    sources: str | None,
    force: bool,
    dry_run: bool,
    tos_aware: bool,
    config_path: str,
    strip_raw: bool,
) -> None:
    """Run the feedback scraper for a product."""
    _setup_logging()

    if not run_all and not sources:
        console.print(
            "[red]Error:[/red] Specify --all or --sources <id1,id2,...>",
            highlight=False,
        )
        sys.exit(1)

    try:
        cfg = _load_config(config_path)
    except FileNotFoundError:
        console.print(f"[red]Config file not found:[/red] {config_path}")
        sys.exit(1)

    product_name = product or cfg["product"]["name"]
    product_slug = cfg["product"]["slug"]
    global_cfg: dict = cfg.get("global", {})
    config_sources: dict = cfg.get("sources", {})
    output_dir: str = global_cfg.get("output_dir", "output")

    source_ids: list[str] | None = None
    if sources:
        source_ids = [s.strip() for s in sources.split(",") if s.strip()]

    if dry_run:
        console.print("[yellow]Dry-run mode:[/yellow] config validation only, no scraping.")

    if tos_aware:
        console.print(
            "[yellow]⚠  --tos-aware active:[/yellow] Tier 2/3 scrapers enabled. "
            "Use for internal research only."
        )

    from scraper.orchestrator import print_summary, run_scrapers

    results = run_scrapers(
        product_name=product_name,
        product_slug=product_slug,
        source_ids=source_ids,
        config_sources=config_sources,
        global_cfg=global_cfg,
        output_dir=output_dir,
        force=force,
        dry_run=dry_run,
        tos_aware=tos_aware,
        strip_raw=strip_raw,
    )

    print_summary(results)


@cli.command("list-sources")
@click.option("--config", "config_path", default="config.yaml", help="Path to config.yaml")
def list_sources_command(config_path: str) -> None:
    """List all available scraper sources with their tier and required keys."""
    from scraper.registry import list_sources

    try:
        cfg = _load_config(config_path)
        config_sources: dict = cfg.get("sources", {})
    except FileNotFoundError:
        config_sources = {}

    sources = list_sources()

    table = Table(title="Available Scraper Sources")
    table.add_column("Source ID", style="bold cyan")
    table.add_column("Tier", justify="center")
    table.add_column("Auth Required")
    table.add_column("Enabled in Config", justify="center")

    tier_colors = {
        "tier1": "green",
        "tier2": "yellow",
        "tier3": "red",
        "optional": "blue",
    }

    for s in sources:
        color = tier_colors.get(s["tier"], "white")
        enabled_cfg = config_sources.get(s["source_id"], {})
        is_enabled = enabled_cfg.get("enabled", True)
        enabled_str = "[green]✓[/green]" if is_enabled else "[dim]✗[/dim]"

        table.add_row(
            s["source_id"],
            f"[{color}]{s['tier']}[/{color}]",
            ", ".join(s["requires_keys"]) or "[dim]none[/dim]",
            enabled_str,
        )

    console.print(table)
    console.print(
        "\n[dim]Tier 2/3 sources require the [bold]--tos-aware[/bold] flag to run.[/dim]"
    )


# Allow direct invocation: python main.py --all  (maps to `run` subcommand)
@click.command("main", cls=click.CommandCollection, sources=[cli])
def _compat():
    pass


if __name__ == "__main__":
    # Support both `python main.py run --all` and legacy `python main.py --all`
    if len(sys.argv) > 1 and sys.argv[1] not in ("run", "list-sources", "--help", "-h"):
        sys.argv.insert(1, "run")
    cli()
