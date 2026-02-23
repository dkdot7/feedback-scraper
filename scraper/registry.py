"""Plugin auto-discovery registry.

Walks all subpackages of ``scraper.plugins`` and registers every concrete
subclass of BaseScraper by its SOURCE_ID.
"""

from __future__ import annotations

import importlib
import pkgutil
import logging
from typing import Type

import scraper.plugins

logger = logging.getLogger(__name__)

# SOURCE_ID → subclass
_REGISTRY: dict[str, Type] = {}
_LOADED = False


def _load_all_plugins() -> None:
    global _LOADED
    if _LOADED:
        return

    # Walk the entire scraper.plugins package tree
    for _finder, name, _ispkg in pkgutil.walk_packages(
        path=scraper.plugins.__path__,
        prefix="scraper.plugins.",
        onerror=lambda n: logger.warning("Could not import %s", n),
    ):
        try:
            importlib.import_module(name)
        except Exception as exc:
            logger.warning("Failed to import plugin module %s: %s", name, exc)

    # Now discover all BaseScraper subclasses
    from scraper.base import BaseScraper

    def _collect(cls):
        for sub in cls.__subclasses__():
            if sub.SOURCE_ID:
                _REGISTRY[sub.SOURCE_ID] = sub
            _collect(sub)

    _collect(BaseScraper)
    _LOADED = True


def get_all() -> dict[str, Type]:
    """Return a mapping of SOURCE_ID → scraper class for all discovered plugins."""
    _load_all_plugins()
    return dict(_REGISTRY)


def get(source_id: str) -> Type | None:
    """Return the scraper class for a given SOURCE_ID, or None."""
    _load_all_plugins()
    return _REGISTRY.get(source_id)


def list_sources() -> list[dict]:
    """Return a sorted list of dicts with source metadata for display."""
    _load_all_plugins()
    return sorted(
        [
            {
                "source_id": cls.SOURCE_ID,
                "tier": cls.TIER,
                "requires_keys": cls.REQUIRES_KEYS,
            }
            for cls in _REGISTRY.values()
        ],
        key=lambda d: (d["tier"], d["source_id"]),
    )
