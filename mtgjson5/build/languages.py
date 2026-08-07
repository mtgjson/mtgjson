"""Set language list construction.

A set's ``languages`` list is derived from the languages present in its cards'
``foreignData``. That data only covers printings our upstream providers track,
so pre-2004 Asian-language printings (and Italian 7th Edition) are absent even
though Wizards of the Coast documented them on the Card Set Archive product
information pages. ``set_language_additions.json`` records those printings so
the per-set list matches the official one.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from functools import lru_cache

from mtgjson5 import constants
from mtgjson5.utils import LOGGER

RESOURCE_NAME = "set_language_additions.json"


@lru_cache(maxsize=1)
def get_set_language_additions() -> dict[str, tuple[str, ...]]:
    """Load the set code -> extra language names mapping, keyed by upper-case set code."""
    file_path = constants.RESOURCE_PATH / RESOURCE_NAME
    if not file_path.exists():
        LOGGER.warning(f"Resource file not found: {file_path}")
        return {}
    with file_path.open("rb") as f:
        raw = json.loads(f.read())
    return {code.upper(): tuple(langs) for code, langs in raw.items()}


def merge_set_languages(set_code: str | None, languages: Iterable[str]) -> list[str]:
    """Combine languages found in card data with documented printings for the set."""
    result = {"English", *languages}
    if set_code:
        result.update(get_set_language_additions().get(set_code.upper(), ()))
    return sorted(result)
