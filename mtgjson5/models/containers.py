"""
Lightweight data containers used across the v2 pipeline.

These are simple dataclasses (not Pydantic models or TypedDicts) for
cross-cutting concerns shared between providers and build code.
"""

from __future__ import annotations

import datetime
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from mtgjson5 import constants
from mtgjson5.mtgjson_config import MtgjsonConfig


@dataclass
class MtgjsonMeta:
    """Build metadata (date + version)."""

    date: str = field(default_factory=lambda: constants.MTGJSON_BUILD_DATE)
    version: str = field(default_factory=lambda: MtgjsonConfig().mtgjson_version)

    def to_json(self) -> dict[str, Any]:
        """convert to JSON format."""
        result: dict[str, Any] = {"date": self.date, "version": self.version}
        for key, value in result.items():
            if isinstance(value, datetime.datetime):
                result[key] = value.strftime("%Y-%m-%d")
        return result


@dataclass
class MtgjsonPriceEntry:
    """Single-provider price entry for one card UUID."""

    source: str
    provider: str
    date: str
    currency: str
    buy_normal: float | None = None
    buy_foil: float | None = None
    buy_etched: float | None = None
    sell_normal: float | None = None
    sell_foil: float | None = None
    sell_etched: float | None = None

    def items(self) -> list[tuple[str, float | None]]:
        """Iterate over all fields as (key, value) pairs."""
        return [
            (key, value)
            for key, value in vars(self).items()
            if not callable(getattr(self, key)) and not key.startswith("__")
        ]

    def to_json(self) -> dict[str, Any]:
        """Convert to nested MTGJSON price format."""
        buy_sell_option: dict[str, Any] = {
            "buylist": defaultdict(dict),
            "retail": defaultdict(dict),
            "currency": self.currency,
        }

        if self.buy_normal is not None:
            buy_sell_option["buylist"]["normal"][self.date] = self.buy_normal
        if self.buy_foil is not None:
            buy_sell_option["buylist"]["foil"][self.date] = self.buy_foil
        if self.buy_etched is not None:
            buy_sell_option["buylist"]["etched"][self.date] = self.buy_etched
        if self.sell_normal is not None:
            buy_sell_option["retail"]["normal"][self.date] = self.sell_normal
        if self.sell_foil is not None:
            buy_sell_option["retail"]["foil"][self.date] = self.sell_foil
        if self.sell_etched is not None:
            buy_sell_option["retail"]["etched"][self.date] = self.sell_etched

        return {self.source: {self.provider: buy_sell_option}}
