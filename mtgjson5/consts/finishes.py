"""
Finish ordering constants.

Defines canonical order for card finishes (nonfoil, foil, etched, etc.).
"""

from __future__ import annotations

from typing import Final


FINISH_ORDER: Final[dict[str, int]] = {
	"nonfoil": 0,
	"foil": 1,
	"etched": 2,
	"signed": 3,
	"other": 4,
}
"""Canonical ordering for card finishes. Lower values sort first."""
