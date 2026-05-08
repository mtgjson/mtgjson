"""
CardSphere provider - loads card ID mappings from a local resource file.

Reads cardsphere_data.json (pre-built from the CardSphere database export)
and produces a DataFrame:

  cards_df: scryfallId -> cardsphereId / cardsphereFoilId / cardsphereAlternativeFoilId
"""

import logging

import polars as pl

from mtgjson5 import constants

LOGGER = logging.getLogger(__name__)

# Schema for the card-level mapping
CARD_SCHEMA = {
    "scryfallId": pl.String,
    "cardsphereId": pl.String,
    "cardsphereFoilId": pl.String,
    "cardsphereAlternativeFoilId": pl.String,
}

RESOURCE_FILENAME = "cardsphere_data.json"


class CardSphereProvider:
    """Provider for CardSphere card ID data loaded from a local resource file."""

    def __init__(self) -> None:
        self._cards_df: pl.DataFrame | None = None

    def load(self) -> pl.DataFrame:
        """Load CardSphere data from the resource file.

        Returns:
            cards_df mapping scryfallId to cardsphereId/cardsphereFoilId/cardsphereAlternativeFoilId.
        """
        resource_path = constants.RESOURCE_PATH / RESOURCE_FILENAME

        if not resource_path.exists():
            LOGGER.warning(f"CardSphere resource file not found: {resource_path}")
            self._cards_df = self._empty_cards_df()
            return self._cards_df

        cards_df = pl.read_json(resource_path, schema=CARD_SCHEMA)

        if cards_df.is_empty():
            LOGGER.warning("CardSphere resource file is empty")
            self._cards_df = self._empty_cards_df()
            return self._cards_df

        self._cards_df = cards_df
        LOGGER.info(f"Loaded CardSphere mapping: {len(cards_df):,} unique cards")

        return cards_df

    @staticmethod
    def _empty_cards_df() -> pl.DataFrame:
        return pl.DataFrame(schema=CARD_SCHEMA)

    @property
    def cards_df(self) -> pl.DataFrame | None:
        return self._cards_df
