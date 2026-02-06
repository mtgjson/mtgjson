"""
Enrichment Provider for MTGJSON
"""

import copy
import json
import logging
from pathlib import Path
from typing import Any

from singleton_decorator import singleton

from ..classes import MtgjsonCardObject
from ..constants import RESOURCE_PATH

LOGGER = logging.getLogger(__name__)


@singleton
class EnrichmentProvider:
    """
    Loads mtgjson5/resources/card_enrichment.json and provides lookup helpers.

    Lookup key format: {SET}->{collector_number}
    """

    _data: dict[str, Any]

    def __init__(self, resource_path: Path | None = None) -> None:
        if resource_path is None:
            resource_path = RESOURCE_PATH
        resource = resource_path.joinpath("card_enrichment.json")
        try:
            with resource.open(encoding="utf-8") as fp:
                self._data: dict[str, Any] = json.load(fp)
            set_count = len(self._data)
            card_count = sum(len(entries) for entries in self._data.values())
            LOGGER.info(f"Loaded enrichment data: {card_count} cards across {set_count} sets")
        except FileNotFoundError:
            LOGGER.warning("card_enrichment.json not found, card enrichment disabled")
            self._data = {}
        except json.JSONDecodeError as e:
            LOGGER.error(f"Malformed card_enrichment.json: {e}")
            self._data = {}
        except OSError as e:
            LOGGER.error(f"Error reading card_enrichment.json: {e}")
            self._data = {}

    def _make_card_key(self, card: MtgjsonCardObject) -> str:
        """
        Construct the lookup key for a card.
        :param card: MTGJSON card object
        :return: Lookup key in format "{number}"
        """
        return card.number

    def get_enrichment_for_set(self, set_code: str) -> dict[str, dict[str, Any]] | None:
        """
        Get all enrichment data for a given set code.
        :param set_code: Set code to look up
        :return: Dictionary of enrichment data keyed by "{number}", or None if not found
        """
        return self._data.get(set_code)

    def get_enrichment_from_set_data(
        self, set_enrichment: dict[str, dict[str, Any]], card: MtgjsonCardObject
    ) -> dict[str, Any] | None:
        """
        Get enrichment data for a card from already-fetched set enrichment data.
        :param set_enrichment: Set-level enrichment dictionary from get_enrichment_for_set()
        :param card: MTGJSON card object to enrich
        :return: Enrichment data dictionary or None if not found
        """
        key = self._make_card_key(card)
        entry = set_enrichment.get(key)
        if not entry:
            return None

        expected_name = entry.get("name")
        if not expected_name:
            LOGGER.warning(f"Enrichment entry for {card.set_code}:{card.number} missing 'name' field")
            return None

        if card.name.lower() == expected_name.lower():
            return entry.get("enrichment")

        LOGGER.warning(
            f"Enrichment name mismatch for {card.set_code}:{card.number}: "
            f"Card name '{card.name}' does not match expected name '{expected_name}'"
        )
        return None

    def get_enrichment_for_card(self, card: MtgjsonCardObject) -> dict[str, Any] | None:
        """
        Get enrichment data for a card using set-based lookup strategies.
        :param card: MTGJSON card object to enrich
        :return: Enrichment data dictionary or None if not found
        """
        set_block = self._data.get(card.set_code, None)
        if not set_block:
            return None

        enrichment = self.get_enrichment_from_set_data(set_block, card)
        if enrichment:
            return copy.deepcopy(enrichment)

        return None
