import copy
import json
import logging
from typing import Any, Dict, Optional

from singleton_decorator import singleton

from ..classes import MtgjsonCardObject
from ..constants import RESOURCE_PATH

LOGGER = logging.getLogger(__name__)


@singleton
class EnrichmentProvider:
    """
    Loads mtgjson5/resources/card_enrichment.json and provides lookup helpers.

    Lookup key format: {SET}->{collector_number}|{name}
    """

    def __init__(self) -> None:
        resource = RESOURCE_PATH.joinpath("card_enrichment.json")
        try:
            with resource.open(encoding="utf-8") as fp:
                self._data: Dict[str, Any] = json.load(fp)
            set_count = len(self._data.keys())
            card_count = sum(len(entries) for entries in self._data.values())
            LOGGER.info(f"Loaded enrichment data: {card_count} cards across {set_count} sets")
        except FileNotFoundError:
            LOGGER.warning(
                "card_enrichment.json not found, card enrichment disabled"
            )
            self._data = {}
        except json.JSONDecodeError as e:
            LOGGER.error(f"Malformed card_enrichment.json: {e}")
            self._data = {}

    def _validate_enrichment_entry(
        self, entry: Dict[str, Any], context: str
    ) -> bool:
        """
        Validate enrichment entry has correct structure.
        :param entry: Enrichment data dictionary
        :param context: Context string for logging (e.g., "UUID:abc123")
        :return: True if valid, False otherwise
        """
        if "promo_types" in entry:
            if not isinstance(entry["promo_types"], list):
                LOGGER.warning(
                    f"Invalid promo_types in {context}: expected list, got {type(entry['promo_types']).__name__}"
                )
                return False
            if not all(isinstance(pt, str) for pt in entry["promo_types"]):
                LOGGER.warning(
                    f"Invalid promo_types in {context}: expected list of strings"
                )
                return False
        return True

    def _make_card_key(self, card: MtgjsonCardObject) -> str:
        """
        Construct the lookup key for a card.
        :param card: MTGJSON card object
        :return: Lookup key in format "{number}|{name}"
        """
        return f"{card.number}|{card.name}"

    def get_enrichment_for_set(self, set_code: str) -> Optional[Dict[str, Dict[str, Any]]]:
        """
        Get all enrichment data for a given set code.
        :param set_code: Set code to look up
        :return: Dictionary of enrichment data keyed by "{number}|{name}", or None if not found
        """
        return self._data.get(set_code, None)

    def get_enrichment_from_set_data(
        self, set_enrichment: Dict[str, Dict[str, Any]], card: MtgjsonCardObject
    ) -> Optional[Dict[str, Any]]:
        """
        Get enrichment data for a card from already-fetched set enrichment data.
        :param set_enrichment: Set-level enrichment dictionary from get_enrichment_for_set()
        :param card: MTGJSON card object to enrich
        :return: Enrichment data dictionary or None if not found
        """
        key = self._make_card_key(card)
        return set_enrichment.get(key)

    def get_enrichment_for_card(
        self, card: MtgjsonCardObject
    ) -> Optional[Dict[str, Any]]:
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
            key = self._make_card_key(card)
            context = f"{card.set_code}:{key}"
            if self._validate_enrichment_entry(enrichment, context):
                return copy.deepcopy(enrichment)

        return None
