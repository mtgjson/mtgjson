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
            LOGGER.info(f"Loaded enrichment data: {set_count} set-specific entries")
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

    def get_enrichment_for_card(
        self, card: MtgjsonCardObject
    ) -> Optional[Dict[str, Any]]:
        """
        Get enrichment data for a card using set-based lookup strategies.
        :param card: MTGJSON card object to enrich
        :return: Enrichment data dictionary or None if not found
        """
        set_block = self._data.get(card.set_code, {})
        number = getattr(card, "number", None)
        name = getattr(card, "name", None)
        if not set_block or not number or not name:
            return None

        key = f"{number}|{name}"
        if key in set_block:
            context = f"{card.set_code}:{key}"
            if self._validate_enrichment_entry(set_block[key], context):
                return copy.deepcopy(set_block[key])

        return None
