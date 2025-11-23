import copy
import json
import logging
from typing import Any, Dict, Optional

from ..classes import MtgjsonCardObject
from ..constants import RESOURCE_PATH

LOGGER = logging.getLogger(__name__)


class EnrichmentProvider:
    """
    Loads mtgjson5/resources/card_enrichment.json and provides lookup helpers.

    Lookup order attempted:
      1) by_uuid
      2) {SET}->{collector_number}|{name}
      3) {SET}->{collector_number}
    """

    def __init__(self) -> None:
        resource = RESOURCE_PATH.joinpath("card_enrichment.json")
        try:
            with resource.open(encoding="utf-8") as fp:
                self._data: Dict[str, Any] = json.load(fp)
            uuid_count = len(self._data.get("by_uuid", {}))
            set_count = len([k for k in self._data.keys() if k != "by_uuid"])
            LOGGER.info(
                f"Loaded enrichment data: {uuid_count} UUID entries, "
                f"{set_count} set-specific entries"
            )
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
        Get enrichment data for a card using multiple lookup strategies.
        This approach assumes that UUIDs are stable, but provides fallbacks.
        It also assumes that the enrichment is specific to a UUID, and a
        future improvement may be to update the lookup to UUID and optionally
        include language and/or side. The fallback would also need to include
        these additional key components.
        :param card: MTGJSON card object to enrich
        :return: Enrichment data dictionary or None if not found
        """
        # 1) by_uuid
        by_uuid = self._data.get("by_uuid", {})
        if by_uuid and getattr(card, "uuid", None) in by_uuid:
            entry = by_uuid[card.uuid]
            if self._validate_enrichment_entry(entry, f"UUID:{card.uuid}"):
                # return a copy to avoid accidental mutation
                return copy.deepcopy(entry)
            return None

        # 2) Fall-back if uuids ever change

        # set-scoped block
        set_block = self._data.get(getattr(card, "set_code", ""), {})
        if not set_block:
            return None

        # Try collectorNumber|Name
        number = getattr(card, "number", None)
        name = getattr(card, "name", None)
        if number and name:
            key = f"{number}|{name}"
            if key in set_block:
                entry = set_block[key]
                context = f"{card.set_code}:{key}"
                if self._validate_enrichment_entry(entry, context):
                    return copy.deepcopy(entry)
                return None

        # Try just collector number
        if number and number in set_block:
            entry = set_block[number]
            context = f"{card.set_code}:{number}"
            if self._validate_enrichment_entry(entry, context):
                return copy.deepcopy(entry)
            return None

        return None
