"""
MTGJSON AllIdentifiers Object
"""

import logging
from typing import Any

from ..classes.json_object import JsonObject
from ..utils import get_all_entities_from_content


LOGGER = logging.getLogger(__name__)


class MtgjsonAllIdentifiersObject(JsonObject):
    """
    MTGJSON AllIdentifiers Object
    """

    all_identifiers_dict: dict[str, Any]

    def __init__(self, all_printings: dict[str, Any]) -> None:
        """
        Initialize to build up the object
        """
        self.all_identifiers_dict = {}

        for card in get_all_entities_from_content(all_printings):
            if card["uuid"] in self.all_identifiers_dict:
                LOGGER.error(
                    f"Duplicate MTGJSON UUID {card['uuid']} detected!\n"
                    f"Card 1: {self.all_identifiers_dict[card['uuid']]}\n"
                    f"Card 2: {card}"
                )
                continue

            self.all_identifiers_dict[card["uuid"]] = card

    def to_json(self) -> dict[str, Any]:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        return self.all_identifiers_dict
