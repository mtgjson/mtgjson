"""MTGJSON All Identifiers compiled model for cross-platform ID mappings."""

import logging
from typing import Any, Dict, Optional

from pydantic import Field

from ...utils import get_all_entities_from_content
from ..mtgjson_base import MTGJsonCompiledModel

LOGGER = logging.getLogger(__name__)


class MtgjsonAllIdentifiersObject(MTGJsonCompiledModel):
    """
    MTGJSON AllIdentifiers Object
    """

    all_identifiers_dict: Dict[str, Any] = Field(default_factory=dict)

    def __init__(
        self, all_printings: Optional[Dict[str, Any]] = None, **data: Any
    ) -> None:
        """
        Initialize to build up the object
        """
        super().__init__(**data)

        if all_printings is not None:
            for card in get_all_entities_from_content(all_printings):
                if card["uuid"] in self.all_identifiers_dict:
                    LOGGER.error(
                        f"Duplicate MTGJSON UUID {card['uuid']} detected!\n"
                        f"Card 1: {self.all_identifiers_dict[card['uuid']]}\n"
                        f"Card 2: {card}"
                    )
                    continue

                self.all_identifiers_dict[card["uuid"]] = card

    def to_json(self) -> Dict[str, Any]:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        return self.all_identifiers_dict
