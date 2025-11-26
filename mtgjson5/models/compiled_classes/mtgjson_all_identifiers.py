"""MTGJSON All Identifiers compiled model for cross-platform ID mappings."""

import logging
from typing import Any

from pydantic import Field

from ...utils import get_all_entities_from_content
from ..mtgjson_base import MTGJsonCompiledModel

LOGGER = logging.getLogger(__name__)


class MtgjsonAllIdentifiersObject(MTGJsonCompiledModel):
    """
    The All Identifiers compiled output mapping UUIDs to card/product data with identifiers.
    """

    all_identifiers_dict: dict[str, Any] = Field(
        default_factory=dict,
        description="A dictionary mapping UUIDs to their corresponding card or product data.",
    )

    def __init__(
        self, all_printings: dict[str, Any] | None = None, **data: Any
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

    def to_json(self) -> dict[str, Any]:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        return self.all_identifiers_dict
