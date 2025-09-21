"""
MTGJSON AtomicCards Object
"""

import json
import re
from collections import defaultdict
from typing import Any, Dict, List, Optional

from ..classes import MtgjsonCardObject
from ..classes.json_object import JsonObject
from ..mtgjson_config import MtgjsonConfig
from ..utils import to_camel_case
from .mtgjson_structures import MtgjsonStructuresObject


class MtgjsonAtomicCardsObject(JsonObject):
    """
    MTGJSON AtomicCards Object
    """

    atomic_cards_dict: Dict[str, List[Dict[str, Any]]]
    __name_regex = re.compile(r"^([^\n]+) \([a-z]\)$")

    def __init__(self, cards_to_parse: Optional[List[Dict[str, Any]]] = None) -> None:
        """
        Initializer to build up the object
        """
        self.atomic_cards_dict = defaultdict(list)
        self.iterate_all_cards(
            MtgjsonStructuresObject().get_all_compiled_file_names(), cards_to_parse
        )

    def iterate_all_cards(
        self,
        files_to_ignore: List[str],
        cards_to_load: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        """
        Iterate all MTGJSON sets in the dictionary
        indexed by file name
        :param files_to_ignore: Files to skip
        :param cards_to_load: Cards to use instead of files
        """
        valid_keys = MtgjsonCardObject().get_atomic_keys()

        if cards_to_load:
            self.update_global_card_list(cards_to_load, valid_keys)
            return

        for set_file in MtgjsonConfig().output_path.glob("*.json"):
            if set_file.stem in files_to_ignore:
                continue

            with set_file.open(encoding="utf-8") as file:
                file_content = json.load(file)

            valid_cards = file_content.get("data", {}).get("cards", [])

            # Workaround for Dungeons so they can be included
            dungeons = [
                token
                for token in file_content.get("data", {}).get("tokens", [])
                if token.get("type") == "Dungeon"
            ]
            for dungeon in dungeons:
                dungeon.update(
                    {
                        "legalities": {},
                        "purchaseUrls": {},
                        "rulings": [],
                    }
                )

            valid_cards.extend(dungeons)

            self.update_global_card_list(
                file_content.get("data", {}).get("cards", []), valid_keys
            )

    def update_global_card_list(
        self, card_list: List[Dict[str, Any]], valid_keys: List[str]
    ) -> None:
        """
        Update the global registrar for each card in the card list, using
        only the valid_key attributes
        :param card_list: Cards to update with
        :param valid_keys: Keys to use per card
        """
        for card in card_list:
            atomic_card: Dict[str, Any] = {
                to_camel_case(key): card.get(to_camel_case(key))
                for key in valid_keys
                if card.get(to_camel_case(key)) is not None
            }

            # Strip out non-atomic keys from identifiers
            # Only the Oracle ID is atomic
            if "scryfallOracleId" in atomic_card.get("identifiers", []):
                atomic_card["identifiers"] = {
                    "scryfallOracleId": atomic_card["identifiers"]["scryfallOracleId"]
                }

            for foreign_data in atomic_card.get("foreignData", {}):
                foreign_data.pop("multiverseId", None)
                foreign_data.pop("identifiers", None)
                foreign_data.pop("uuid", None)

            # Strip out the (a), (b) stuff
            values = self.__name_regex.findall(atomic_card["name"])
            card_name = values[0] if values else atomic_card["name"]

            should_add_card = True
            for card_entry in self.atomic_cards_dict[card_name]:
                if card_entry.get("text") == atomic_card.get("text"):
                    # Some printings might not have foreign data or legalities, so we ensure they're established
                    for field_to_copy in ["foreignData", "legalities"]:
                        if not card_entry.get(field_to_copy):
                            card_entry[field_to_copy] = atomic_card.get(field_to_copy)

                    # If the newly added card is the original printing, lets set it
                    if not card.get("isReprint"):
                        card_entry["firstPrinting"] = card.get("setCode")
                    should_add_card = False
                    break

            if should_add_card:
                # Sometimes, the first card added _is_ the original printing
                if not card.get("isReprint"):
                    atomic_card["firstPrinting"] = card.get("setCode")

                self.atomic_cards_dict[card_name].append(atomic_card)
                self.atomic_cards_dict[card_name].sort(key=lambda x: x.get("side", "z"))

            # ForeignData is consumable on all components, but not always
            # included by upstreams. This updates foreignData if necessary
            hold_entry = atomic_card
            if not atomic_card.get("foreignData"):
                for entry in self.atomic_cards_dict[card_name]:
                    if entry.get("foreignData"):
                        hold_entry = entry
                        break

            for entry in self.atomic_cards_dict[card_name]:
                if entry.get("text") == hold_entry.get("text"):
                    entry["foreignData"] = hold_entry.get("foreignData", [])

    def to_json(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        return self.atomic_cards_dict
