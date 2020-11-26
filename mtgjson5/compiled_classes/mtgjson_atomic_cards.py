"""
MTGJSON AtomicCards Object
"""
import json
import re
from typing import Any, Dict, List

from ..classes import MtgjsonCardObject
from ..consts import OUTPUT_PATH
from ..utils import to_camel_case
from .mtgjson_structures import MtgjsonStructuresObject


class MtgjsonAtomicCardsObject:
    """
    MTGJSON AtomicCards Object
    """

    atomic_cards_dict: Dict[str, List[Dict[str, Any]]]
    __name_regex = re.compile(r"^([^\n]+) \([a-z]\)$")

    def __init__(self, cards_to_parse: List[Dict[str, Any]] = None) -> None:
        """
        Initializer to build up the object
        """
        self.atomic_cards_dict = {}
        self.iterate_all_cards(
            MtgjsonStructuresObject().get_all_compiled_file_names(), cards_to_parse
        )

    def iterate_all_cards(
        self, files_to_ignore: List[str], cards_to_load: List[Dict[str, Any]] = None
    ) -> None:
        """
        Iterate and all all MTGJSON sets to the dictionary
        indexed by file name
        :param files_to_ignore: Files to skip
        :param cards_to_load: Cards to use instead of files
        """
        valid_keys = MtgjsonCardObject().get_atomic_keys()

        if cards_to_load:
            self.update_global_card_list(cards_to_load, valid_keys)
            return

        for set_file in OUTPUT_PATH.glob("*.json"):
            if set_file.stem in files_to_ignore:
                continue

            with set_file.open(encoding="utf-8") as file:
                file_content = json.load(file)

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

            # Strip out the (a), (b) stuff
            values = self.__name_regex.findall(atomic_card["name"])
            card_name = values[0] if values else atomic_card["name"]

            if card_name not in self.atomic_cards_dict.keys():
                self.atomic_cards_dict[card_name] = []

            should_add_card = True
            for card_entry in self.atomic_cards_dict[card_name]:
                if card_entry.get("text") == atomic_card.get("text"):
                    should_add_card = False
                    break

            if should_add_card:
                self.atomic_cards_dict[card_name].append(atomic_card)

            # ForeignData is consumable on all components, but not always
            # included by upstreams. This updates foreignData if necessary
            hold_entry = atomic_card
            if not atomic_card["foreignData"]:
                for entry in self.atomic_cards_dict[card_name]:
                    if entry["foreignData"]:
                        hold_entry = entry
                        break

            for entry in self.atomic_cards_dict[card_name]:
                if entry.get("text") == hold_entry.get("text"):
                    entry["foreignData"] = hold_entry["foreignData"]

    def to_json(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        return self.atomic_cards_dict
