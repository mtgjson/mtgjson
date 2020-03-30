"""
MTGJSON AllCards container
"""
from typing import Any, Dict, List

import simplejson as json

from ..classes import MtgjsonCardObject
from ..consts import OUTPUT_PATH
from ..utils import to_camel_case
from .mtgjson_structures import MtgjsonStructuresObject


class MtgjsonAllCardsObject:
    """
    AllCards container
    """

    all_cards_dict: Dict[str, List[Dict[str, Any]]]

    def __init__(self, cards_to_parse: Dict[str, Dict[str, Any]] = None) -> None:
        self.all_cards_dict = {}
        self.iterate_all_cards(
            MtgjsonStructuresObject().get_all_compiled_file_names(), cards_to_parse
        )

    def iterate_all_cards(
        self,
        files_to_ignore: List[str],
        cards_to_load: Dict[str, Dict[str, Any]] = None,
    ) -> None:
        """
        Iterate and all all MTGJSON sets to the dictionary
        indexed by file name
        :param files_to_ignore: Files to skip
        :param cards_to_load: Cards to use instead of files
        """
        valid_keys = MtgjsonCardObject().get_atomic_keys()

        if cards_to_load:
            self.update_global_card_list(list(cards_to_load.values()), valid_keys)
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

            for foreign_data in atomic_card.get("foreignData", {}):
                foreign_data.pop("multiverseId", None)

            if atomic_card["name"] not in self.all_cards_dict.keys():
                self.all_cards_dict[atomic_card["name"]] = []

            should_add_card = True
            for card_entry in self.all_cards_dict[atomic_card["name"]]:
                if card_entry.get("purchaseUrls") == atomic_card.get("purchase_urls"):
                    should_add_card = False
                    break

            if should_add_card:
                self.all_cards_dict[atomic_card["name"]].append(atomic_card)

    def for_json(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Support json.dumps()
        :return: JSON serialized object
        """
        return self.all_cards_dict
