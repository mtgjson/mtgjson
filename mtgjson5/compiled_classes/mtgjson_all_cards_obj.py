"""
MTGJSON AllCards container
"""
from typing import Any, Dict, List

import simplejson as json

from ..classes import MtgjsonCardObject
from ..consts import OUTPUT_PATH
from ..utils import to_camel_case
from .mtgjson_structures_obj import MtgjsonStructuresObject


class MtgjsonAllCardsObject:
    """
    AllCards container
    """

    all_cards_dict: Dict[str, Dict[str, Any]]

    def __init__(self) -> None:
        self.all_cards_dict = {}
        self.iterate_all_cards(MtgjsonStructuresObject().get_all_compiled_file_names())

    def iterate_all_cards(self, files_to_ignore: List[str]) -> None:
        """
        Iterate and all all MTGJSON sets to the dictionary
        indexed by file name
        :param files_to_ignore: Files to skip
        """
        valid_keys = MtgjsonCardObject().get_atomic_keys()

        for set_file in OUTPUT_PATH.glob("*.json"):
            if set_file.stem in files_to_ignore:
                continue

            with set_file.open(encoding="utf-8") as file:
                file_content = json.load(file)

            for card in file_content["cards"]:
                if card["name"] in self.all_cards_dict.keys():
                    continue

                atomic_card = {key: card.get(to_camel_case(key)) for key in valid_keys}
                for foreign_data in atomic_card.get("foreignData", []):
                    foreign_data.pop("multiverseId")

                self.all_cards_dict[atomic_card["name"]] = atomic_card

    def for_json(self) -> Dict[str, Dict[str, Any]]:
        """
        Support json.dumps()
        :return: JSON serialized object
        """
        return self.all_cards_dict
