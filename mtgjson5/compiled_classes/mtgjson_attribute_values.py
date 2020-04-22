"""
MTGJSON AttributeValues Object
"""
from typing import Any, Dict, List, Union

from ..compiled_classes.mtgjson_all_printings import MtgjsonAllPrintingsObject
from ..utils import sort_internal_lists


class MtgjsonAttributeValuesObject:
    """
    MTGJSON AttributeValues Object
    """

    attr_value_dict: Dict[str, Union[Dict[str, List[str]], List[str]]]

    __included_set_keys = ["code"]

    __included_card_keys = [
        "borderColor",
        "colorIdentity",
        "colorIndicator",
        "colors",
        "duelDeck",
        "frameEffects",
        "frameVersion",
        "layout",
        "rarity",
        "side",
        "subtypes",
        "supertypes",
        "types",
        "watermark",
    ]

    __included_sub_card_keys = {"foreignData": "language"}

    def __init__(self) -> None:
        self.construct_internal_enums(MtgjsonAllPrintingsObject().to_json())

    def construct_internal_enums(self, all_printings_content: Dict[str, Any]) -> None:
        """
        Given AllPrintings, compile enums based on the types found in the file
        :param all_printings_content: AllPrintings internally
        """
        type_map: Dict[str, Any] = {
            "set": {},
            "card": {},
        }

        full_key_options = self.__included_card_keys + list(
            self.__included_sub_card_keys.keys()
        )
        for set_code, set_contents in all_printings_content.items():
            for find_set_key in self.__included_set_keys:
                if find_set_key not in type_map["set"].keys():
                    type_map["set"][find_set_key] = set()
                type_map["set"][find_set_key].add(set_code)

            for card in set_contents.get("cards", []) + set_contents.get("tokens", []):
                for card_key, card_value in card.items():
                    # Key not apart of the operation
                    if card_key not in full_key_options:
                        continue

                    # Determine if the key has sub-key or not
                    if card_key not in type_map["card"].keys():
                        type_map["card"][card_key] = (
                            dict()
                            if card_key in self.__included_sub_card_keys.keys()
                            else set()
                        )

                    # A string addition doesn't need more processing
                    if not isinstance(card_value, list):
                        type_map["card"][card_key].add(card_value)
                        continue

                    # List processing for operations
                    for value in card_value:
                        if not isinstance(value, dict):
                            type_map["card"][card_key].add(value)
                            continue

                        field_name = self.__included_sub_card_keys[card_key]
                        if field_name not in type_map["card"][card_key].keys():
                            type_map["card"][card_key][field_name] = set()

                        type_map["card"][card_key][field_name].add(value[field_name])

        self.attr_value_dict = sort_internal_lists(type_map)

    def to_json(self) -> Dict[str, Union[Dict[str, List[str]], List[str]]]:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        return self.attr_value_dict
