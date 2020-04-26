"""
MTGJSON EnumValues Object
"""
from typing import Any, Dict, List, Union

from ..compiled_classes.mtgjson_all_printings import MtgjsonAllPrintingsObject
from ..utils import sort_internal_lists


class MtgjsonEnumValuesObject:
    """
    MTGJSON EnumValues Object
    """

    attr_value_dict: Dict[str, Union[Dict[str, List[str]], List[str]]]

    key_struct = {
        "card": [
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
        ],
        "set": ["type"],
        "foreignData": ["language"],
    }

    def __init__(self) -> None:
        """
        Initializer to build the internal mapping
        """
        self.construct_internal_enums(MtgjsonAllPrintingsObject().to_json())

    def construct_internal_enums(self, all_printing_content: Dict[str, Any]) -> None:
        """
        Given AllPrintings, compile enums based on the types found in the file
        :param all_printing_content: AllPrintings internally
        """
        type_map: Dict[str, Any] = {}
        for object_name, object_values in self.key_struct.items():
            type_map[object_name] = dict()
            for object_field_name in object_values:
                type_map[object_name][object_field_name] = set()

        for set_code, set_contents in all_printing_content.items():
            for set_contents_key in set_contents.keys():
                if set_contents_key in self.key_struct["set"]:
                    type_map["set"][set_contents_key].add(set_code)

            match_keys = set(self.key_struct["card"]).union(set(self.key_struct.keys()))
            for card in set_contents.get("cards", []) + set_contents.get("tokens", []):
                for card_key in card.keys():
                    if card_key not in match_keys:
                        continue

                    # Get the value when actually needed
                    card_value = card[card_key]

                    # String, Integer, etc can be added as-is
                    if not isinstance(card_value, list):
                        type_map["card"][card_key].add(card_value)
                        continue

                    for single_value in card_value:
                        # Iterating a non-dict is fine
                        if not isinstance(single_value, dict):
                            type_map["card"][card_key].add(single_value)
                            continue

                        # Internal attributes are sometimes added
                        for attribute in self.key_struct.get(card_key, []):
                            type_map[card_key][attribute].add(single_value[attribute])

        self.attr_value_dict = sort_internal_lists(type_map)

    def to_json(self) -> Dict[str, Union[Dict[str, List[str]], List[str]]]:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        return self.attr_value_dict
