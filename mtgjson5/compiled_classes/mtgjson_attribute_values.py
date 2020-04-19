"""
MTGJSON AttributeValues container
"""
from typing import Any, Dict, List, Union

from ..compiled_classes.mtgjson_all_printings import MtgjsonAllPrintingsObject
from ..utils import sort_internal_lists


class MtgjsonAttributeValuesObject:
    """
    AttributeValues container
    """

    key_values_dict: Dict[str, Union[Dict[str, List[str]], List[str]]]

    _set_keys = ["code"]
    _card_keys = [
        "borderColor",
        "colorIdentity",
        "colorIndicator",
        "colors",
        "duelDeck",
        "frameEffects",
        "frameVersion",
        "rarity",
        "layout",
        "side",
        "watermark",
    ]
    _expanded_card_keys = {"foreignData": "language"}

    def __init__(self) -> None:
        self.construct_internal_enums(MtgjsonAllPrintingsObject().for_json())

    def construct_internal_enums(self, all_printings_content: Dict[str, Any]) -> None:
        """
        Given AllPrintings, compile enums based on the types found in the file
        :param all_printings_content: AllPrintings internally
        """
        type_map: Dict[str, Any] = {
            "set": {},
            "card": {},
        }

        full_key_options = self._card_keys + list(self._expanded_card_keys.keys())
        for set_code, set_contents in all_printings_content.items():
            for find_set_key in self._set_keys:
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
                            if card_key in self._expanded_card_keys.keys()
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

                        field_name = self._expanded_card_keys[card_key]
                        if field_name not in type_map["card"][card_key].keys():
                            type_map["card"][card_key][field_name] = set()

                        type_map["card"][card_key][field_name].add(value[field_name])

        self.key_values_dict = sort_internal_lists(type_map)

    def for_json(self) -> Dict[str, Union[Dict[str, List[str]], List[str]]]:
        """
        Support json.dumps()
        :return: JSON serialized object
        """
        return self.key_values_dict
