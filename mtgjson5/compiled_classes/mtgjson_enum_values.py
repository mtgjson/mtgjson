"""
MTGJSON EnumValues Object
"""
import json
import logging
import pathlib
from typing import Any, Dict, List, Union

from ..compiled_classes.mtgjson_all_printings import MtgjsonAllPrintingsObject
from ..consts import OUTPUT_PATH
from ..utils import sort_internal_lists
from .mtgjson_structures import MtgjsonStructuresObject

LOGGER = logging.getLogger(__name__)


class MtgjsonEnumValuesObject:
    """
    MTGJSON EnumValues Object
    """

    attr_value_dict: Dict[str, Union[Dict[str, List[str]], List[str]]]

    set_key_struct = {
        "card": [
            "availability",
            "borderColor",
            "colorIdentity",
            "colorIndicator",
            "colors",
            "duelDeck",
            "frameEffects",
            "frameVersion",
            "layout",
            "promoTypes",
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

    deck_key_struct = {"deck": ["type"]}

    def __init__(self) -> None:
        """
        Initializer to build the internal mapping
        """
        self.attr_value_dict = {}

        set_and_cards = self.construct_set_and_card_enums(
            MtgjsonAllPrintingsObject().to_json()
        )
        self.attr_value_dict.update(set_and_cards)

        decks = self.construct_deck_enums(OUTPUT_PATH.joinpath("decks"))
        self.attr_value_dict.update(decks)

        # Load in pre-generated Keywords content
        keywords = OUTPUT_PATH.joinpath(MtgjsonStructuresObject().key_words + ".json")
        if not keywords.is_file():
            LOGGER.warning(f"Unable to find {keywords}")
        else:
            with keywords.open(encoding="utf-8") as file:
                content = json.load(file).get("data", {})
            self.attr_value_dict.update({"keywords": content})

    def construct_deck_enums(self, decks_directory: pathlib.Path) -> Dict[str, Any]:
        """
        Given Decks Path, compile enums based on the types found in the files
        :param decks_directory: Path to the decks/ output directory
        :return Sorted list of enum options for each key
        """
        type_map: Dict[str, Any] = {}
        for object_name, object_values in self.deck_key_struct.items():
            type_map[object_name] = dict()
            for object_field_name in object_values:
                type_map[object_name][object_field_name] = set()

        for deck in decks_directory.glob("**/*.json"):
            with deck.open(encoding="utf-8") as file:
                content = json.load(file).get("data", {})

            for key in content.keys():
                if key in self.deck_key_struct["deck"]:
                    type_map["deck"][key].add(content[key])

        return dict(sort_internal_lists(type_map))

    def construct_set_and_card_enums(
        self, all_printing_content: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Given AllPrintings, compile enums based on the types found in the file
        :param all_printing_content: AllPrintings internally
        :return Sorted list of enum options for each key
        """
        type_map: Dict[str, Any] = {}
        for object_name, object_values in self.set_key_struct.items():
            type_map[object_name] = dict()
            for object_field_name in object_values:
                type_map[object_name][object_field_name] = set()

        for set_contents in all_printing_content.values():
            for set_contents_key in set_contents.keys():
                if set_contents_key in self.set_key_struct["set"]:
                    type_map["set"][set_contents_key].add(
                        set_contents.get(set_contents_key)
                    )

            match_keys = set(self.set_key_struct["card"]).union(
                set(self.set_key_struct.keys())
            )
            for card in set_contents.get("cards", []) + set_contents.get("tokens", []):
                for card_key in card.keys():
                    if card_key not in match_keys:
                        continue

                    # Get the value when actually needed
                    card_value = card[card_key]

                    # For Dicts, we just enum the keys
                    if isinstance(card_value, dict):
                        for value in card_value.keys():
                            type_map["card"][card_key].add(value)
                        continue

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
                        for attribute in self.set_key_struct.get(card_key, []):
                            type_map[card_key][attribute].add(single_value[attribute])

        return dict(sort_internal_lists(type_map))

    def to_json(self) -> Dict[str, Union[Dict[str, List[str]], List[str]]]:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        return self.attr_value_dict
