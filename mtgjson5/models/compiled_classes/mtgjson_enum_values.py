"""MTGJSON Enum Values compiled model for enumerated field values."""

from __future__ import annotations

import json
import logging
import pathlib
from typing import Any, ClassVar

from pydantic import Field

from ... import mtgjson_config, providers, utils
from ..mtgjson_base import MTGJsonCompiledModel
from .mtgjson_all_printings import MtgjsonAllPrintingsObject
from .mtgjson_structures import MtgjsonStructuresObject

MtgjsonConfig = mtgjson_config.MtgjsonConfig
tcgplayer = providers.tcgplayer
sort_internal_lists = utils.sort_internal_lists

LOGGER = logging.getLogger(__name__)


class MtgjsonEnumValuesObject(MTGJsonCompiledModel):
    """
    The Enum Values compiled output containing all possible values for enumerated fields.
    """

    attr_value_dict: dict[str, dict[str, list[str]] | list[str]] = Field(
        default_factory=dict,
        description="A dictionary mapping field names to their possible enumerated values.",
    )

    set_key_struct: ClassVar[dict[str, list[str] | dict[str, list[str]]]] = {
        "card": [
            "availability",
            "boosterTypes",
            "borderColor",
            "colorIdentity",
            "colorIndicator",
            "colors",
            "duelDeck",
            "finishes",
            "frameEffects",
            "frameVersion",
            "language",
            "layout",
            "promoTypes",
            "rarity",
            "securityStamp",
            "side",
            "subtypes",
            "supertypes",
            "types",
            "watermark",
        ],
        "foreignData": ["language"],
        "set": ["type", "languages"],
        "setInner": {
            "sealedProduct": ["category", "subtype"],
        },
    }

    deck_key_struct: ClassVar[dict[str, list[str]]] = {"deck": ["type"]}

    def __init__(self, **kwargs: Any) -> None:
        """
        Initializer to build the internal mapping
        """
        super().__init__(**kwargs)
        self.attr_value_dict = {}

        set_and_cards = self.construct_set_and_card_enums(
            MtgjsonAllPrintingsObject().to_json()
        )
        self.attr_value_dict.update(set_and_cards)

        decks = self.construct_deck_enums(MtgjsonConfig().output_path.joinpath("decks"))
        self.attr_value_dict.update(decks)

        # Load in pre-generated Keywords content
        keywords = MtgjsonConfig().output_path.joinpath(
            MtgjsonStructuresObject().key_words + ".json"
        )
        if not keywords.is_file():
            LOGGER.warning(f"Unable to find {keywords}")
        else:
            with keywords.open(encoding="utf-8") as file:
                content = json.load(file).get("data", {})
            self.attr_value_dict.update({"keywords": content})

        # Add TCGPlayer SKUs
        self.attr_value_dict.update(
            {
                "tcgplayerSkus": {
                    "condition": [key.name for key in tcgplayer.CardCondition],
                    "finishes": [key.name for key in tcgplayer.CardFinish],
                    "language": [key.name for key in tcgplayer.CardLanguage],
                    "printing": [key.name for key in tcgplayer.CardPrinting],
                }
            }
        )

    def construct_deck_enums(self, decks_directory: pathlib.Path) -> dict[str, Any]:
        """
        Given Decks Path, compile enums based on the types found in the files
        :param decks_directory: Path to the decks/ output directory
        :return Sorted list of enum options for each key
        """
        type_map: dict[str, Any] = {}
        for object_name, object_values in self.deck_key_struct.items():
            type_map[object_name] = {}
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
        self, all_printing_content: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Given AllPrintings, compile enums based on the types found in the file
        :param all_printing_content: AllPrintings internally
        :return Sorted list of enum options for each key
        """
        type_map: dict[str, Any] = {}
        for object_name, object_values in self.set_key_struct.items():
            type_map[object_name] = {}
            for object_field_name in object_values:
                type_map[object_name][object_field_name] = set()

        for set_contents in all_printing_content.values():
            for set_contents_key in set_contents.keys():
                if set_contents_key in self.set_key_struct["set"]:
                    value = set_contents.get(set_contents_key)
                    if isinstance(value, list):
                        type_map["set"][set_contents_key].update(value)
                    else:
                        type_map["set"][set_contents_key].add(value)
                elif set_contents_key in self.set_key_struct["setInner"]:
                    for set_inner_field in self.set_key_struct["setInner"][
                        set_contents_key
                    ]:
                        if set_inner_field not in type_map:
                            type_map[set_inner_field] = set()

                        for inner_struct in set_contents.get(set_contents_key):
                            value = inner_struct.get(set_inner_field)

                            if isinstance(value, list):
                                type_map[set_inner_field].update(value)
                            else:
                                type_map[set_inner_field].add(value)

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

    def to_json(self) -> dict[str, dict[str, list[str]] | list[str]]:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        return self.attr_value_dict
