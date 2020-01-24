"""
MTGJSON CardTypes container
"""
import re
import string
from typing import Any, Dict, List, Match, Optional

from ..classes import MtgjsonMetaObject
from ..providers import ScryfallProvider, WizardsProvider
from ..utils import parse_magic_rules_subset, to_camel_case


class MtgjsonCardTypesObject:
    """
    CardTypes container
    """

    class MtgjsonCardTypesInnerObject:
        """
        CardTypes inner container for each type
        """

        artifact: List[str]
        conspiracy: List[str]
        creature: List[str]
        enchantment: List[str]
        instant: List[str]
        land: List[str]
        phenomenon: List[str]
        plane: List[str]
        planeswalker: List[str]
        scheme: List[str]
        sorcery: List[str]
        tribal: List[str]
        vanguard: List[str]

        def __init__(self, magic_rules: str) -> None:
            """
            Internal initializer
            :param magic_rules: Rules for MTG from Wizards
            """
            self.artifact = ScryfallProvider().get_catalog_entry("artifact")
            self.conspiracy = []
            self.creature = ScryfallProvider().get_catalog_entry("creature")
            self.enchantment = ScryfallProvider().get_catalog_entry("enchantment")
            self.instant = ScryfallProvider().get_catalog_entry("spell")
            self.land = ScryfallProvider().get_catalog_entry("land")
            self.phenomenon = []
            self.plane = regex_str_to_list(
                re.search(r".*planar types are (.*)\.", magic_rules)
            )
            self.planeswalker = ScryfallProvider().get_catalog_entry("planeswalker")
            self.scheme = []
            self.sorcery = ScryfallProvider().get_catalog_entry("spell")
            self.tribal = []
            self.vanguard = []

        def for_json(self) -> Dict[str, Any]:
            """
            Support json.dumps()
            :return: JSON serialized object
            """
            return {
                to_camel_case(key): value
                for key, value in self.__dict__.items()
                if not key.startswith("__") and not callable(value)
            }

    types: Dict[str, Dict[str, List[str]]]
    meta: MtgjsonMetaObject

    def __init__(self) -> None:
        self.types = {}
        self.meta = MtgjsonMetaObject()

        comp_rules = parse_magic_rules_subset(
            WizardsProvider().get_magic_rules(),
            "205. Type Line",
            "206. Expansion Symbol",
        )

        inner_sets = self.MtgjsonCardTypesInnerObject(comp_rules)

        super_types = regex_str_to_list(
            re.search(r".*supertypes are (.*)\.", comp_rules)
        )

        for key, value in inner_sets.for_json().items():
            self.types[key] = {"subTypes": value, "superTypes": super_types}

    def get_card_types(self) -> Dict[str, Any]:
        """
        Get all possible card super, sub, and types from the rules.
        :return: Card types for return_value
        """

    def for_json(self) -> Dict[str, Any]:
        """
        Support json.dumps()
        :return: JSON serialized object
        """
        return {
            to_camel_case(key): value
            for key, value in self.__dict__.items()
            if not key.startswith("__") and not callable(value)
        }


def regex_str_to_list(regex_match: Optional[Match]) -> List[str]:
    """
    Take a regex match object and turn a string in
    format "a, b, c, ..., and z." into [a,b,c,...,z]
    :param regex_match: Regex match object
    :return: List of strings
    """
    if not regex_match:
        return []

    # Get only the sentence with the types
    card_types = regex_match.group(1).split(". ")[0]

    # Split the types by comma
    card_types_split: List[str] = card_types.split(", ")

    # If there are only two elements, split by " and " instead
    if len(card_types_split) == 1:
        card_types_split = card_types.split(" and ")
    else:
        # Replace the last one from "and XYZ" to just "XYZ"
        card_types_split[-1] = card_types_split[-1].split(" ", 1)[1]

    for index, value in enumerate(card_types_split):
        card_types_split[index] = string.capwords(value.split(" (")[0])

    return card_types_split
