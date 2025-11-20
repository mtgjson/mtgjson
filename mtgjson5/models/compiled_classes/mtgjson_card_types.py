from typing import Any, Dict, List, Optional, Match
from pydantic import Field
import re
import string

from ... import providers, utils
from ..mtgjson_base import MTGJsonCompiledModel

ScryfallProvider = providers.scryfall.monolith.ScryfallProvider
WizardsProvider = providers.wizards.WizardsProvider
parse_magic_rules_subset = utils.parse_magic_rules_subset


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


class MtgjsonCardTypesObject(MTGJsonCompiledModel):
    """
    MTGJSON CardTypes Object
    """

    class MtgjsonCardTypesInnerObject(MTGJsonCompiledModel):
        """
        MTGJSON CardTypes.CardTypesInner Object
        """

        artifact: List[str] = Field(default_factory=list)
        battle: List[str] = Field(default_factory=list)
        conspiracy: List[str] = Field(default_factory=list)
        creature: List[str] = Field(default_factory=list)
        enchantment: List[str] = Field(default_factory=list)
        instant: List[str] = Field(default_factory=list)
        land: List[str] = Field(default_factory=list)
        phenomenon: List[str] = Field(default_factory=list)
        plane: List[str] = Field(default_factory=list)
        planeswalker: List[str] = Field(default_factory=list)
        scheme: List[str] = Field(default_factory=list)
        sorcery: List[str] = Field(default_factory=list)
        tribal: List[str] = Field(default_factory=list)
        vanguard: List[str] = Field(default_factory=list)

        def __init__(self, magic_rules: str, **kwargs) -> None:
            """
            Internal initializer
            :param magic_rules: Rules for MTG from Wizards
            """
            super().__init__(**kwargs)

            planar_regex = re.compile(r".*The planar types are (.*)\.")

            self.artifact = ScryfallProvider().get_catalog_entry("artifact-types")
            self.battle = ScryfallProvider().get_catalog_entry("battle-types")
            self.conspiracy = []
            self.creature = ScryfallProvider().get_catalog_entry("creature-types")
            self.enchantment = ScryfallProvider().get_catalog_entry("enchantment-types")
            self.instant = ScryfallProvider().get_catalog_entry("spell-types")
            self.land = ScryfallProvider().get_catalog_entry("land-types")
            self.phenomenon = []
            self.plane = regex_str_to_list(planar_regex.search(magic_rules))
            self.planeswalker = ScryfallProvider().get_catalog_entry("planeswalker-types")
            self.scheme = []
            self.sorcery = self.instant
            self.tribal = []
            self.vanguard = []

    types: Dict[str, Dict[str, List[str]]] = Field(default_factory=dict)

    def __init__(self, **kwargs) -> None:
        """
        Initializer to build up the object
        """
        super().__init__(**kwargs)
        self.types = {}

        comp_rules = parse_magic_rules_subset(WizardsProvider().get_magic_rules())

        inner_sets = self.MtgjsonCardTypesInnerObject(comp_rules)

        super_regex = re.compile(r".*The supertypes are (.*)\.")
        super_types = regex_str_to_list(super_regex.search(comp_rules))

        for key, value in inner_sets.to_json().items():
            self.types[key] = {"subTypes": value, "superTypes": super_types}

    def to_json(self) -> Any:
        return self.types
