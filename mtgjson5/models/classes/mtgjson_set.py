"""MTGJSON Set Object model for MTG card set data and metadata."""

from typing import Any

from pydantic import Field

from ... import constants
from ..mtgjson_base import MTGJsonSetModel
from .mtgjson_booster_config import MtgjsonBoosterConfigObject
from .mtgjson_card import MtgjsonCardObject
from .mtgjson_deck import MtgjsonDeckObject
from .mtgjson_sealed_product import MtgjsonSealedProductObject
from .mtgjson_translations import MtgjsonTranslationsObject


class MtgjsonSetObject(MTGJsonSetModel):
    """
    The Set Data Model describes the properties of an individual set.
    """

    # Class variable for conditional exclusion
    _allow_if_falsey: set[str] = {
        "cards",
        "tokens",
        "is_foil_only",
        "is_online_only",
        "base_set_size",
        "total_set_size",
    }

    # Required fields
    base_set_size: int = Field(default=0, description="The number of cards in the set.")
    block: str | None = Field(
        default=None, description="The block name the set is in."
    )
    cards: list[MtgjsonCardObject] = Field(
        default_factory=list, description="The list of cards in the set."
    )
    code: str = Field(
        default="", description="The printing set code for the set."
    )
    code_v3: str | None = Field(
        default=None,
        description="The alternate printing set code Wizards of the Coast uses for a select few duel deck sets.",
    )
    decks: list[MtgjsonDeckObject] = Field(
        default_factory=list, description="All decks associated to the set."
    )
    is_foreign_only: bool | None = Field(
        default=None,
        description="If the set is only available outside the United States of America.",
    )
    is_foil_only: bool = Field(
        default=False,
        validation_alias="foil_only",
        description="If the set is only available in foil.",
    )
    is_non_foil_only: bool | None = Field(
        default=None,
        validation_alias="nonfoil_only",
        description="If the set is only available in non-foil.",
    )
    is_online_only: bool = Field(
        default=False,
        validation_alias="digital",
        description="If the set is only available in online game play variations.",
    )
    is_partial_preview: bool | None = Field(
        default=None, description="If the set is still in preview (spoiled)."
    )
    keyrune_code: str = Field(
        default="", description="The matching Keyrune code for set image icons."
    )
    languages: list[str] = Field(
        default_factory=list, description="The languages the set was printed in."
    )
    mtgo_code: str | None = Field(
        default=None,
                description="The set code for the set as it appears on Magic: The Gathering Online.",
    )
    name: str = Field(default="", description="The name of the set.")
    parent_code: str | None = Field(
        default=None,
        validation_alias="parent_set_code",
        description="The parent printing set code for set variations.",
    )
    release_date: str = Field(
        default="",
        validation_alias="released_at",
        description="The release date in ISO 8601 format for the set.",
    )
    sealed_product: list[MtgjsonSealedProductObject] = Field(
        default_factory=list, description="The sealed product information for the set."
    )
    tokens: list[MtgjsonCardObject] = Field(
        default_factory=list, description="The tokens cards in the set."
    )
    total_set_size: int = Field(
        default=0, description="The total number of cards in the set."
    )
    translations: MtgjsonTranslationsObject = Field(
        default_factory=MtgjsonTranslationsObject,
        description="The translated set name by language.",
    )
    type: str = Field(
        default="", validation_alias="set_type", description="The expansion type of the set."
    )

    booster: dict[str, MtgjsonBoosterConfigObject] | None = Field(
        default=None,
        description="A breakdown of possibilities and weights of cards in a booster pack.",
    )
    cardsphere_set_id: int | None = Field(
        default=None, description="The Cardsphere set identifier."
    )
    mcm_id: int | None = Field(
        default=None, description="The Cardmarket set identifier."
    )
    mcm_id_extras: int | None = Field(
        default=None, description="The split Cardmarket set identifier."
    )
    mcm_name: str | None = Field(default=None, description="The Cardmarket set name.")
    tcgplayer_group_id: int | None = Field(
        default=None,
        alias="tcgplayer_id",
        description="The group identifier of the set on TCGplayer.",
    )
    token_set_code: str | None = Field(
        default=None, description="The tokens set code, formatted in uppercase."
    )

    # Private/excluded fields
    extra_tokens: list[dict[str, Any]] = Field(default_factory=list, exclude=True)
    search_uri: str = Field(default="", exclude=True)

    def __str__(self) -> str:
        """
        MTGJSON Set as a string for debugging purposes
        :return: MTGJSON Set as a string
        """
        return str(vars(self))

    def build_keys_to_skip(self) -> set[str]:
        """
        Build this object's instance of what keys to skip under certain circumstances
        :return: What keys to skip over
        """
        excluded_keys: set[str] = {
            "added_scryfall_tokens",
            "search_uri",
            "extra_tokens",
        }

        for key, value in self.__dict__.items():
            if not value:
                if key not in self._allow_if_falsey:
                    excluded_keys.add(key)

        return excluded_keys

    def get_windows_safe_set_code(self) -> str:
        """
        In the Windows OS, there are certain file names that are not allowed.
        In case we have a set with such a name, we will add a _ to the end to allow its existence
        on Windows.
        :return: Set name with appended underscore, if necessary
        """
        if self.code in constants.BAD_FILE_NAMES:
            return self.code + "_"
        return self.code
