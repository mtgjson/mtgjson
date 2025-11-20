from typing import Any, Dict, List, Optional, Set

from pydantic import Field

from ... import constants
from ..mtgjson_base import MTGJsonSetModel
from .mtgjson_card import MtgjsonCardObject
from .mtgjson_deck import MtgjsonDeckObject
from .mtgjson_sealed_product import MtgjsonSealedProductObject
from .mtgjson_translations import MtgjsonTranslationsObject


class MtgjsonSetObject(MTGJsonSetModel):
    """
    MTGJSON Singular Set Object
    """

    # Class variable for conditional exclusion
    _allow_if_falsey: Set[str] = {
        "cards",
        "tokens",
        "is_foil_only",
        "is_online_only",
        "base_set_size",
        "total_set_size",
    }

    # Required fields
    base_set_size: int = 0
    block: str = ""
    cards: List[MtgjsonCardObject] = Field(default_factory=list)
    code: str = ""
    code_v3: str = ""
    decks: List[MtgjsonDeckObject] = Field(default_factory=list)
    is_foreign_only: bool = False
    is_foil_only: bool = False
    is_non_foil_only: bool = False
    is_online_only: bool = False
    is_partial_preview: bool = False
    keyrune_code: str = ""
    languages: List[str] = Field(default_factory=list)
    mtgo_code: str = ""
    name: str = ""
    parent_code: str = ""
    release_date: str = ""
    sealed_product: List[MtgjsonSealedProductObject] = Field(default_factory=list)
    tokens: List[MtgjsonCardObject] = Field(default_factory=list)
    total_set_size: int = 0
    translations: MtgjsonTranslationsObject = Field(default_factory=MtgjsonTranslationsObject)
    type: str = ""
    extra_tokens: List[Dict[str, Any]] = Field(default_factory=list, exclude=True)
    search_uri: str = Field(default="", exclude=True)

    # Optional fields
    booster: Optional[Dict[str, Any]] = None
    cardsphere_set_id: Optional[int] = None
    mcm_id: Optional[int] = None
    mcm_id_extras: Optional[int] = None
    mcm_name: Optional[str] = None
    tcgplayer_group_id: Optional[int] = None
    token_set_code: Optional[str] = None

    def __str__(self) -> str:
        """
        MTGJSON Set as a string for debugging purposes
        :return: MTGJSON Set as a string
        """
        return str(vars(self))

    def build_keys_to_skip(self) -> Set[str]:
        """
        Build this object's instance of what keys to skip under certain circumstances
        :return: What keys to skip over
        """
        excluded_keys: Set[str] = {
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
