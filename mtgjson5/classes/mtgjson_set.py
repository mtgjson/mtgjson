"""
MTGJSON Singular Set Object
"""
from typing import Any, Dict, List, Optional, Set

from ..classes.mtgjson_card import MtgjsonCardObject
from ..classes.mtgjson_translations import MtgjsonTranslationsObject
from ..utils import to_camel_case


class MtgjsonSetObject:
    """
    MTGJSON Singular Set Object
    """

    base_set_size: int
    block: str
    booster: Optional[Dict[str, Any]]
    cards: List[MtgjsonCardObject]
    code: str
    code_v3: str
    is_foreign_only: bool
    is_foil_only: bool
    is_non_foil_only: bool
    is_online_only: bool
    is_partial_preview: bool
    keyrune_code: str
    mcm_id: Optional[int]
    mcm_id_extras: Optional[int]
    mcm_name: Optional[str]
    mtgo_code: str
    name: str
    parent_code: str
    release_date: str
    tcgplayer_group_id: Optional[int]
    tokens: List[MtgjsonCardObject]
    total_set_size: int
    translations: MtgjsonTranslationsObject
    type: str

    extra_tokens: List[Dict[str, Any]]
    search_uri: str

    __allow_if_falsey = {
        "cards",
        "tokens",
        "is_foil_only",
        "is_online_only",
        "base_set_size",
        "total_set_size",
    }

    def __init__(self) -> None:
        """
        Initializer to ensure arrays are pre-loaded
        """
        self.extra_tokens = []
        self.cards = []
        self.tokens = []

    def __str__(self) -> str:
        """
        MTGJSON Set as a string for debugging purposes
        :return MTGJSON Set as a string
        """
        return str(vars(self))

    def build_keys_to_skip(self) -> Set[str]:
        """
        Build this object's instance of what keys to skip under certain circumstances
        :return What keys to skip over
        """
        excluded_keys: Set[str] = {
            "added_scryfall_tokens",
            "search_uri",
            "extra_tokens",
        }

        for key, value in self.__dict__.items():
            if not value:
                if key not in self.__allow_if_falsey:
                    excluded_keys.add(key)

        return excluded_keys

    def to_json(self) -> Dict[str, Any]:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        skip_keys = self.build_keys_to_skip()

        return {
            to_camel_case(key): value
            for key, value in self.__dict__.items()
            if "__" not in key and not callable(value) and key not in skip_keys
        }
