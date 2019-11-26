"""
Class structure for a MTGJSON Set Object
"""
from typing import Any, Dict, List, Optional

from mtgjson5.classes.mtgjson_card_obj import MtgjsonCardObject
from mtgjson5.classes.mtgjson_meta_obj import MtgjsonMetaObject
from mtgjson5.classes.mtgjson_translations_obj import MtgjsonTranslationsObject
from mtgjson5.globals import to_camel_case


class MtgjsonSetObject:
    """
    MTGJSON Set Object
    """

    base_set_size: int
    block: str
    booster_v3: List[Any]
    cards: List[MtgjsonCardObject]
    code: str
    code_v3: str
    is_foreign_only: bool
    is_foil_only: bool
    is_online_only: bool
    is_partial_preview: bool
    keyrune_code: str
    mcm_id: int
    mcm_name: str
    meta: MtgjsonMetaObject
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

    def __init__(self) -> None:
        pass

    def __str__(self) -> str:
        return str(vars(self))

    def for_json(self) -> Dict[str, Any]:
        """
        Support json.dumps()
        :return: JSON serialized object
        """
        skip_keys = {"added_scryfall_tokens", "search_uri"}

        return {
            to_camel_case(key): value
            for key, value in self.__dict__.items()
            if not key.startswith("__") and not callable(value) and key not in skip_keys
        }
