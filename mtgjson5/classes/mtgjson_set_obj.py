"""
Class structure for a MTGJSON Set Object
"""
import datetime
import json
from typing import List, Any, Dict

from mtgjson5.classes.mtgjson_card_obj import MtgjsonCardObject
from mtgjson5.classes.mtgjson_meta_obj import MtgjsonMetaObject
from mtgjson5.classes.mtgjson_translations_obj import MtgjsonTranslationsObject


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
    tcgplayer_group_id: int
    tokens: List[MtgjsonCardObject]
    total_set_size: int
    translations: MtgjsonTranslationsObject
    type: str

    search_uri: str

    def __init__(self):
        pass

    def __str__(self):
        return str(vars(self))

    def for_json(self) -> Dict[str, Any]:
        """
        Support json.dumps()
        :return: JSON serialized object
        """
        return {
            key: value
            for key, value in self.__dict__.items()
            if not key.startswith("__") and not callable(value)
        }
