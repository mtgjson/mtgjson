"""
MTGJSON container for foreign entries
"""
import logging
from typing import Any, Dict, List

from mtgjson5.globals import LANGUAGE_MAP, init_thread_logger, to_camel_case
from mtgjson5.providers.scryfall_provider import ScryfallProvider


class MtgjsonForeignDataObject:
    """
    Foreign data rows
    """

    flavor_text: str
    language: str
    multiverse_id: int
    name: str
    text: str
    type: str

    _url: str
    _number: float
    _set_code: str

    def __init__(self):
        pass

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
