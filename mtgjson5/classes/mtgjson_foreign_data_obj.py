"""
MTGJSON container for foreign entries
"""

from typing import Any, Dict, Set

from ..utils import to_camel_case


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

    url: str
    number: float
    set_code: str

    def __init__(self) -> None:
        pass

    def build_keys_to_skip(self) -> Set[str]:
        """
        Build this object's instance of what keys to skip under certain circumstances
        :return What keys to skip over
        """
        excluded_keys: Set[str] = set()

        for key, value in self.__dict__.items():
            if not value:
                excluded_keys.add(key)

        return excluded_keys

    def for_json(self) -> Dict[str, Any]:
        """
        Support json.dumps()
        :return: JSON serialized object
        """
        skip_keys = self.build_keys_to_skip().union({"url", "number", "set_code"})

        return {
            to_camel_case(key): value
            for key, value in self.__dict__.items()
            if not key.startswith("__") and not callable(value) and key not in skip_keys
        }
