"""
MTGJSON Top Level Object
"""
import abc
from typing import Any, Dict, List

from mtgjson5.utils import to_camel_case


class JsonObject(abc.ABC):
    """
    Top level Json Dump object class
    """

    @abc.abstractmethod
    def build_keys_to_skip(self) -> List[str]:
        """
        Determine what keys should be avoided in the JSON dump
        :return Keys to avoid
        """

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
