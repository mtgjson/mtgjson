"""
MTGJSON CompiledList container
"""
from typing import Any, Dict, List

from ..utils import to_camel_case
from .mtgjson_structures import MtgjsonStructuresObject


class MtgjsonCompiledListObject:
    """
    CompiledList container
    """

    files: List[str]

    def __init__(self) -> None:
        self.files = MtgjsonStructuresObject().get_compiled_list_files()

    def for_json(self) -> Dict[str, Any]:
        """
        Support json.dumps()
        :return: JSON serialized object
        """
        return {
            to_camel_case(key): value
            for key, value in self.__dict__.items()
            if "__" not in key and not callable(value)
        }
