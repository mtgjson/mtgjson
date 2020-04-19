"""
MTGJSON CompiledList Object
"""
from typing import Any, Dict, List

from ..utils import to_camel_case
from .mtgjson_structures import MtgjsonStructuresObject


class MtgjsonCompiledListObject:
    """
    MTGJSON CompiledList Object
    """

    files: List[str]

    def __init__(self) -> None:
        """
        Initializer to build up the object
        """
        self.files = MtgjsonStructuresObject().get_compiled_list_files()

    def to_json(self) -> Dict[str, Any]:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        return {
            to_camel_case(key): value
            for key, value in self.__dict__.items()
            if "__" not in key and not callable(value)
        }
