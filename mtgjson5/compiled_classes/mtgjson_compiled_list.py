"""
MTGJSON CompiledList Object
"""
from typing import List

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
        self.files = sorted(MtgjsonStructuresObject().get_compiled_list_files())

    def to_json(self) -> List[str]:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        return self.files
