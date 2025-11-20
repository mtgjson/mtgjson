from typing import Any, List

from pydantic import Field

from ..mtgjson_base import MTGJsonCompiledModel
from .mtgjson_structures import MtgjsonStructuresObject


class MtgjsonCompiledListObject(MTGJsonCompiledModel):
    """
    MTGJSON CompiledList Object
    """

    files: List[str] = Field(default_factory=list)

    def __init__(self, **data: Any) -> None:
        """
        Initializer to build up the object
        """
        super().__init__(**data)
        if not self.files:
            self.files = sorted(MtgjsonStructuresObject().get_compiled_list_files())

    def to_json(self) -> List[str]:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        return self.files
