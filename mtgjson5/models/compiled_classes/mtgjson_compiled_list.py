"""MTGJSON Compiled List base model for list-based compiled outputs."""

from typing import Any

from pydantic import Field

from ..mtgjson_base import MTGJsonCompiledModel
from .mtgjson_structures import MtgjsonStructuresObject


class MtgjsonCompiledListObject(MTGJsonCompiledModel):
    """
    The Compiled List output containing a list of all compiled file names.
    """

    files: list[str] = Field(
        default_factory=list,
        description="A list of all available compiled output file names.",
    )

    def __init__(self, **data: Any) -> None:
        """
        Initializer to build up the object
        """
        super().__init__(**data)
        if not self.files:
            self.files = sorted(MtgjsonStructuresObject().get_compiled_list_files())

    def to_list(self) -> list[str]:
        """
        Support json.dump()
        :return: List serialized object
        """
        return self.files
