"""MTGJSON Foreign Data Object model for localized card information."""

from typing import Any, Dict, Optional, Set

from pydantic import Field

from ..mtgjson_base import MTGJsonModel
from .mtgjson_identifiers import MtgjsonIdentifiersObject


class MtgjsonForeignDataObject(MTGJsonModel):
    """
    MTGJSON Singular Card.ForeignData Object
    """

    uuid: str = ""
    language: str = ""
    identifiers: MtgjsonIdentifiersObject = Field(
        default_factory=MtgjsonIdentifiersObject
    )
    multiverse_id: Optional[int] = None
    face_name: Optional[str] = None
    flavor_text: Optional[str] = None
    name: Optional[str] = None
    text: Optional[str] = None
    type: Optional[str] = None

    def build_keys_to_skip(self) -> Set[str]:
        """
        Keys to skip in JSON output
        :return: Set of keys to skip
        """
        return {"url", "number", "set_code"}

    def to_json(self) -> Dict[str, Any]:
        """
        Custom JSON serialization that filters out None values
        :return: JSON serialized object
        """
        parent = super().to_json()
        return {key: value for key, value in parent.items() if value is not None}
