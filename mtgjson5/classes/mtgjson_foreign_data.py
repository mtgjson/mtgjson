"""
MTGJSON Singular Card.ForeignData Object
"""

from typing import Any, Dict, Iterable, Optional

from .json_object import JsonObject
from .mtgjson_identifiers import MtgjsonIdentifiersObject


class MtgjsonForeignDataObject(JsonObject):
    """
    MTGJSON Singular Card.ForeignData Object
    """

    language: str
    multiverse_id: Optional[int]  # Deprecated - Remove in 5.4.0
    identifiers: MtgjsonIdentifiersObject
    face_name: Optional[str]
    flavor_text: Optional[str]
    name: Optional[str]
    text: Optional[str]
    type: Optional[str]

    def __init__(self) -> None:
        self.multiverse_id = None
        self.identifiers = MtgjsonIdentifiersObject()
        self.face_name = None
        self.flavor_text = None
        self.name = None
        self.text = None
        self.type = None

    def build_keys_to_skip(self) -> Iterable[str]:
        return {"url", "number", "set_code"}

    def to_json(self) -> Dict[str, Any]:
        parent = super().to_json()
        return {key: value for key, value in parent.items() if value is not None}
