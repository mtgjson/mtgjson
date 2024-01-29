"""
MTGJSON Meta Object
"""
import datetime
from typing import Any, Dict, Union

from .. import constants
from ..mtgjson_config import MtgjsonConfig
from .json_object import JsonObject


class MtgjsonMetaObject(JsonObject):
    """
    MTGJSON Meta Object
    """

    date: str
    version: str

    def __init__(
        self,
        date: Union[str, datetime.datetime] = constants.MTGJSON_BUILD_DATE,
        version: str = MtgjsonConfig().mtgjson_version,
    ) -> None:
        self.date = date if isinstance(date, str) else date.strftime("%Y-%m-%d")
        self.version = version

    def to_json(self) -> Dict[str, Any]:
        parent: Dict[str, Any] = super().to_json()
        for key, value in parent.items():
            if isinstance(value, datetime.datetime):
                parent[key] = value.strftime("%Y-%m-%d")

        return parent
