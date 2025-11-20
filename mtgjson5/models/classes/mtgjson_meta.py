import datetime
from typing import Any, Dict, Union

from ... import constants
from ...mtgjson_config import MtgjsonConfig
from ..mtgjson_base import MTGJsonModel


class MtgjsonMetaObject(MTGJsonModel):
    """
    MTGJSON Meta Object
    """

    date: str
    version: str

    def __init__(
        self,
        date: Union[str, datetime.datetime, None] = None,
        version: str = None,
        **data
    ):
        """
        Initialize meta object with date and version
        :param date: Build date (string or datetime)
        :param version: MTGJSON version
        """
        # Handle date conversion
        if date is None:
            date = constants.MTGJSON_BUILD_DATE
        if isinstance(date, datetime.datetime):
            date = date.strftime("%Y-%m-%d")

        # Handle version default
        if version is None:
            version = MtgjsonConfig().mtgjson_version

        super().__init__(date=date, version=version, **data)

    def to_json(self) -> Dict[str, Any]:
        """
        Custom JSON serialization with datetime handling
        :return: JSON object
        """
        parent: Dict[str, Any] = super().to_json()
        for key, value in parent.items():
            if isinstance(value, datetime.datetime):
                parent[key] = value.strftime("%Y-%m-%d")
        return parent
