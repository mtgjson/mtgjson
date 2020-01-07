"""
MTGJSON's meta object to determine time and version
"""
import datetime
from typing import Any, Dict

from ..consts import MtgjsonGlobals
from ..utils import to_camel_case


class MtgjsonMetaObject:
    """
    Determine what version of software built this object
    """

    date: datetime.datetime
    prices_date: datetime.datetime
    version: str

    def __init__(
        self,
        date: datetime.datetime = datetime.datetime.today(),
        prices_date: datetime.datetime = datetime.datetime.today(),
    ) -> None:
        self.date = date
        self.prices_date = prices_date
        self.version = MtgjsonGlobals.MTGJSON_VERSION

    def for_json(self) -> Dict[str, Any]:
        """
        Support json.dumps()
        :return: JSON serialized object
        """
        options = {
            to_camel_case(key): value
            for key, value in self.__dict__.items()
            if not key.startswith("__") and not callable(value)
        }

        for key, value in options.items():
            if isinstance(value, datetime.datetime):
                options[key] = value.strftime("%Y-%m-%d")

        return options
