"""
MTGJSON's meta object to determine time and version
"""
import datetime
from typing import Any, Dict, Union

from ..consts import MTGJSON_BUILD_DATE, MTGJSON_PRICE_BUILD_DATE, MTGJSON_VERSION
from ..utils import to_camel_case


class MtgjsonMetaObject:
    """
    Determine what version of software built this object
    """

    date: str
    prices_date: str
    version: str

    def __init__(
        self,
        date: Union[str, datetime.datetime] = MTGJSON_BUILD_DATE,
        prices_date: Union[str, datetime.datetime] = MTGJSON_PRICE_BUILD_DATE,
        version: str = MTGJSON_VERSION,
    ) -> None:
        self.date = date if isinstance(date, str) else date.strftime("%Y-%m-%d")
        self.prices_date = (
            prices_date
            if isinstance(prices_date, str)
            else prices_date.strftime("%Y-%m-%d")
        )
        self.version = version

    def for_json(self) -> Dict[str, Any]:
        """
        Support json.dumps()
        :return: JSON serialized object
        """
        options = {
            to_camel_case(key): value
            for key, value in self.__dict__.items()
            if "__" not in key and not callable(value)
        }

        for key, value in options.items():
            if isinstance(value, datetime.datetime):
                options[key] = value.strftime("%Y-%m-%d")

        return options
