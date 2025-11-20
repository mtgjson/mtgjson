"""
API for how providers need to interact with other classes
"""
from __future__ import annotations


import abc
import copy
import datetime
import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Union, TYPE_CHECKING

import requests
import requests_cache

from ..mtgjson_config import MtgjsonConfig
from ..retryable_session import retryable_session

if TYPE_CHECKING:
    from ..models import MtgjsonPricesObject

LOGGER = logging.getLogger(__name__)


class AbstractProvider(abc.ABC):
    """
    Abstract class to indicate what other providers should provide
    """

    class_id: str
    session: Union[requests.Session, requests_cache.CachedSession]
    today_date: str = datetime.datetime.today().strftime("%Y-%m-%d")

    def __init__(self, headers: Dict[str, str]) -> None:
        super().__init__()
        self.class_id = ""
        self.session = retryable_session()
        self.session.headers.update(headers)

    # Abstract Methods
    @abc.abstractmethod
    def _build_http_header(self) -> Dict[str, str]:
        """
        Construct the HTTP authorization header
        :return: Authorization header
        """

    @abc.abstractmethod
    def download(
        self, url: str, params: Optional[Dict[str, Union[str, int]]] = None
    ) -> Any:
        """
        Download an object from a service using appropriate authentication protocols
        :param url: URL to download content from
        :param params: Options to give to the GET request
        """

    def set_session(self, session: requests.Session) -> None:
        """
        Override the HTTP session (primarily for test injection).
        :param session: Custom session to use for HTTP requests
        """
        self.session = session

    # Class Methods
    @classmethod
    def get_class_name(cls) -> str:
        """
        Get the name of the calling class
        :return: Calling class name
        """
        return cls.__name__

    @classmethod
    def get_class_id(cls) -> str:
        """
        Grab the class ID for hashing purposes
        :return Class ID
        """
        return cls.class_id

    @staticmethod
    def log_download(response: Any) -> None:
        """
        Log how the URL was acquired
        :param response: Response from Server
        """
        from_cache = (
            getattr(response, "from_cache", False)
            if MtgjsonConfig().use_cache
            else False
        )
        LOGGER.debug(f"Downloaded {response.url} (Cache = {from_cache})")

    def generic_generate_today_price_dict(
        self,
        third_party_to_mtgjson: Dict[str, Set[Any]],
        price_data_rows: List[Dict[str, Any]],
        card_platform_id_key: str,
        default_prices_object: MtgjsonPricesObject,
        foil_key: str,
        retail_key: Optional[str] = None,
        retail_quantity_key: Optional[str] = None,
        buy_key: Optional[str] = None,
        buy_quantity_key: Optional[str] = None,
        etched_key: Optional[str] = None,
        etched_value: Optional[str] = None,
    ) -> Dict[str, MtgjsonPricesObject]:
        """
        Generically convert price data to MTGJSON data format
        :param third_party_to_mtgjson: Mapping of 3rdPartyID to MTGJSON ID(s)
        :param price_data_rows: Rows from 3rd Party provider with price data
        :param card_platform_id_key: ID in each price data row to get the 3rd Party ID from
        :param default_prices_object: Default prices object for the price points
        :param foil_key: ID in each price data row to determine if card is foil or non-foil
        :param retail_key: Optional determination key to see if we have sell prices
        :param retail_quantity_key: Optional determination key to check for quantity, for pruning
        :param buy_key: Optional determination key to see if we have buy prices
        :param buy_quantity_key: Optional determination key to check for quantity, for pruning
        :param etched_key: Optional determination key to see if we have an etched card
        :param etched_value: Optional value to find in etched_key to see if etched card or not
        :return Today's price setup in MTGJSON Price Format
        """
        from ..models import MtgjsonPricesObject
        today_dict: Dict[str, MtgjsonPricesObject] = defaultdict(
            lambda: copy.copy(default_prices_object)
        )

        for data_row in price_data_rows:
            third_party_id = str(data_row[card_platform_id_key])
            if third_party_id not in third_party_to_mtgjson:
                continue

            mtgjson_uuids = third_party_to_mtgjson[third_party_id]
            for mtgjson_uuid in mtgjson_uuids:
                is_foil = str(data_row[foil_key]).lower() == "true"
                is_etched = bool(
                    etched_key and etched_value in data_row.get(etched_key, {})
                )

                if retail_key and not (
                    retail_quantity_key and data_row.get(retail_quantity_key, 0) == 0
                ):
                    price_field_name = self.get_price_field_name(
                        is_foil, is_etched, True
                    )
                    price = float(data_row[retail_key])
                    setattr(today_dict[mtgjson_uuid], price_field_name, price)

                if buy_key and not (
                    buy_quantity_key and data_row.get(buy_quantity_key, 0) == 0
                ):
                    price_field_name = self.get_price_field_name(
                        is_foil, is_etched, False
                    )
                    price = float(data_row[buy_key])
                    setattr(today_dict[mtgjson_uuid], price_field_name, price)

        return today_dict

    @staticmethod
    def get_price_field_name(is_foil: bool, is_etched: bool, is_sell: bool) -> str:
        """
        Determine what MtgjsonPricesObject field needs to be set based on params
        :param is_foil: Is the card foil?
        :param is_etched: Is the card (foil) etched?
        :param is_sell: Is the card a sell option? (Or a buy option?)
        :return MtgjsonPricesObject key to set
        """
        if is_etched:
            return "sell_etched" if is_sell else "buy_etched"
        if is_foil:
            return "sell_foil" if is_sell else "buy_foil"
        return "sell_normal" if is_sell else "buy_normal"
