"""
API for how providers need to interact with other classes
"""
import abc
import copy
import datetime
import logging
from typing import Any, Dict, List, Optional, Set, Union

import requests
import requests_cache

from ..classes import MtgjsonPricesObject
from ..mtgjson_config import MtgjsonConfig
from ..retryable_session import retryable_session

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
        LOGGER.debug(
            f"Downloaded {response.url} (Cache = {response.from_cache if MtgjsonConfig().use_cache else False})"
        )

    @staticmethod
    def generic_generate_today_price_dict(
        third_party_to_mtgjson: Dict[str, Set[Any]],
        price_data_rows: List[Dict[str, Any]],
        card_platform_id_key: str,
        default_prices_object: MtgjsonPricesObject,
        foil_key: str,
        retail_key: Optional[str] = None,
        buy_key: Optional[str] = None,
        buy_quantity_key: Optional[str] = None,
    ) -> Dict[str, MtgjsonPricesObject]:
        """
        Generically convert price data to MTGJSON data format
        :param third_party_to_mtgjson: Mapping of 3rdPartyID to MTGJSON ID(s)
        :param price_data_rows: Rows from 3rd Party provider with price data
        :param card_platform_id_key: ID in each price data row to get the 3rd Party ID from
        :param default_prices_object: Default prices object for the price points
        :param foil_key: ID in each price data row to determine if card is foil or non-foil
        :param retail_key: Optional determination key to see if we have sell prices
        :param buy_key: Optional determination key to see if we have buy prices
        :param buy_quantity_key: Optional determination key to check for quantity, for pruning
        :return Today's price setup in MTGJSON Price Format
        """

        today_dict: Dict[str, MtgjsonPricesObject] = {}

        for data_row in price_data_rows:
            third_party_id = str(data_row[card_platform_id_key])
            if third_party_id not in third_party_to_mtgjson:
                continue

            mtgjson_uuids = third_party_to_mtgjson[third_party_id]
            for mtgjson_uuid in mtgjson_uuids:
                if mtgjson_uuid not in today_dict:
                    today_dict[mtgjson_uuid] = copy.copy(default_prices_object)

                if data_row[foil_key] == "true":
                    if retail_key:
                        today_dict[mtgjson_uuid].sell_foil = float(data_row[retail_key])
                    if buy_key:
                        if buy_quantity_key and data_row[buy_quantity_key] == 0:
                            continue
                        today_dict[mtgjson_uuid].buy_foil = float(data_row[buy_key])
                else:
                    if retail_key:
                        today_dict[mtgjson_uuid].sell_normal = float(
                            data_row[retail_key]
                        )
                    if buy_key:
                        if buy_quantity_key and data_row[buy_quantity_key] == 0:
                            continue
                        today_dict[mtgjson_uuid].buy_normal = float(data_row[buy_key])

        return today_dict
