"""
API for how providers need to interact with other classes
"""
import abc
import configparser
import datetime
import logging
from typing import Any, Dict, Union

import requests_cache

from ..consts import CACHE_PATH, CONFIG, USE_CACHE

LOGGER = logging.getLogger(__name__)


class AbstractProvider(abc.ABC):
    """
    Abstract class to indicate what other providers should provide
    """

    class_id: str
    session_header: Dict[str, str]
    today_date: str = datetime.datetime.today().strftime("%Y-%m-%d")

    def __init__(self, headers: Dict[str, str]):
        super().__init__()
        self.class_id = ""
        self.session_header = headers
        self.__install_cache()

    # Abstract Methods
    @abc.abstractmethod
    def _build_http_header(self) -> Dict[str, str]:
        """
        Construct the HTTP authorization header
        :return: Authorization header
        """

    @abc.abstractmethod
    def download(self, url: str, params: Dict[str, Union[str, int]] = None) -> Any:
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
    def get_configs() -> configparser.ConfigParser:
        """
        Parse the config for this specific setup
        :return: Parsed config file
        """
        return CONFIG

    @staticmethod
    def log_download(response: Any) -> None:
        """
        Log how the URL was acquired
        :param response: Response from Server
        """
        LOGGER.debug(
            f"Downloaded {response.url} (Cache = {response.from_cache if USE_CACHE else False})"
        )

    # Private Methods
    def __install_cache(self) -> None:
        """
        Initiate the MTGJSON cache for requests
        (Useful for development and re-running often)
        """
        if USE_CACHE:
            CACHE_PATH.mkdir(exist_ok=True)
            requests_cache.install_cache(
                str(CACHE_PATH.joinpath(self.get_class_name()))
            )
