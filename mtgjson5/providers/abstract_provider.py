"""
API for how providers need to interact with other classes
"""
from __future__ import annotations

import abc
import collections
import configparser
import multiprocessing
from typing import Any, Deque, Dict, Union

import requests
import requests.adapters
import requests_cache
import urllib3

from ..consts import CACHE_PATH, CONFIG_PATH, USE_CACHE
from ..utils import get_thread_logger

LOGGER = get_thread_logger()


class AbstractProvider(abc.ABC):
    """
    Abstract class to indicate what other providers should provide
    """

    class_id: str
    session_pool: Deque[requests.Session]

    def __init__(self, headers: Dict[str, str]):
        get_thread_logger()

        super().__init__()

        self.class_id = ""
        self.session_pool = collections.deque()

        self.__install_cache()
        self.__init_session_pool(headers)

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
    def get_configs() -> configparser.RawConfigParser:
        """
        Parse the config for this specific setup
        :return: Parsed config file
        """
        config = configparser.RawConfigParser()
        config.read(CONFIG_PATH)

        return config

    @staticmethod
    def log_download(response: Any) -> None:
        """
        Log how the URL was acquired
        :param response: Response from Server
        """
        LOGGER.info(
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

    def __init_session_pool(self, headers: Dict[str, str] = None) -> None:
        """
        Initialize the pool of sessions a download request can pull from to have
        greater throughput
        """
        if headers is None:
            headers = {}

        for _ in range(0, multiprocessing.cpu_count() * 4):
            session = requests.Session()

            if headers:
                session.headers.update(headers)

            session = self.__retryable_session(session)
            self.session_pool.append(session)

    @staticmethod
    def __retryable_session(
        session: requests.Session, retries: int = 8
    ) -> requests.Session:
        """
        Session with requests to allow for re-attempts at downloading missing data
        :param session: Session to download with
        :param retries: How many retries to attempt
        :return: Session that does downloading
        """
        retry = urllib3.util.retry.Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=0.3,
            status_forcelist=(500, 502, 504),
        )

        adapter = requests.adapters.HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session
