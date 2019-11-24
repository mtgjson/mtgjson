"""
API for how providers need to interact with other classes
"""
from __future__ import annotations

import abc
import configparser
import multiprocessing
import queue
from typing import Dict

import requests
import requests.adapters
import requests_cache
import urllib3

from mtgjson5.globals import CONFIG_PATH, CACHE_PATH


class AbstractProvider(abc.ABC):
    """
    Abstract class to indicate what other providers should provide
    """

    session_pool: queue.Queue[requests.Session]

    def __init__(self, headers: Dict[str, str], use_cache: bool = True):
        super().__init__()

        self.session_pool = queue.Queue()

        self.__install_cache(use_cache)
        self.__init_session_pool(headers)

    @classmethod
    def get_class_name(cls) -> str:
        """
        Get the name of the calling class
        :return: Calling class name
        """
        return cls.__name__

    @abc.abstractmethod
    def download(self, url: str) -> str:
        """
        Download an object from a service using appropriate authentication protocols
        :param url: URL to download content from
        """
        pass

    def __install_cache(self, use_cache: bool):
        if use_cache:
            requests_cache.install_cache(
                str(CACHE_PATH.joinpath(f"{self.get_class_name()}_cache"))
            )

    def __init_session_pool(self, headers: Dict[str, str] = None) -> None:
        """
        Initialize the pool of sessions a download request can pull from to have
        greater throughput
        """
        if headers is None:
            headers = {}

        for i in range(0, multiprocessing.cpu_count() * 3):
            session = requests.Session()

            if headers:
                session.headers.update(headers)

            session = self.__retryable_session(session)
            self.session_pool.put(session)

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
