"""
API for how providers need to interact with other classes
"""
import abc


class AbstractProvider(abc.ABC):
    """
    Abstract class to indicate what other providers should provide
    """

    def __init__(self):
        super().__init__()

    @abc.abstractmethod
    def download(self, url: str) -> str:
        """
        Download an object from a service using appropriate authentication protocols
        :param url: URL to download content from
        """
        pass

    @abc.abstractmethod
    def init_session_pool(self) -> None:
        """
        Initialize the pool of sessions a download request can pull from to have
        greater throughput
        """
        pass
