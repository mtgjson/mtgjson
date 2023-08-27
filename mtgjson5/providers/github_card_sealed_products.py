"""
Card Sealed Products via GitHub 3rd party provider
"""
import logging
from typing import Any, Dict, List, Optional, Union

from singleton_decorator import singleton

from mtgjson5.providers.abstract import AbstractProvider
from mtgjson5.utils import retryable_session

LOGGER = logging.getLogger(__name__)


@singleton
class GitHubCardSealedProductsProvider(AbstractProvider):
    """
    GitHub Card Sealed Products Provider
    """

    card_products_api_url: str = "https://github.com/mtgjson/mtg-sealed-content/raw/main/outputs/card_map.json?raw=True"
    card_uuid_to_products: Dict[str, Dict[str, List[str]]]

    def __init__(self) -> None:
        """
        Initializer
        """
        super().__init__(self._build_http_header())
        self.card_uuid_to_products = self.download(self.card_products_api_url)

    def _build_http_header(self) -> Dict[str, str]:
        return {}

    def download(
        self, url: str, params: Optional[Dict[str, Union[str, int]]] = None
    ) -> Any:
        session = retryable_session()

        response = session.get(url)
        self.log_download(response)
        if response.ok:
            return response.json()

        LOGGER.error(f"Error downloading GitHub Cards: {response} --- {response.text}")
        return []

    def get_products_card_found_in(
        self, mtgjson_uuid: str
    ) -> Optional[Dict[str, List[str]]]:
        """
        Get Card Products from UUID
        :param mtgjson_uuid: Card UUID to get products for
        :returns Card Products, if available
        """
        return self.card_uuid_to_products.get(mtgjson_uuid)
