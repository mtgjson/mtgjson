"""
Sealed Products via GitHub 3rd party provider
"""
import logging
from typing import Any, Dict, List, Optional, Union

from singleton_decorator import singleton

from ..classes import (
    MtgjsonSealedProductCategory,
    MtgjsonSealedProductObject,
    MtgjsonSealedProductSubtype,
    MtgjsonSetObject,
)
from ..providers.abstract import AbstractProvider
from ..utils import retryable_session

LOGGER = logging.getLogger(__name__)


@singleton
class GitHubSealedProvider(AbstractProvider):
    """
    GitHubSealedProvider container
    """

    sealed_contents_url: str = "https://github.com/mtgjson/mtg-sealed-content/blob/main/outputs/contents.json?raw=true"
    sealed_products_url: str = "https://github.com/mtgjson/mtg-sealed-content/blob/main/outputs/products.json?raw=true"
    sealed_products: Dict[str, Any]
    sealed_contents: Dict[str, Any]

    def __init__(self) -> None:
        """
        Initializer
        """
        super().__init__(self._build_http_header())
        self.sealed_products = self.download(self.sealed_products_url)
        self.sealed_contents = self.download(self.sealed_contents_url)

    def _build_http_header(self) -> Dict[str, str]:
        """
        Construct the Authorization header
        :return: Authorization header
        """
        return {}

    def download(
        self, url: str, params: Optional[Dict[str, Union[str, int]]] = None
    ) -> Any:
        """
        Download content from GitHub
        :param url: Download URL
        :param params: Options for URL download
        """
        session = retryable_session()

        response = session.get(url)
        self.log_download(response)
        if response.ok:
            return response.json()

        LOGGER.error(
            f"Error downloading GitHub Boosters: {response} --- {response.text}"
        )
        return {}

    def get_sealed_products_data(
        self, set_code: str
    ) -> List[MtgjsonSealedProductObject]:
        """
        Grab an individual set's additional sealed products, if it exists
        :param set_code: Set to pull data from
        :return sealed product list, if applicable
        """
        LOGGER.info(f"Getting booster data for {set_code}")
        products_list = []
        for sealed_product_name, sealed_product in self.sealed_products.get(
            set_code.lower(), {}
        ).items():
            product_obj = MtgjsonSealedProductObject()
            product_obj.name = sealed_product_name
            product_obj.release_date = sealed_product.get("release_date")

            try:
                product_obj.category = getattr(
                    MtgjsonSealedProductCategory,
                    sealed_product.get("category", "UNKNOWN").upper(),
                )
            except AttributeError:
                product_obj.category = MtgjsonSealedProductCategory.UNKNOWN
            try:
                product_obj.subtype = getattr(
                    MtgjsonSealedProductSubtype,
                    sealed_product.get("subtype", "UNKNOWN").upper(),
                )
            except AttributeError:
                product_obj.subtype = MtgjsonSealedProductSubtype.UNKNOWN

            product_obj.raw_purchase_urls = sealed_product.get("purchase_url", {})
            products_list.append(product_obj)

            for location, identifier in sealed_product.get("identifiers", {}).items():
                setattr(product_obj.identifiers, location, identifier)
        return products_list

    def apply_sealed_contents_data(
        self, set_code: str, mtgjson_set: MtgjsonSetObject
    ) -> None:
        """
        Adds the sealed contents to each element of sealed_products.
        :param set_code: Code of set to update
        :param mtgjson_set: Set object to update
        """
        LOGGER.info(f"Adding sealed product contents to {set_code}")
        set_contents = self.sealed_contents.get(set_code.lower())
        if not set_contents:
            return

        for product in mtgjson_set.sealed_product:
            product_contents = set_contents.get(product.name)
            if product_contents:
                if "size" in product_contents:
                    product.product_size = product_contents.pop("size")
                if "card_count" in product_contents:
                    product.card_count = product_contents.pop("card_count")
                product.contents = product_contents
