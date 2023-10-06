"""
Card Kingdom 3rd party provider
"""
import logging
import pathlib
import re
from typing import Any, Dict, List, Optional, Union

from singleton_decorator import singleton

from .. import constants
from ..classes import MtgjsonPricesObject, MtgjsonSealedProductObject
from ..providers.abstract import AbstractProvider
from ..utils import generate_card_mapping, retryable_session

LOGGER = logging.getLogger(__name__)


@singleton
class CardKingdomProvider(AbstractProvider):
    """
    Card Kingdom container
    """

    api_url: str = "https://api.cardkingdom.com/api/pricelist"
    sealed_url: str = "https://api.cardkingdom.com/api/sealed_pricelist"

    def __init__(self) -> None:
        """
        Initializer
        """
        super().__init__(self._build_http_header())

    def _build_http_header(self) -> Dict[str, str]:
        """
        Construct the Authorization header for CardHoarder
        :return: Authorization header
        """
        return {}

    def download(
        self, url: str, params: Optional[Dict[str, Union[str, int]]] = None
    ) -> Any:
        """
        Download content
        Api calls always return JSON
        :param url: URL to download from
        :param params: Options for URL download
        """
        session = retryable_session()
        session.headers.update(self.session_header)

        response = session.get(url)
        self.log_download(response)

        return response.json()

    def strip_sealed_name(self, product_name: str) -> str:
        """
        Cleans and strips sealed product names for easier comparison.
        """
        name = product_name.strip()
        name = re.sub(r"[^\w\s]", "", name)
        name = re.sub(" +", " ", name)
        return name.lower()

    def generate_today_price_dict(
        self, all_printings_path: pathlib.Path
    ) -> Dict[str, MtgjsonPricesObject]:
        """
        Generate a single-day price structure for MTGO from CardHoarder
        :return MTGJSON prices single day structure
        """
        request_api_response: Dict[str, Any] = self.download(self.api_url)
        price_data_rows = request_api_response.get("data", [])

        # Start with non-foil IDs
        card_kingdom_id_to_mtgjson = generate_card_mapping(
            all_printings_path, ("identifiers", "cardKingdomId"), ("uuid",)
        )

        # Then add in foil IDs
        card_kingdom_id_to_mtgjson.update(
            generate_card_mapping(
                all_printings_path, ("identifiers", "cardKingdomFoilId"), ("uuid",)
            )
        )

        default_prices_obj = MtgjsonPricesObject(
            "paper", "cardkingdom", self.today_date, "USD"
        )

        LOGGER.info("Building CardKingdom buylist & retail data")
        return super().generic_generate_today_price_dict(
            third_party_to_mtgjson=card_kingdom_id_to_mtgjson,
            price_data_rows=price_data_rows,
            card_platform_id_key="id",
            default_prices_object=default_prices_obj,
            foil_key="is_foil",
            retail_key="price_retail",
            buy_key="price_buy",
        )

    def update_sealed_urls(
        self, sealed_products: List[MtgjsonSealedProductObject]
    ) -> None:
        """
        Queries the CK sealed product API to add URLs to any sealed product with a
        Card Kingdom ID.
        """

        api_data = self.download(self.sealed_url)

        for product in sealed_products:
            try:
                ck_id = product.identifiers.card_kingdom_id
            except AttributeError:
                continue
            for remote_product in api_data["data"]:
                if str(remote_product["id"]) == ck_id:
                    product.raw_purchase_urls["cardKingdom"] = (
                        api_data["meta"]["base_url"]
                        + remote_product["url"]
                        + constants.CARD_KINGDOM_REFERRAL
                    )
                    break
            else:
                LOGGER.debug(f"No Card Kingdom URL found for product {product.name}")
