"""
Card Kingdom 3rd party provider
"""
import json
import logging
import pathlib
import re
from typing import Any, Dict, List, Optional, Union

from singleton_decorator import singleton

from .. import constants
from ..classes import (
    MtgjsonPricesObject,
    MtgjsonSealedProductObject,
)
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
        name = re.sub(r"[^\w\s]", "", product_name)
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

    def update_sealed_product(
        self, set_name: str, sealed_products: List[MtgjsonSealedProductObject]
    ) -> None:
        """
        Builds MTGJSON Sealed Product Objects from Card Kingdom data
        :param group_id: group id for the set to get data for
        :param set_code: short abbreviation for the set name
        :return: A list of MtgjsonSealedProductObject for a given set
        """

        sealed_data = self.download(self.sealed_url)
        num_sealed = len(sealed_data["data"])
        LOGGER.debug(f"Found {num_sealed} sealed products")

        cardkingdom_sealed_products = []

        with constants.RESOURCE_PATH.joinpath("sealed_name_fixes.json").open(
            encoding="utf-8"
        ) as f:
            sealed_name_fixes = json.load(f)

        with constants.RESOURCE_PATH.joinpath(
            "cardkingdom_sealed_name_mapping.json"
        ).open(encoding="utf-8") as f:
            cardkingdom_sealed_translator = json.load(f)

        existing_names = {
            self.strip_sealed_name(product.name): product for product in sealed_products
        }

        updated_set_name = cardkingdom_sealed_translator["editions"].get(
            set_name.lower(), set_name.lower()
        )
        LOGGER.debug(", ".join({product["edition"] for product in sealed_data["data"]}))
        try:
            LOGGER.debug(", ".join([set_name, updated_set_name]))
        except TypeError:
            LOGGER.debug(", ".join([set_name] + updated_set_name))

        for product in sealed_data["data"]:
            if product["edition"].lower() != updated_set_name:
                continue

            skip_products = ["pure bulk:", "complete set", "complete foil set"]
            if any(s in product["name"].lower() for s in skip_products):
                continue

            product_name = product["name"]
            for tag, fix in sealed_name_fixes.items():
                if tag in product_name:
                    product_name = product_name.replace(tag, fix)

            check_name = self.strip_sealed_name(product_name)
            check_name = cardkingdom_sealed_translator["products"].get(
                check_name, check_name
            )

            if check_name in existing_names:
                sealed_product = existing_names[check_name]
                LOGGER.debug(f"{sealed_product.name}: adding CardKingdom values")
                sealed_product.raw_purchase_urls["cardKingdom"] = (
                    sealed_data["meta"]["base_url"]
                    + product["url"]
                    + constants.CARD_KINGDOM_REFERRAL
                )
                sealed_product.identifiers.card_kingdom_id = str(product["id"])
                continue

            sealed_product = MtgjsonSealedProductObject()

            sealed_product.name = product_name

            sealed_product.identifiers.card_kingdom_id = str(product["id"])

            sealed_product.category = sealed_product.determine_mtgjson_sealed_product_category(
                sealed_product.name.lower()
            )
            sealed_product.subtype = sealed_product.determine_mtgjson_sealed_product_subtype(
                sealed_product.name.lower(), sealed_product.category
            )

            LOGGER.debug(
                f"{sealed_product.name}: {sealed_product.category}.{sealed_product.subtype}"
            )
            sealed_product.raw_purchase_urls["cardKingdom"] = (
                sealed_data["meta"]["base_url"]
                + product["url"]
                + constants.CARD_KINGDOM_REFERRAL
            )
            cardkingdom_sealed_products.append(sealed_product)

        sealed_products.extend(cardkingdom_sealed_products)
