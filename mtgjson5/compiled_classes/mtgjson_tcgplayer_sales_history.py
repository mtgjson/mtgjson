"""
MTGJSON TcgplayerSalesHistory Object
"""
import json
import logging
import pathlib
from collections import defaultdict
from typing import Any, Dict, List, Union

import ratelimit
import requests
import requests_cache

from ..classes import MtgjsonSalesHistoryObject
from ..providers.tcgplayer import CardConditionV2, CardLanguageV2, CardPrintingV2
from ..utils import get_all_cards_and_tokens, parallel_call, retryable_session

LOGGER = logging.getLogger(__name__)


class MtgjsonTcgplayerSalesHistoryObject:
    """
    MTGJSON MtgjsonTcgplayerSalesHistory Object
    """

    __session: Union[requests.Session, requests_cache.CachedSession]
    __latest_sales_url = "https://mpapi.tcgplayer.com/v2/product/{}/latestsales"
    sales_history: Dict[str, Dict[str, List[Dict[str, Any]]]]

    def __init__(
        self, all_printings_path: pathlib.Path, tcgplayer_skus_path: pathlib.Path
    ) -> None:
        if not all_printings_path.exists() or not tcgplayer_skus_path.exists():
            LOGGER.error("Unable to find AllPrintings or TcgplayerSkus, can't build sales history")
            self.sales_history = {}
            return

        self.__session = retryable_session()

        with tcgplayer_skus_path.open() as f:
            tcgplayer_skus = json.load(f).get("data")

        cards_and_tokens = list(get_all_cards_and_tokens(all_printings_path))[0:1000]
        self.sales_history = parallel_call(
            self.__get_tcgplayer_sales_history,
            cards_and_tokens,
            repeatable_args=[tcgplayer_skus],
            fold_dict=True,
        )

    def __get_tcgplayer_sales_history(
        self, card: Dict[str, Any], tcgplayer_skus: Dict[str, List[Dict[str, Any]]]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Get the TCGPlayer sales history of a specific card
        :param card: MTGJSON Card Object
        :param tcgplayer_skus: MTGJSON TcgplayerSkus ingestion
        :return MTGJSON TcgplayerSalesHistory data
        """
        LOGGER.info(
            f"Getting sales for {card.get('setCode')}: {card.get('name')}"
        )
        tcgplayer_product_id = card.get("identifiers", {}).get("tcgplayerProductId")
        if tcgplayer_product_id:
            product_id_skus = tcgplayer_skus.get(card["uuid"])
            if not product_id_skus:
                LOGGER.warning(
                    f"Unable to get Product SKUs from {card.get('uuid')}, skipping"
                )
                return {}

            sales_data = self.__get_tcgplayer_latest_sales_data(tcgplayer_product_id)
            skus_associated_with_card = self.split_sales_data_by_sku(
                product_id_skus, sales_data
            )
            return {card["uuid"]: skus_associated_with_card}
        return {}

    @ratelimit.sleep_and_retry
    @ratelimit.limits(calls=7, period=1)
    def __download(
        self, tcgplayer_product_id: int, json_body: Dict[str, Any]
    ) -> requests.Response:
        """
        Download data from TCGPlayer's V2 API (Rate limited for safety)
        :param tcgplayer_product_id: TCGPlayer Product ID to get details on
        :param json_body: Pagination object
        """
        return self.__session.post(
            self.__latest_sales_url.format(tcgplayer_product_id), json=json_body
        )

    def __get_tcgplayer_latest_sales_data(
        self, tcgplayer_product_id: int
    ) -> List[Dict[str, Any]]:
        """
        Download sales data from TCGPlayer for a specific Product ID
        :param tcgplayer_product_id: TCGPlayer Product ID To download sales data on
        :return All sales from that specific TCGPlayer Product ID
        """
        return_value = []

        for offset in range(0, 10_000, 25):
            json_body = {"listingType": "All", "limit": 25, "offset": offset}
            response = self.__download(tcgplayer_product_id, json_body)

            try:
                return_value.extend(response.json().get("data"))
            except Exception as exception:
                message = "Rate limited" if "403" not in response.body else exception
                LOGGER.error(
                    f"Unable to download latest sales for {tcgplayer_product_id}: {message}"
                )
                return []

            if response.json().get("nextPage") != "Yes":
                break

        return return_value

    def split_sales_data_by_sku(
        self,
        product_id_skus: List[Dict[str, Any]],
        product_id_sales_data: List[Dict[str, Any]],
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Take a homogeneous set of sales data and split it up into individual
        SKUs (i.e. Traits) for enhanced usability
        :param product_id_skus: SKUs associated with a specific TCGPlayer Product ID
        :param product_id_sales_data: Sales data associated with a specific TCGPlayer Product ID, intermixed
        :return A properly sorted mapping of sales data per SKU of a TCGPlayer Product ID
        """
        return_value = defaultdict(list)

        traits_to_sku = {
            self.__get_sku_hash(sku): sku["skuId"] for sku in product_id_skus
        }

        for sales_entry in product_id_sales_data:
            sku_details = {
                "language": CardLanguageV2(sales_entry["language"]).name.replace(
                    "_", " "
                ),
                "printing": CardPrintingV2(sales_entry["variant"]).name.replace(
                    "_", " "
                ),
                "condition": CardConditionV2(sales_entry["condition"]).name.replace(
                    "_", " "
                ),
            }

            product_sku = traits_to_sku.get(self.__get_sku_hash(sku_details))
            if product_sku:
                mtgjson_sales_entry = self.__cleanup_sales_entry(sales_entry)
                return_value[product_sku].append(mtgjson_sales_entry)

        return dict(return_value)

    @staticmethod
    def __get_sku_hash(sku: Dict[str, Any]) -> str:
        """
        Generate a simple hash for comparisons between TCGPlayer SKUs
        :param sku: TCGPlayer SKU data
        :return Hash that can be used for comparisons
        """
        return f"{sku['condition']}-{sku['language']}-{sku['printing']}"

    @staticmethod
    def __cleanup_sales_entry(sales_entry: Dict[str, Any]) -> MtgjsonSalesHistoryObject:
        """
        Convert a TCGPlayer "dirty" sales object into a MtgjsonSalesHistoryObject
        :param sales_entry: TCGPlayer sales entry
        :return MtgjsonSalesHistoryObject, which can be sorted
        """
        custom_listing_id = sales_entry.get("customListingId")
        order_date = (
            sales_entry["orderDate"].split(".")[0].replace("T", " ")  # Fix DateTime
        )

        return_object = MtgjsonSalesHistoryObject()
        return_object.quantity = sales_entry["quantity"]
        return_object.purchase_price = sales_entry["purchasePrice"]
        return_object.shipping_price = sales_entry["shippingPrice"]
        return_object.order_date = order_date
        if custom_listing_id:
            return_object.custom_listing_id = custom_listing_id

        return return_object

    def to_json(self) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        return self.sales_history
