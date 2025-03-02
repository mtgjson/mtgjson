"""
TCGPlayer 3rd party provider
"""
import copy
import enum
import json
import logging
import pathlib
import re
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import requests
from singleton_decorator import singleton

from ..classes import MtgjsonPricesObject, MtgjsonSealedProductObject
from ..mtgjson_config import MtgjsonConfig
from ..parallel_call import parallel_call
from ..providers.abstract import AbstractProvider
from ..utils import generate_entity_mapping

LOGGER = logging.getLogger(__name__)


class CardFinish(enum.Enum):
    """
    TCGPlayer Card Condition
    Self Driven by MTGJSON
    """

    FOIL_ETCHED = "Foil Etched"

    @classmethod
    def has_value(cls, value: str) -> bool:
        """
        See if value exists in the Enum class
        :returns If value exists or not
        """
        return any(x.value == value for x in CardFinish)


class CardCondition(enum.Enum):
    """
    TCGPlayer Card Condition IDs
    """

    NEAR_MINT = 1
    LIGHTLY_PLAYED = 2
    MODERATELY_PLAYED = 3
    HEAVILY_PLAYED = 4
    DAMAGED = 5
    UNOPENED = 6


class CardPrinting(enum.Enum):
    """
    TCGPlayer Printing IDs
    """

    NON_FOIL = 1
    FOIL = 2


class CardLanguage(enum.Enum):
    """
    TCGPlayer Language IDs
    """

    ENGLISH = 1
    CHINESE_SIMPLIFIED = 2
    CHINESE_TRADITIONAL = 3
    FRENCH = 4
    GERMAN = 5
    ITALIAN = 6
    JAPANESE = 7
    KOREAN = 8
    PORTUGUESE_BRAZIL = 9
    RUSSIAN = 10
    SPANISH = 11


@singleton
class TCGPlayerProvider(AbstractProvider):
    """
    TCGPlayer container
    """

    api_version: str = ""
    tcg_to_mtgjson_map: Dict[str, str]
    product_types = [
        "Booster Box",
        "Booster Pack",
        "Sealed Products",
        "Intro Pack",
        "Fat Pack",
        "Box Sets",
        "Precon/Event Decks",
        "Magic Deck Pack",
        "Magic Booster Box Case",
        "All 5 Intro Packs",
        "Intro Pack Display",
        "3x Magic Booster Packs",
        "Booster Battle Pack",
    ]
    product_default_size = {
        "set": 30,
        "collector": 12,
        "default": 36,
        "jumpstart": 18,
        "theme": 12,
    }
    product_url = (
        "https://partner.tcgplayer.com/c/4948039/1780961/21018?subId1=api&u="
        "https%3A%2F%2Fwww.tcgplayer.com%2Fproduct%2F{}%3Fpage%3D1"
    )

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
        return {"Authorization": f"Bearer {self._request_tcgplayer_bearer()}"}

    def _request_tcgplayer_bearer(self) -> str:
        """
        Attempt to get the latest TCGPlayer Bearer token for
        API access. Use the credentials found in the local
        config to contact the server.
        :return: Empty string or current Bearer token
        """

        if not MtgjsonConfig().has_section("TCGPlayer"):
            LOGGER.warning(
                "TCGPlayer config section not established. Skipping requests"
            )
            return ""

        if not (
            MtgjsonConfig().has_option("TCGPlayer", "client_id")
            and MtgjsonConfig().has_option("TCGPlayer", "client_secret")
        ):
            LOGGER.warning("TCGPlayer keys not established. Skipping requests")
            return ""

        tcg_post = requests.post(
            "https://api.tcgplayer.com/token",
            data={
                "grant_type": "client_credentials",
                "client_id": MtgjsonConfig().get("TCGPlayer", "client_id"),
                "client_secret": MtgjsonConfig().get("TCGPlayer", "client_secret"),
            },
            timeout=60,
        )

        if not tcg_post.ok:
            LOGGER.error(f"Unable to contact TCGPlayer. Reason: {tcg_post.reason}")
            return ""

        api_version = MtgjsonConfig().has_option("TCGPlayer", "api_version")
        self.api_version = (
            MtgjsonConfig().get("TCGPlayer", "api_version")
            if api_version
            else "v1.39.0"
        )
        request_as_json = json.loads(tcg_post.text)

        return str(request_as_json.get("access_token", ""))

    def download(
        self, url: str, params: Optional[Dict[str, Union[str, int]]] = None
    ) -> Any:
        """
        Download content from Scryfall
        Api calls always return JSON from Scryfall
        :param url: URL to download from
        :param params: Options for URL download
        """
        response = self.session.get(
            url.replace("[API_VERSION]", self.api_version), params=params
        )
        self.log_download(response)
        return response.content.decode()

    def get_tcgplayer_magic_set_ids(self) -> List[Tuple[str, str]]:
        """
        Download and grab all TCGPlayer set IDs for Magic: the Gathering
        :return: List of TCGPlayer Magic sets
        """
        magic_set_ids = []
        api_offset = 0

        while True:
            response = self.get_api_results(
                "https://api.tcgplayer.com/[API_VERSION]/catalog/categories/1/groups",
                {"offset": str(api_offset)},
            )

            if not response:
                # No more entries
                break

            for magic_set in response:
                magic_set_ids.append((magic_set["groupId"], magic_set["name"]))

            api_offset += len(response)

        return magic_set_ids

    def generate_today_price_dict(
        self, all_printings_path: pathlib.Path
    ) -> Dict[str, MtgjsonPricesObject]:
        """
        Download the TCGPlayer pricing API and collate into MTGJSON format
        :param all_printings_path Path to AllPrintings.json for pre-processing
        :return: Prices to combine with others
        """
        ids_and_names = self.get_tcgplayer_magic_set_ids()
        tcg_foil_and_non_foil_to_mtgjson_map = generate_entity_mapping(
            all_printings_path, ("identifiers", "tcgplayerProductId"), ("uuid",)
        )
        tcg_etched_foil_to_mtgjson_map = generate_entity_mapping(
            all_printings_path,
            ("identifiers", "tcgplayerEtchedProductId"),
            ("uuid",),
        )

        LOGGER.info("Building TCGPlayer buylist data")
        buylist_dict = parallel_call(
            get_tcgplayer_buylist_prices_map,
            ids_and_names,
            repeatable_args=[
                tcg_foil_and_non_foil_to_mtgjson_map,
                tcg_etched_foil_to_mtgjson_map,
            ],
            fold_dict=True,
        )

        LOGGER.info("Building TCGPlayer retail list data")
        retail_dict = parallel_call(
            get_tcgplayer_prices_map,
            ids_and_names,
            repeatable_args=[
                tcg_foil_and_non_foil_to_mtgjson_map,
                tcg_etched_foil_to_mtgjson_map,
            ],
            fold_dict=True,
        )

        # Deep dict merge doesn't catch this right now
        # As such, we will do the deep merge manually
        combined_listings = buylist_dict.copy()
        for key, value in combined_listings.items():
            for retail_key, retail_value in retail_dict.get(key, {}).items():
                if retail_value:
                    setattr(combined_listings[key], retail_key, retail_value)
        for key, value in retail_dict.items():
            if key not in combined_listings:
                combined_listings[key] = value

        return dict(combined_listings)

    @staticmethod
    def update_sealed_urls(sealed_products: List[MtgjsonSealedProductObject]) -> None:
        """
        Queries the TCGPlayer sealed product API to add URLs to any sealed product with a
        TCGPlayer ID.
        :param sealed_products: Sealed products within the set
        """
        for sealed_product in sealed_products:
            if sealed_product.identifiers.tcgplayer_product_id:
                sealed_product.raw_purchase_urls[
                    "tcgplayer"
                ] = TCGPlayerProvider().product_url.format(
                    sealed_product.identifiers.tcgplayer_product_id
                )

    def get_tcgplayer_sku_data(
        self, group_id_and_name: Tuple[str, str]
    ) -> List[Dict[str, Any]]:
        """
        Finds all sku data for a given group using the TCGPlayer API
        :param group_id_and_name: group id and name for the set to get data for
        :return: product data including skus to be parsed into a sku map
        """
        magic_set_product_data = []
        api_offset = 0

        while True:
            results = self.get_api_results(
                "https://api.tcgplayer.com/catalog/products",
                {
                    "offset": str(api_offset),
                    "limit": 100,
                    "categoryId": 1,
                    "includeSkus": True,
                    "groupId": group_id_and_name[0],
                },
            )

            if not results:
                # No more entries
                break

            magic_set_product_data.extend(results)
            api_offset += len(results)

        return magic_set_product_data

    def get_tcgplayer_sealed_data(
        self, group_id: Optional[int]
    ) -> List[Dict[str, Any]]:
        """
        Finds all sealed product for a given group
        :param group_id: group id for the set to get data for
        :return: sealed product data with extended fields
        """
        magic_set_sealed_data = []
        api_offset = 0

        while True:
            results = self.get_api_results(
                "https://api.tcgplayer.com/catalog/products",
                {
                    "offset": str(api_offset),
                    "limit": 100,
                    "categoryId": 1,
                    "groupId": str(group_id),
                    "getExtendedFields": True,
                    "productTypes": ",".join(TCGPlayerProvider().product_types),
                },
            )

            if not results:
                # No more entries
                break

            magic_set_sealed_data.extend(results)
            api_offset += len(results)

            # If we got fewer results than requested, no more data is needed
            if len(results) < 100:
                break

        return magic_set_sealed_data

    @staticmethod
    def get_tcgplayer_sku_map(
        tcgplayer_set_sku_data: List[Dict[str, Any]],
    ) -> Dict[str, Dict[str, Optional[int]]]:
        """
        takes product info and builds a sku map
        :param tcgplayer_set_sku_data: list of product data dicts used to a build a product id to sku map
        :return: Map of TCGPlayerID -> NM Foil and Nonfoil SKU
        """
        tcgplayer_sku_map = {}

        for product_data in tcgplayer_set_sku_data:
            map_entry = {}

            for sku in product_data["skus"]:
                if CardCondition(sku["conditionId"]) is not CardCondition.NEAR_MINT:
                    continue

                if CardLanguage(sku["languageId"]) is not CardLanguage.ENGLISH:
                    continue

                if CardPrinting(sku["printingId"]) is CardPrinting.NON_FOIL:
                    map_entry["nonfoil_sku"] = sku["skuId"]
                elif CardPrinting(sku["printingId"]) is CardPrinting.FOIL:
                    map_entry["foil_sku"] = sku["skuId"]
                else:
                    LOGGER.warning(f"TCGPlayer unidentified printing: {sku}")

            product_id = str(product_data["productId"])
            tcgplayer_sku_map[product_id] = map_entry

        return tcgplayer_sku_map

    def get_api_results(
        self, tcg_api_url: str, params: Optional[Dict[str, Union[str, int]]] = None
    ) -> List[Dict[str, Any]]:
        """
        Get TCGPlayer API Results Object
        :param tcg_api_url: Url to get data from
        :param params: Params to pass to API call
        :returns Pricing objects
        """
        api_response = self.download(tcg_api_url, params)
        if not api_response:
            return []

        try:
            response = json.loads(api_response)
        except json.decoder.JSONDecodeError:
            LOGGER.error(f"Unable to decode TCGPlayer API Response {api_response}")
            return []

        return list(response.get("results", []))

    @staticmethod
    def get_card_finish(card_name: str) -> Optional[str]:
        """
        Determine a card's TCGPlayer finish based on the card name,
        as TCGPlayer indicates their finishes by ending a card's name
        with "(Finish)". This can be a bit wonky for some edge cases,
        but overall this should be good enough.
        :param card_name: Card name from TCGPlayer
        :return Card finish, if one is found
        """
        result_card_finish = None

        card_finishes = re.findall(r"\(([^)0-9]+)\)", card_name)
        for card_finish in card_finishes:
            if not CardFinish.has_value(card_finish):
                continue

            result_card_finish = CardFinish(card_finish).name.replace("_", " ")
            break

        return result_card_finish

    def convert_sku_data_enum(
        self, product: Dict[str, Any]
    ) -> List[Dict[str, Union[int, str]]]:
        """
        Converts a TCGPlayer Product's SKUs from IDs to components
        :param product: TCGPlayer Product
        :return: Enhanced List of TCGPlayer SKU dict objects
        """
        results = []

        name = product["name"]
        card_finish = self.get_card_finish(name)

        skus = product["skus"]
        for sku in skus:
            entry = {
                "skuId": sku["skuId"],
                "productId": sku["productId"],
                "language": CardLanguage(sku["languageId"]).name.replace("_", " "),
                "printing": CardPrinting(sku["printingId"]).name.replace("_", " "),
                "condition": CardCondition(sku["conditionId"]).name.replace("_", " "),
            }
            if card_finish:
                entry["finish"] = card_finish
            results.append(entry)

        return results


def get_tcgplayer_buylist_prices_map(
    group_id_and_name: Tuple[str, str],
    tcg_foil_and_non_foil_to_mtgjson_map: Dict[str, Set[str]],
    tcg_etched_foil_to_mtgjson_map: Dict[str, Set[str]],
) -> Dict[str, MtgjsonPricesObject]:
    """
    takes a group id and name and finds all buylist data for that group
    :param group_id_and_name: TCGPlayer Set ID & Name to build
    :param tcg_foil_and_non_foil_to_mtgjson_map: TCGPlayer ID to MTGJSON UUID mapping
    :param tcg_etched_foil_to_mtgjson_map: TCGPlayer ID to MTGJSON UUID mapping
    :return: returns a map of tcgplayer buylist data to card uuids
    """
    LOGGER.debug(f"Tcgplayer Building buylist data for {group_id_and_name[1]}")

    results = TCGPlayerProvider().get_api_results(
        f"https://api.tcgplayer.com/pricing/buy/group/{group_id_and_name[0]}"
    )
    if not results:
        return {}

    prices_map: Dict[str, MtgjsonPricesObject] = defaultdict(
        lambda: copy.copy(
            MtgjsonPricesObject(
                "paper", "tcgplayer", TCGPlayerProvider().today_date, "USD"
            )
        )
    )

    tcgplayer_sku_data = TCGPlayerProvider().get_tcgplayer_sku_data(group_id_and_name)
    sku_map = TCGPlayerProvider().get_tcgplayer_sku_map(tcgplayer_sku_data)

    for buylist_data in results:
        product_id = str(buylist_data["productId"])
        keys_are_etched = False
        keys = tcg_foil_and_non_foil_to_mtgjson_map.get(product_id)
        if not keys:
            keys_are_etched = True
            keys = tcg_etched_foil_to_mtgjson_map.get(product_id)
            if not keys:
                continue

        for sku in buylist_data["skus"]:
            if not sku["prices"]["high"]:
                # We want the buylist high price, not buylist market price
                continue

            if not sku_map.get(product_id):
                LOGGER.debug(f"TCGPlayer ProductId {product_id} not found")
                continue

            for key in keys:
                product_sku = sku["skuId"]

                if sku_map[product_id].get("nonfoil_sku") == product_sku:
                    prices_map[key].buy_normal = sku["prices"]["high"]
                elif sku_map[product_id].get("foil_sku") == product_sku:
                    if keys_are_etched:
                        prices_map[key].buy_etched = sku["prices"]["high"]
                    else:
                        prices_map[key].buy_foil = sku["prices"]["high"]

    return prices_map


def get_tcgplayer_prices_map(
    group_id_and_name: Tuple[str, str],
    tcg_foil_and_non_foil_to_mtgjson_map: Dict[str, Set[str]],
    tcg_etched_foil_to_mtgjson_map: Dict[str, Set[str]],
) -> Dict[str, MtgjsonPricesObject]:
    """
    Construct MtgjsonPricesObjects from TCGPlayer data
    :param group_id_and_name: TCGPlayer Set ID & Name to build
    :param tcg_foil_and_non_foil_to_mtgjson_map: TCGPlayer ID to MTGJSON UUID mapping
    :param tcg_etched_foil_to_mtgjson_map: TCGPlayer ID to MTGJSON UUID mapping
    :return: Cards with prices from Set ID & Name
    """
    results = TCGPlayerProvider().get_api_results(
        f"https://api.tcgplayer.com/[API_VERSION]/pricing/group/{group_id_and_name[0]}"
    )
    if not results:
        return {}

    prices_map: Dict[str, MtgjsonPricesObject] = {}
    for tcgplayer_object in results:
        product_id = str(tcgplayer_object["productId"])
        keys_are_etched = False
        keys = tcg_foil_and_non_foil_to_mtgjson_map.get(product_id)
        if not keys:
            keys_are_etched = True
            keys = tcg_etched_foil_to_mtgjson_map.get(product_id)
        if not keys:
            continue

        is_non_foil = tcgplayer_object["subTypeName"] == "Normal"
        card_price = tcgplayer_object["marketPrice"]

        for key in keys:
            if key not in prices_map:
                prices_map[key] = MtgjsonPricesObject(
                    "paper", "tcgplayer", TCGPlayerProvider().today_date, "USD"
                )

            if is_non_foil:
                prices_map[key].sell_normal = card_price
            elif keys_are_etched:
                prices_map[key].sell_etched = card_price
            else:
                prices_map[key].sell_foil = card_price

    return prices_map
