"""
TCGPlayer 3rd party provider
"""
import enum
import json
import logging
import pathlib
from typing import Any, Dict, List, Optional, Tuple, Union

import requests
from singleton_decorator import singleton

from ..classes import MtgjsonPricesObject
from ..providers.abstract import AbstractProvider
from ..utils import generate_card_mapping, parallel_call, retryable_session

LOGGER = logging.getLogger(__name__)


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
    __keys_found: bool

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
        headers = {"Authorization": f"Bearer {self._request_tcgplayer_bearer()}"}
        return headers

    def _request_tcgplayer_bearer(self) -> str:
        """
        Attempt to get the latest TCGPlayer Bearer token for
        API access. Use the credentials found in the local
        config to contact the server.
        :return: Empty string or current Bearer token
        """
        config = self.get_configs()

        if "TCGPlayer" not in config.sections():
            LOGGER.warning("TCGPlayer section not established. Skipping requests")
            self.__keys_found = False
            return ""

        if not (
            config.get("TCGPlayer", "client_id")
            and config.get("TCGPlayer", "client_secret")
        ):
            LOGGER.warning("TCGPlayer keys not established. Skipping requests")
            self.__keys_found = False
            return ""

        self.__keys_found = True
        tcg_post = requests.post(
            "https://api.tcgplayer.com/token",
            data={
                "grant_type": "client_credentials",
                "client_id": config.get("TCGPlayer", "client_id"),
                "client_secret": config.get("TCGPlayer", "client_secret"),
            },
        )

        if not tcg_post.ok:
            LOGGER.error(f"Unable to contact TCGPlayer. Reason: {tcg_post.reason}")
            return ""

        self.api_version = config.get("TCGPlayer", "api_version")
        request_as_json = json.loads(tcg_post.text)

        return str(request_as_json.get("access_token", ""))

    def download(self, url: str, params: Dict[str, Union[str, int]] = None) -> Any:
        """
        Download content from Scryfall
        Api calls always return JSON from Scryfall
        :param url: URL to download from
        :param params: Options for URL download
        """
        session = retryable_session()
        session.headers.update(self.session_header)
        response = session.get(
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
            api_response = self.download(
                "https://api.tcgplayer.com/[API_VERSION]/catalog/categories/1/groups",
                {"offset": str(api_offset)},
            )

            if not api_response:
                # No more entries
                break

            response = json.loads(api_response)
            if not response["results"]:
                # Something went wrong
                break

            for magic_set in response["results"]:
                magic_set_ids.append((magic_set["groupId"], magic_set["name"]))

            api_offset += len(response["results"])

        return magic_set_ids

    def generate_today_price_dict(
        self, all_printings_path: pathlib.Path
    ) -> Dict[str, MtgjsonPricesObject]:
        """
        Download the TCGPlayer pricing API and collate into MTGJSON format
        :param all_printings_path Path to AllPrintings.json for pre-processing
        :return: Prices to combine with others
        """
        if not self.__keys_found:
            LOGGER.warning("Keys not found for TCGPlayer, skipping")
            return {}

        ids_and_names = self.get_tcgplayer_magic_set_ids()
        tcg_to_mtgjson_map = generate_card_mapping(
            all_printings_path, ("identifiers", "tcgplayerProductId"), ("uuid",)
        )

        LOGGER.info("Building TCGPlayer buylist data")
        buylist_dict = parallel_call(
            get_tcgplayer_buylist_prices_map,
            ids_and_names,
            repeatable_args=[tcg_to_mtgjson_map],
            fold_dict=True,
        )

        LOGGER.info("Building TCGPlayer retail list data")
        retail_dict = parallel_call(
            get_tcgplayer_prices_map,
            ids_and_names,
            repeatable_args=[tcg_to_mtgjson_map],
            fold_dict=True,
        )

        # Deep dict merge doesn't catch this right now
        # As such, we will do the deep merge manually
        combined_listings = buylist_dict.copy()
        for key, value in combined_listings.items():
            if key in retail_dict:
                combined_listings[key].sell_normal = retail_dict[key].sell_normal
                combined_listings[key].sell_foil = retail_dict[key].sell_foil
        for key, value in retail_dict.items():
            if key not in combined_listings:
                combined_listings[key] = value

        return dict(combined_listings)


def get_tcgplayer_sku_data(group_id_and_name: Tuple[str, str]) -> List[Dict[str, Any]]:
    """
    Finds all sku data for a given group using the TCGPlayer API
    :param group_id_and_name: group id and name for the set to get data for
    :return: product data including skus to be parsed into a sku map
    """
    magic_set_product_data = []
    api_offset = 0

    while True:
        api_response = TCGPlayerProvider().download(
            "https://api.tcgplayer.com/catalog/products",
            {
                "offset": str(api_offset),
                "limit": 100,
                "categoryId": 1,
                "includeSkus": True,
                "groupId": group_id_and_name[0],
            },
        )

        if not api_response:
            # No more entries
            break

        response = json.loads(api_response)
        if not response["results"]:
            # Something went wrong
            break

        magic_set_product_data.extend(response["results"])
        api_offset += len(response["results"])

    return magic_set_product_data


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


def get_tcgplayer_buylist_prices_map(
    group_id_and_name: Tuple[str, str], tcg_to_mtgjson_map: Dict[str, str]
) -> Dict[str, MtgjsonPricesObject]:
    """
    takes a group id and name and finds all buylist data for that group
    :param group_id_and_name: TCGPlayer Set ID & Name to build
    :param tcg_to_mtgjson_map: TCGPlayer ID to MTGJSON UUID mapping
    :return: returns a map of tcgplayer buylist data to card uuids
    """
    LOGGER.debug(f"Tcgplayer Building buylist data for {group_id_and_name[1]}")
    api_response = TCGPlayerProvider().download(
        f"https://api.tcgplayer.com/pricing/buy/group/{group_id_and_name[0]}"
    )

    if not api_response:
        return {}

    response = json.loads(api_response)
    if not response["results"]:
        return {}

    prices_map: Dict[str, MtgjsonPricesObject] = {}

    tcgplayer_sku_data = get_tcgplayer_sku_data(group_id_and_name)
    sku_map = get_tcgplayer_sku_map(tcgplayer_sku_data)

    for buylist_data in response["results"]:
        product_id = str(buylist_data["productId"])
        key = tcg_to_mtgjson_map.get(product_id)
        if not key:
            continue

        for sku in buylist_data["skus"]:
            if not sku["prices"]["high"]:
                # We want the buylist high price, not buylist market price
                continue

            if not sku_map.get(product_id):
                LOGGER.debug(f"TCGPlayer ProductId {product_id} not found")
                continue

            if key not in prices_map.keys():
                prices_map[key] = MtgjsonPricesObject(
                    "paper", "tcgplayer", TCGPlayerProvider().today_date, "USD"
                )

            product_sku = sku["skuId"]

            if sku_map[product_id].get("nonfoil_sku") == product_sku:
                prices_map[key].buy_normal = sku["prices"]["high"]
            elif sku_map[product_id].get("foil_sku") == product_sku:
                prices_map[key].buy_foil = sku["prices"]["high"]

    return prices_map


def get_tcgplayer_prices_map(
    group_id_and_name: Tuple[str, str], tcg_to_mtgjson_map: Dict[str, str]
) -> Dict[str, MtgjsonPricesObject]:
    """
    Construct MtgjsonPricesObjects from TCGPlayer data
    :param group_id_and_name: TCGPlayer Set ID & Name to build
    :param tcg_to_mtgjson_map: TCGPlayer ID to MTGJSON UUID mapping
    :return: Cards with prices from Set ID & Name
    """
    api_response = TCGPlayerProvider().download(
        f"https://api.tcgplayer.com/[API_VERSION]/pricing/group/{group_id_and_name[0]}"
    )

    if not api_response:
        return {}

    response = json.loads(api_response)
    if not response["results"]:
        return {}

    prices_map: Dict[str, MtgjsonPricesObject] = {}
    for tcgplayer_object in response["results"]:
        key = tcg_to_mtgjson_map.get(str(tcgplayer_object["productId"]))
        if not key:
            continue

        is_non_foil = tcgplayer_object["subTypeName"] == "Normal"
        card_price = tcgplayer_object["marketPrice"]

        if key not in prices_map.keys():
            prices_map[key] = MtgjsonPricesObject(
                "paper", "tcgplayer", TCGPlayerProvider().today_date, "USD"
            )

        if is_non_foil:
            prices_map[key].sell_normal = card_price
        else:
            prices_map[key].sell_foil = card_price

    return prices_map


def convert_sku_data_enum(sku: Dict[str, int]) -> Dict[str, Union[int, str]]:
    """
    Converts a TCGPlayer SKU from IDs to components
    :param sku: TCGPlayer SKU component
    :return: Enhanced TCGPlayer SKU dict
    """
    return {
        "skuId": sku["skuId"],
        "productId": sku["productId"],
        "language": CardLanguage(sku["languageId"]).name.replace("_", " "),
        "printing": CardPrinting(sku["printingId"]).name.replace("_", " "),
        "condition": CardCondition(sku["conditionId"]).name.replace("_", " "),
    }
