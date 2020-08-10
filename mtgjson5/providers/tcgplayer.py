"""
TCGPlayer 3rd party provider
"""
import json
import logging
import pathlib
from typing import Any, Dict, List, Optional, Tuple, Union

import requests
from singleton_decorator import singleton

from ..classes import MtgjsonPricesObject
from ..consts import CACHE_PATH, OUTPUT_PATH
from ..providers.abstract import AbstractProvider
from ..utils import parallel_call, retryable_session

LOGGER = logging.getLogger(__name__)


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

        if tcg_post.status_code != 200:
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

        # Future ways to put this into shared memory so all threads can access
        tcg_to_mtgjson_map = generate_tcgplayer_to_mtgjson_map(all_printings_path)
        CACHE_PATH.mkdir(parents=True, exist_ok=True)
        with CACHE_PATH.joinpath("tcgplayer_price_map.json").open(
            "w", encoding="utf-8"
        ) as file:
            json.dump(tcg_to_mtgjson_map, file)

        ids_and_names = self.get_tcgplayer_magic_set_ids()

        results = parallel_call(get_tcgplayer_prices_map, ids_and_names, fold_dict=True)

        return dict(results)


def get_tcgplayer_sku_data(group_id_and_name: Tuple[str, str]) -> List[Dict]:
    """
    finds all sku data for a given group using the TCGPlayer API
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


def generate_tcgplayer_sku_map(
    tcgplayer_set_sku_data: List[Dict],
) -> Dict[str, Dict[str, Optional[int]]]:
    """
    takes product info and builds a sku map
    :param tcgplayer_set_sku_data: list of product data dicts used to a build a product id to sku map
    :return: Map of TCGPlayerID -> NM Foil and Nonfoil SKU
    """
    tcgplayer_sku_map: Dict[str, Dict[str, Optional[int]]] = {}
    for product_data in tcgplayer_set_sku_data:
        nonfoil_sku: Optional[int] = None
        foil_sku: Optional[int] = None
        for sku in product_data["skus"]:
            if (
                sku["conditionId"] == 1
                and sku["printingId"] == 1
                and sku["languageId"] == 1
            ):
                nonfoil_sku = sku["skuId"]
            elif (
                sku["conditionId"] == 1
                and sku["printingId"] == 2
                and sku["languageId"] == 1
            ):
                foil_sku = sku["skuId"]
        tcgplayer_sku_map[str(product_data["productId"])] = {
            "nonfoilSku": nonfoil_sku,
            "foilSku": foil_sku,
        }
    return tcgplayer_sku_map


def generate_tcgplayer_to_mtgjson_map(
    all_printings_path: pathlib.Path,
) -> Dict[str, str]:
    """
    Generate a TCGPlayerID -> MTGJSON UUID map that can be used
    across the system.
    :param all_printings_path: Path to JSON compiled version
    :return: Map of TCGPlayerID -> MTGJSON UUID
    """
    with all_printings_path.expanduser().open(encoding="utf-8") as f:
        file_contents = json.load(f).get("data", {})

    dump_map: Dict[str, str] = {}
    for value in file_contents.values():
        for card in value.get("cards", []) + value.get("tokens", []):
            try:
                dump_map[card["identifiers"]["tcgplayerProductId"]] = card["uuid"]
            except KeyError:
                pass

    return dump_map


def get_tcgplayer_buylist_prices_map(
    group_id_and_name: Tuple[str, str]
) -> Dict[str, MtgjsonPricesObject]:
    """
    takes a group id and name and finds all buylist data for that group
    :param group_id_and_name: TCGPlayer Set ID & Name to build
    :return: returns a map of tcgplayer buylist data to card uuids
    """
    LOGGER.info(f"Building {group_id_and_name[1]} TCGPlayer Buylist data")
    api_response = TCGPlayerProvider().download(
        f"https://api.tcgplayer.com/pricing/buy/group/{group_id_and_name[0]}"
    )
    if not api_response:
        return {}
    response = json.loads(api_response)
    if not response["results"]:
        return {}
    prices_map: Dict[str, MtgjsonPricesObject] = {}
    # when building the full system, only build this once and take it as an arg/write to a file
    uuid_map = generate_tcgplayer_to_mtgjson_map(
        OUTPUT_PATH.joinpath("AllPrintings.json")
    )
    # this is set specific, built each time unlike the uuid map
    sku_map = generate_tcgplayer_sku_map(get_tcgplayer_sku_data(group_id_and_name))
    for product_buylist_data in response["results"]:
        # checks if the product id is in the uuid map
        if not uuid_map.get(str(product_buylist_data["productId"])):
            continue
        # if it is in the uuid map we set the key for the pricing object
        key: str = uuid_map[str(product_buylist_data["productId"])]
        # parse each sku to find the near mint skus
        for sku in product_buylist_data["skus"]:
            if sku["prices"]["high"]:
                if sku["skuId"] == sku_map[str(product_buylist_data["productId"])].get(
                    "nonfoilSku"
                ):

                    if key not in prices_map.keys():
                        prices_map[key] = MtgjsonPricesObject(
                            "paper", "tcgplayer", TCGPlayerProvider().today_date
                        )

                        prices_map[key].buy_normal = sku["prices"]["high"]
                elif sku["skuId"] == sku_map[
                    str(product_buylist_data["productId"])
                ].get("foilSku"):

                    if key not in prices_map.keys():
                        prices_map[key] = MtgjsonPricesObject(
                            "paper", "tcgplayer", TCGPlayerProvider().today_date
                        )
                    prices_map[key].buy_foil = sku["prices"]["high"]
    return prices_map


def get_tcgplayer_prices_map(
    group_id_and_name: Tuple[str, str]
) -> Dict[str, MtgjsonPricesObject]:
    """
    Construct MtgjsonPricesObjects from TCGPlayer data
    :param group_id_and_name: TCGPlayer Set ID & Name to build
    :return: Cards with prices from Set ID & Name
    """

    with CACHE_PATH.joinpath("tcgplayer_price_map.json").open(encoding="utf-8") as file:
        tcg_to_mtgjson_map = json.load(file)

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
        key = tcg_to_mtgjson_map.get(str(tcgplayer_object["productId"]), 0)
        if not key:
            continue

        is_non_foil = tcgplayer_object["subTypeName"] == "Normal"
        card_price = tcgplayer_object["marketPrice"]

        if key not in prices_map.keys():
            prices_map[key] = MtgjsonPricesObject(
                "paper", "tcgplayer", TCGPlayerProvider().today_date
            )

        if is_non_foil:
            prices_map[key].sell_normal = card_price
        else:
            prices_map[key].sell_foil = card_price

    return prices_map


def generate_mtgjson_to_tcgplayer_map(
    all_printings_path: pathlib.Path,
) -> Dict[str, str]:
    """
    Generate a TCGPlayerID -> MTGJSON UUID map that can be used
    across the system.
    :param all_printings_path: Path to JSON compiled version
    :return: Map of TCGPlayerID -> MTGJSON UUID
    """
    with all_printings_path.expanduser().open(encoding="utf-8") as f:
        file_contents = json.load(f).get("data", {})

    dump_map: Dict[str, str] = {}
    for value in file_contents.values():
        for card in value.get("cards", []) + value.get("tokens", []):
            try:
                dump_map[card["uuid"]] = card["identifiers"]["tcgplayerProductId"]
            except KeyError:
                pass

    return dump_map
