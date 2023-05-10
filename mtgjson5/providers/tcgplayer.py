"""
TCGPlayer 3rd party provider
"""
import enum
import json
import logging
import pathlib
import re
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import requests
from singleton_decorator import singleton

from .. import constants
from ..classes import (
    MtgjsonPricesObject,
    MtgjsonSealedProductCategory,
    MtgjsonSealedProductObject,
    MtgjsonSealedProductSubtype,
)
from ..mtgjson_config import MtgjsonConfig
from ..providers.abstract import AbstractProvider
from ..utils import generate_card_mapping, parallel_call, retryable_session

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
    __keys_found: bool
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

        if not MtgjsonConfig().has_section("TCGPlayer"):
            LOGGER.warning("TCGPlayer section not established. Skipping requests")
            self.__keys_found = False
            return ""

        if not (
            MtgjsonConfig().has_option("TCGPlayer", "client_id")
            and MtgjsonConfig().has_option("TCGPlayer", "client_secret")
        ):
            LOGGER.warning("TCGPlayer keys not established. Skipping requests")
            self.__keys_found = False
            return ""

        self.__keys_found = True
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
        tcg_to_mtgjson_map.update(
            generate_card_mapping(
                all_printings_path,
                ("identifiers", "tcgplayerEtchedProductId"),
                ("uuid",),
            )
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

    def determine_mtgjson_sealed_product_category(
        self, product_name: str
    ) -> MtgjsonSealedProductCategory:
        """
        Best-effort to parse the product name and determine the sealed product category
        :param product_name Name of the product from TCG, must be lowercase
        :return: category
        """

        # The order of the checks is important: for example, we need to catch all 'case'
        # products before everything else, since "booster box case" would be caught in
        # the 'booster box' check. The same applies to several sub products, such as
        # 'prerelease' (vs guild kit), 'box set' (vs set boosters), and so on.
        if any(
            tag in product_name
            for tag in [
                "booster case",
                "box case",
                "bundle case",
                "display case",
                "intro display",
                "intro pack display",
                "pack box",
                "pack case",
                "tournament pack display",
                "vip edition box",
            ]
        ):
            return MtgjsonSealedProductCategory.CASE

        if any(
            tag in product_name
            for tag in ["booster box", "booster display", "mythic edition"]
        ):
            return MtgjsonSealedProductCategory.BOOSTER_BOX

        # Needs to be before BOOSTER_PACK due to aliasing
        if any(
            tag in product_name
            for tag in [
                "blister pack",
                "booster draft pack",
                "booster packs draft set",
                "multipack",
            ]
        ):
            return MtgjsonSealedProductCategory.DRAFT_SET

        if any(
            tag in product_name
            for tag in [
                "booster pack",
                "booster retail pack",
                "box topper",
                "hanger pack",
                "omega pack",
                "promo pack",
                "theme booster",
                "vip edition pack",
            ]
        ):
            if "set of" in product_name:
                return MtgjsonSealedProductCategory.SUBSET
            return MtgjsonSealedProductCategory.BOOSTER_PACK

        if "prerelease" in product_name:
            return MtgjsonSealedProductCategory.PRERELEASE_PACK

        if any(
            tag in product_name
            for tag in [
                "booster battle pack",
                "clash pack",
                "fourth edition gift box",
                "quick start set",
                "two player starter",
            ]
        ):
            return MtgjsonSealedProductCategory.TWO_PLAYER_STARTER_SET

        if any(
            tag in product_name
            for tag in [
                "box set",
                "builders toolkit",
                "commander collection",
                "deckmasters tin",
                "deluxe collection",
                "edition box",
                "game night",
                "global series",
                "hascon",
                "modern event deck",
                "planechase",
                "planeswalker set",
                "premium deck series",
                "sdcc",
                "secret lair",
                "signature spellbook",
            ]
        ):
            # In this section, only Planechase may have a "set of"
            if "planechase" in product_name and "set of" in product_name:
                return MtgjsonSealedProductCategory.CASE
            return MtgjsonSealedProductCategory.BOX_SET

        if "commander" in product_name or "brawl deck" in product_name:
            if "set of" in product_name:
                return MtgjsonSealedProductCategory.CASE
            return MtgjsonSealedProductCategory.COMMANDER_DECK

        if "deck box" in product_name or "deck display" in product_name:
            return MtgjsonSealedProductCategory.DECK_BOX

        if any(
            tag in product_name
            for tag in [
                "challenge deck",
                "challenger deck",
                "championship deck",
                "event deck",
                "guild kit",
                "intro pack",
                "planeswalker deck",
                "starter deck",
                "theme deck",
                "tournament deck",
                "tournament pack",
            ]
        ):
            if "set of" in product_name:
                return MtgjsonSealedProductCategory.SUBSET
            return MtgjsonSealedProductCategory.DECK

        if any(
            tag in product_name
            for tag in [
                "bundle",
                "fat pack",
                "gift box",
            ]
        ):
            return MtgjsonSealedProductCategory.BUNDLE

        if "land station" in product_name:
            return MtgjsonSealedProductCategory.LAND_STATION

        return MtgjsonSealedProductCategory.UNKNOWN

    # Best-effort to parse the product name and determine the sealed product category
    def determine_mtgjson_sealed_product_subtype(
        self, product_name: str, category: MtgjsonSealedProductCategory
    ) -> MtgjsonSealedProductSubtype:
        """
        Best-effort to parse the product name and determine the sealed product subtype
        :param product_name Name of the product from TCG
        :param category Category as parsed from determine_mtgjson_sealed_product_category()
        :return: subtype
        """
        if category == MtgjsonSealedProductCategory.UNKNOWN:
            return MtgjsonSealedProductSubtype.UNKNOWN

        for subtype in MtgjsonSealedProductSubtype:
            if subtype is MtgjsonSealedProductSubtype.UNKNOWN:
                continue

            # Prevent aliasing from Eventide
            if (
                subtype is MtgjsonSealedProductSubtype.EVENT
                and category is not MtgjsonSealedProductCategory.DECK
            ):
                continue

            # Prevent assigning 'set' or 'draft_set' to Core Set editions
            if "core set" in product_name and "set" in subtype.value:
                continue

            # Skip 'collector' for this set, since they weren't introduced it yet
            if (
                "ravnica allegiance" in product_name
                and subtype is MtgjsonSealedProductSubtype.COLLECTOR
            ):
                continue

            # Prevent assigning 'set' (for set boosters) to unrelated categories
            if subtype is MtgjsonSealedProductSubtype.SET and (
                category is not MtgjsonSealedProductCategory.BOOSTER_PACK
                and category is not MtgjsonSealedProductCategory.BOOSTER_BOX
            ):
                continue

            # Do the replace to use the tag as text
            if subtype.value.replace("_", " ") in product_name:
                return subtype

        # Special handling because sometimes 'default' is not tagged
        if category in [
            MtgjsonSealedProductCategory.BOOSTER_BOX,
            MtgjsonSealedProductCategory.BOOSTER_PACK,
            MtgjsonSealedProductCategory.DRAFT_SET,
        ]:
            return MtgjsonSealedProductSubtype.DEFAULT
        return MtgjsonSealedProductSubtype.UNKNOWN

    def generate_mtgjson_sealed_product_objects(
        self, group_id: Optional[int], set_code: str
    ) -> List[MtgjsonSealedProductObject]:
        """
        Builds MTGJSON Sealed Product Objects from TCGPlayer data
        :param group_id: group id for the set to get data for
        :param set_code: short abbreviation for the set name
        :return: A list of MtgjsonSealedProductObject for a given set
        """
        if not self.__keys_found:
            LOGGER.warning("Keys not found for TCGPlayer, skipping")
            return []

        # Skip request if there is no group_id
        if group_id is None:
            return []

        sealed_data = get_tcgplayer_sealed_data(group_id)

        # adjust for worlds decks by looking at the last two digits being present in product name
        if set_code in ["WC97", "WC98", "WC99", "WC00", "WC01", "WC02", "WC03", "WC04"]:
            sealed_data = [
                product
                for product in sealed_data
                if set_code[:-2] in product["cleanName"]
            ]

        # adjust for mystery booster
        if set_code == "CMB1":
            sealed_data = [
                product for product in sealed_data if "2021" not in product["cleanName"]
            ]
        elif set_code == "CMB2":
            sealed_data = [
                product for product in sealed_data if "2021" in product["cleanName"]
            ]

        mtgjson_sealed_products = []

        with constants.RESOURCE_PATH.joinpath("sealed_name_fixes.json").open(
            encoding="utf-8"
        ) as f:
            sealed_name_fixes = json.load(f)

        with constants.RESOURCE_PATH.joinpath("booster_box_size_overrides.json").open(
            encoding="utf-8"
        ) as f:
            booster_box_size_overrides = json.load(f)

        for product in sealed_data:
            sealed_product = MtgjsonSealedProductObject()

            sealed_product.name = product["cleanName"]
            for tag, fix in sealed_name_fixes.items():
                if tag in sealed_product.name:
                    sealed_product.name = sealed_product.name.replace(tag, fix)

            sealed_product.identifiers.tcgplayer_product_id = str(product["productId"])
            sealed_product.release_date = product["presaleInfo"].get("releasedOn")

            sealed_product.category = self.determine_mtgjson_sealed_product_category(
                sealed_product.name.lower()
            )
            sealed_product.subtype = self.determine_mtgjson_sealed_product_subtype(
                sealed_product.name.lower(), sealed_product.category
            )

            LOGGER.debug(
                f"{sealed_product.name}: {sealed_product.category.value}.{sealed_product.subtype.value}"
            )

            if sealed_product.category == MtgjsonSealedProductCategory.BOOSTER_BOX:
                sealed_product.product_size = int(
                    booster_box_size_overrides.get(
                        sealed_product.subtype.value, {}
                    ).get(
                        set_code,
                        self.product_default_size.get(sealed_product.subtype.value, 0),
                    )
                )
            elif sealed_product.category == MtgjsonSealedProductCategory.DRAFT_SET:
                # Use the last number found in the name to skip years and multipliers
                numbers = re.findall(r"[0-9]+", sealed_product.name)
                if numbers:
                    sealed_product.product_size = int(numbers.pop())

            if sealed_product.release_date is not None:
                sealed_product.release_date = sealed_product.release_date[0:10]
            sealed_product.raw_purchase_urls[
                "tcgplayer"
            ] = f"https://shop.tcgplayer.com/product/productsearch?id={sealed_product.identifiers.tcgplayer_product_id}&utm_campaign=affiliate&utm_medium=api&utm_source=mtgjson"
            mtgjson_sealed_products.append(sealed_product)

        return mtgjson_sealed_products


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


def get_tcgplayer_sealed_data(group_id: Optional[int]) -> List[Dict[str, Any]]:
    """
    Finds all sealed product for a given group
    :param group_id: group id for the set to get data for
    :return: sealed product data with extended fields
    """
    magic_set_sealed_data = []
    api_offset = 0

    while True:
        api_response = TCGPlayerProvider().download(
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

        if not api_response:
            # No more entries
            break

        response = json.loads(api_response)
        if not response["results"]:
            LOGGER.warning(f"Issue with Sealed Product for Group ID: {group_id}")
            break

        magic_set_sealed_data.extend(response["results"])
        api_offset += len(response["results"])

        # If we got fewer results than requested, no more data is needed
        if len(response["results"]) < 100:
            break

    return magic_set_sealed_data


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
    group_id_and_name: Tuple[str, str], tcg_to_mtgjson_map: Dict[str, Set[str]]
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
        keys = tcg_to_mtgjson_map.get(product_id)
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
                if key not in prices_map:
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
        keys = tcg_to_mtgjson_map.get(str(tcgplayer_object["productId"]))
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
            else:
                prices_map[key].sell_foil = card_price

    return prices_map


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


def convert_sku_data_enum(product: Dict[str, Any]) -> List[Dict[str, Union[int, str]]]:
    """
    Converts a TCGPlayer Product's SKUs from IDs to components
    :param product: TCGPlayer Product
    :return: Enhanced List of TCGPlayer SKU dict objects
    """
    results = []

    name = product["name"]
    card_finish = get_card_finish(name)

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
