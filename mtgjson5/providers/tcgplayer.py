"""
TCGPlayer 3rd party provider
"""
import json
import logging
import pathlib
from typing import Any, Dict, List, Tuple, Union

import requests
from singleton_decorator import singleton

from ..classes import MtgjsonPricesObject
from ..consts import CACHE_PATH
from ..providers.abstract import AbstractProvider
from ..utils import iterate_cards_and_tokens, parallel_call, retryable_session

LOGGER = logging.getLogger(__name__)


@singleton
class TCGPlayerProvider(AbstractProvider):
    """
    TCGPlayer container
    """

    api_version: str = ""
    condition_map: Dict[int, str]
    language_map: Dict[int, str]
    tcg_to_mtgjson_map: Dict[str, str]
    __keys_found: bool

    def __init__(self) -> None:
        """
        Initializer
        """
        super().__init__(self._build_http_header())

        # Keys found status established when building header
        if self.__keys_found:
            self.condition_map = self.get_tcgplayer_condition_map()
            self.language_map = self.get_tcgplayer_language_map()

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

    def get_tcgplayer_language_map(self) -> Dict[int, str]:
        """
        gets tcgplayer map for language ids to language abbreviations using the List All Category Conditions endpoint
        :return: dictionary mapping language ids to abbreviations
        """
        api_response = self.download(
            "https://api.tcgplayer.com/catalog/categories/1/languages"
        )
        language_map: Dict[int, str] = {}
        tcg_response = json.loads(api_response)
        for language in tcg_response["results"]:
            language_map[language["languageId"]] = language["abbr"]
        return language_map

    def get_tcgplayer_condition_map(self) -> Dict[int, str]:
        """
        Get condition map for language ids to language abbreviations
        using the List All Category Languages endpoint
        :return: Dict mapping condition ids to abbreviations
        """
        api_response = self.download(
            "https://api.tcgplayer.com/catalog/categories/1/conditions"
        )
        condition_map: Dict[int, str] = {}
        tcg_response = json.loads(api_response)
        for condition in tcg_response["results"]:
            condition_map[condition["conditionId"]] = condition["abbreviation"]
        return condition_map

    def get_tcgplayer_magic_set_ids(self) -> List[Tuple[str, str]]:
        """
        Download and grab all TCGPlayer set IDs for MTG
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
        with CACHE_PATH.joinpath("tcgplayer_price_map.json").open("w") as file:
            json.dump(tcg_to_mtgjson_map, file)

        ids_and_names = self.get_tcgplayer_magic_set_ids()

        results = parallel_call(get_tcgplayer_prices_map, ids_and_names, fold_dict=True)

        return dict(results)


def generate_tcgplayer_to_mtgjson_map(
    all_printings_path: pathlib.Path,
) -> Dict[str, str]:
    """
    Generate a TCGPlayerID -> MTGJSON UUID map that can be used
    across the system.
    :param all_printings_path: Path to JSON compiled version
    :return: Map of TCGPlayerID -> MTGJSON UUID
    """
    dump_map = dict()
    for card in iterate_cards_and_tokens(all_printings_path):
        try:
            dump_map[card["identifiers"]["tcgplayerProductId"]] = card["uuid"]
        except KeyError:
            pass
    return dump_map


def get_tcgplayer_prices_map(
    group_id_and_name: Tuple[str, str]
) -> Dict[str, MtgjsonPricesObject]:
    """
    Construct MtgjsonPricesObjects from TCGPlayer data
    :param group_id_and_name: TCGPlayer Set ID & Name to build
    :return: Cards with prices from Set ID & Name
    """
    with CACHE_PATH.joinpath("tcgplayer_price_map.json").open() as file:
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
