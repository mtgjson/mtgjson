"""
TCGPlayer 3rd party provider
"""
import collections
import datetime
import multiprocessing
import pathlib
from typing import Any, Dict, List, Tuple, Union

import requests

import simplejson as json
from singleton_decorator import singleton

from ..classes import MtgjsonPricesObject
from ..consts import CACHE_PATH
from ..providers.abstract_provider import AbstractProvider
from ..utils import get_thread_logger

LOGGER = get_thread_logger()


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
        file_contents = json.load(f)

    dump_map: Dict[str, str] = {}
    for value in file_contents.values():
        for card in value.get("cards") + value.get("tokens"):
            if "tcgplayerProductId" in card.keys():
                dump_map[card["tcgplayerProductId"]] = card["uuid"]

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
            prices_map[key] = MtgjsonPricesObject(key)

        if is_non_foil:
            prices_map[key].paper[TCGPlayerProvider().today_date] = card_price
        else:
            prices_map[key].paper_foil[TCGPlayerProvider().today_date] = card_price

    return prices_map


@singleton
class TCGPlayerProvider(AbstractProvider):
    """
    TCGPlayer container
    """

    today_date: str = datetime.datetime.today().strftime("%Y-%m-%d")
    api_version: str = ""
    tcg_to_mtgjson_map: Dict[str, str]

    def __init__(self) -> None:
        """
        Initializer
        """
        get_thread_logger()
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
        session = self.session_pool.popleft()
        response = session.get(
            url.replace("[API_VERSION]", self.api_version), params=params
        )
        self.session_pool.append(session)

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
    ) -> Dict[str, str]:
        """
        Download the TCGPlayer pricing API and collate into MTGJSON format
        :param all_printings_path Path to AllPrintings.json for pre-processing
        :return: Prices to combine with others
        """
        # Future ways to put this into shared memory so all threads can access
        tcg_to_mtgjson_map = generate_tcgplayer_to_mtgjson_map(all_printings_path)
        with CACHE_PATH.joinpath("tcgplayer_price_map.json").open("w") as file:
            json.dump(tcg_to_mtgjson_map, file)

        ids_and_names = self.get_tcgplayer_magic_set_ids()

        with multiprocessing.Pool(multiprocessing.cpu_count()) as pool:
            results = pool.map(get_tcgplayer_prices_map, ids_and_names)

        return dict(collections.ChainMap(*results))
