"""TCGPlayer retrieval and processing."""
import configparser
import contextvars
import hashlib
import json
import logging
import pathlib
from typing import Any, Dict, List

import mtgjson4
from mtgjson4 import util
import requests


class TCGPlayer:
    """
    TCGPlayer downloader class
    """

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        self.session: requests.Session = self.__get_session()
        self.api_version: contextvars.ContextVar = contextvars.ContextVar(
            "API_TCGPLAYER"
        )

    def __get_session(self) -> requests.Session:
        """Get or create a requests session for TCGPlayer."""
        tmp_session = requests.Session()
        header_auth = {
            "Authorization": "Bearer " + self._request_tcgplayer_bearer()
        }
        tmp_session.headers.update(header_auth)
        tmp_session = util.retryable_session(tmp_session)
        return tmp_session

    def _request_tcgplayer_bearer(self) -> str:
        """
        Attempt to get the latest TCGPlayer Bearer token for
        API access. Use the credentials found in the local
        config to contact the server.
        :return: Empty string or current Bearer token
        """
        if not pathlib.Path(mtgjson4.CONFIG_PATH).is_file():
            self.logger.error(
                "Unable to import TCGPlayer keys. Config at {} not found".format(
                    mtgjson4.CONFIG_PATH
                )
            )
            return ""

        config = configparser.RawConfigParser()
        config.read(mtgjson4.CONFIG_PATH)

        tcg_post = requests.post(
            "https://api.tcgplayer.com/token",
            data={
                "grant_type": "client_credentials",
                "client_id": config.get("TCGPlayer", "client_id"),
                "client_secret": config.get("TCGPlayer", "client_secret"),
            },
        )

        if tcg_post.status_code != 200:
            self.logger.error(
                "Unable to contact TCGPlayer. Reason: {}".format(tcg_post.reason)
            )
            return ""

        self.api_version.set(config.get("TCGPlayer", "api_version"))
        request_as_json = json.loads(tcg_post.text)
        return str(request_as_json.get("access_token", ""))

    def download(self, tcgplayer_url: str, params_str: Dict[str, Any] = None) -> str:
        """
        Download content from TCGPlayer with a given URL that
        can include a wildcard for default API version, as well
        as a way to pass in custom params to the URL
        :param tcgplayer_url: URL to get information from
        :param params_str: Additional params to pass to TCGPlayer call
        :return: Data from TCGPlayer API Call
        """
        if params_str is None:
            params_str = {}

        response = self.session.get(
            url=tcgplayer_url.replace("[API_VERSION]", self.api_version.get("")),
            params=params_str,
            timeout=5.0,
        )

        self.logger.info("Downloaded URL: {0}".format(response.url))

        if response.status_code != 200:
            if response.status_code == 404:
                self.logger.info(
                    "Status Code: {} Failed to download from TCGPlayer with URL: {}, Params: {}".format(
                        response.status_code, response.url, params_str
                    )
                )
            else:
                self.logger.warning(
                    "Status Code: {} Failed to download from TCGPlayer with URL: {}, Params: {}".format(
                        response.status_code, response.url, params_str
                    )
                )
            return ""

        return response.text

    def get_group_id_cards(self, group_id: int) -> List[Dict[str, Any]]:
        """
        Given a group_id, get all the cards within that set.
        :param group_id: Set to get all cards from
        :return: List of card objects
        """
        if group_id < 0:
            self.logger.error(
                "Unable to get cards from a negative group_id: {}".format(group_id)
            )
            return []

        cards: List[Dict[str, Any]] = []
        offset = 0

        while True:
            response = self.download(
                "http://api.tcgplayer.com/[API_VERSION]/catalog/products",
                {
                    "categoryId": "1",  # MTG
                    "groupId": group_id,
                    "productTypes": "Cards",  # Cards only
                    "limit": 100,
                    "offset": offset,
                },
            )

            if not response:
                break

            tcg_data = json.loads(response)

            if not tcg_data["results"]:
                break

            cards += tcg_data["results"]
            offset += len(tcg_data["results"])

        return cards

    def get_card_property(
        self, card_name: str, card_list: List[Dict[str, Any]], card_field: str
    ) -> Any:
        """
        Go through the passed in card object list to find the matching
        card from the set and get its attribute.
        :param card_name: Card name to find in the list
        :param card_list: List of TCGPlayer card objects
        :param card_field: Field to pull from TCGPlayer card object
        :return: Value of field
        """
        for card in card_list:
            if card_name.lower() == card["name"].lower():
                return card.get(card_field, None)

        self.logger.warning(
            "Unable to find card {} in TCGPlayer card list".format(card_name)
        )
        return None

    @staticmethod
    def url_keygen(prod_id: int) -> str:
        """
        Generates a key that MTGJSON will use for redirection
        :param prod_id: Seed
        :return: URL Key
        """
        return hashlib.sha256(str(prod_id).encode()).hexdigest()[:16]

    def log_redirection_url(self, prod_id: int, send_url: str) -> str:
        """
        Create the URL that can be accessed to get the TCGPlayer URL.
        Also builds up the redirection table, that can be called later.
        :param prod_id: ID of card/object
        :param send_url: URL to forward to
        :return: URL that can be used
        """
        key = self.url_keygen(prod_id)
        partner_string = "?partner=mtgjson&utm_campaign=affiliate&utm_medium=mtgjson&utm_source=mtgjson"

        self.write_tcgplayer_information({key: send_url + partner_string})
        return "https://mtgjson.com/links/{}".format(key)

    @staticmethod
    def write_tcgplayer_information(data: Dict[str, str]) -> None:
        """
        Write out the tcgplayer redirection keys to file
        :param data: tcg content
        """
        mtgjson4.COMPILED_OUTPUT_DIR.mkdir(exist_ok=True)
        with pathlib.Path(
            mtgjson4.COMPILED_OUTPUT_DIR, mtgjson4.REFERRAL_DB_OUTPUT + ".json"
        ).open("a", encoding="utf-8") as f:
            for key, value in data.items():
                f.write("{}\t{}\n".format(key, value))
