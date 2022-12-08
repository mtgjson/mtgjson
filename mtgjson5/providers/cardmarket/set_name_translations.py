import re
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple, Union

import bs4
import requests
import requests_cache
from singleton_decorator import singleton

from ...classes import MtgjsonTranslationsObject
from ...utils import LOGGER, retryable_session
from ..abstract import AbstractProvider


@singleton
class CardMarketProviderSetNameTranslations(AbstractProvider):
    CARD_MARKET_URL = "https://www.cardmarket.com/{}/Magic/Products/Booster-Boxes?mode=gallery&idCategory=7&idExpansion=0&sortBy=date_asc&site={}"
    SUPPORTED_LOCALES = {
        "en": "English",
        "fr": "French",
        "de": "German",
        "it": "Italian",
        "es": "Spanish",
    }
    SESSION: Union[requests.Session, requests_cache.CachedSession]
    translation_table: Dict[str, Dict[str, str]]

    def __init__(self) -> None:
        super().__init__({})
        self.SESSION = retryable_session()
        self.translation_table = {}

    def _build_http_header(self) -> Dict[str, str]:
        raise NotImplementedError()

    def download(self, url: str, params: Dict[str, Union[str, int]] = None) -> Any:
        raise NotImplementedError()

    def get_set_translation_object(self, set_code: str) -> MtgjsonTranslationsObject:
        if not self.translation_table:
            self.translation_table = self.__generate_set_name_translations()

        if set_code in self.translation_table.keys():
            return MtgjsonTranslationsObject(self.translation_table[set_code])

        return MtgjsonTranslationsObject()

    def __generate_set_name_translations(self) -> Dict[str, Dict[str, str]]:
        translation_table: Dict[str, Dict[str, str]] = defaultdict(
            lambda: defaultdict(str)
        )

        for page_number in range(1, 20):
            for locale_short, locale_long in self.SUPPORTED_LOCALES.items():
                for position, set_name_localized in self.__download_localized_data(
                    locale_short, page_number
                ):
                    translation_table[position][locale_long] = set_name_localized

        return self.__convert_intermediate_translation_table(dict(translation_table))

    def __download_localized_data(
        self, locale_short: str, page_number: int
    ) -> List[Tuple[str, str]]:
        return_results = []

        response = self.SESSION.get(
            self.CARD_MARKET_URL.format(locale_short, page_number)
        )
        self.log_download(response)

        soup = bs4.BeautifulSoup(response.text, "html.parser")
        booster_box_entities = soup.find_all("h2", class_="card-title")
        for booster_box_entity in booster_box_entities:
            header_page_element = booster_box_entity.find_next("span")

            position_finder = re.finditer(
                r"background-position: ([^;]+);",
                str(header_page_element.find_next("span")),
            )

            if not position_finder:
                LOGGER.error(
                    "Unable to parse background-position from {} in request {}",
                    header_page_element,
                    response.url,
                )

            return_results.append(
                (str(next(position_finder)), header_page_element["title"])
            )

        return return_results

    def __convert_intermediate_translation_table(
        self, intermediate_table: Dict[str, Dict[str, str]]
    ) -> Dict[str, Dict[str, str]]:
        final_translation_table: Dict[str, Dict[str, str]] = defaultdict(dict)

        for key, value in intermediate_table.items():
            set_code = self.__convert_set_name_to_set_code(value["English"])
            if set_code:
                final_translation_table[set_code] = value

        return final_translation_table

    def __convert_set_name_to_set_code(self, long_name: str) -> Optional[str]:
        long_name_sanitized = long_name
        for char in ":'’.& ":
            long_name_sanitized = long_name_sanitized.replace(char, "")

        results = self.SESSION.get(
            "https://api.scryfall.com/sets/{}".format(long_name_sanitized)
        ).json()
        if results["object"] == "error":
            return None

        return str(results["code"].upper())
