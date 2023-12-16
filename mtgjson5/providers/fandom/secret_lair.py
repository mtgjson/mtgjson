import logging
from typing import Dict, List, Optional, Union

import bs4
from singleton_decorator import singleton

from ...providers.abstract import AbstractProvider


@singleton
class FandomProviderSecretLair(AbstractProvider):
    PAGE_URL = "https://mtg.fandom.com/wiki/Secret_Lair/Drop_Series"
    logger: logging.Logger

    def __init__(self, headers: Optional[Dict[str, str]] = None):
        super().__init__(headers or {})
        self.logger = logging.getLogger(__name__)

    def _build_http_header(self) -> Dict[str, str]:
        return {}

    def download(
        self, url: str = "", params: Optional[Dict[str, Union[str, int]]] = None
    ) -> Dict[str, str]:
        """
        Download Fandom Secret Lair page and parse it out
        for user consumption
        :returns Mapping of Card ID to Secret Lair Drop Name
        """
        response = self.session.get(url if url else self.PAGE_URL)
        self.log_download(response)

        return self.__parse_secret_lair_table(response.text)

    def __parse_secret_lair_table(self, page_text: str) -> Dict[str, str]:
        results = {}

        soup = bs4.BeautifulSoup(page_text, "html.parser")
        table = soup.find("table", {"class": "wikitable sortable"})
        table_rows = table.find_all("tr")
        for table_row in table_rows[1:]:
            table_cols = table_row.find_all("td")

            secret_lair_name = table_cols[1].text.strip()
            card_numbers = self.__convert_range_to_page_style(table_cols[2].text)

            if not secret_lair_name or not card_numbers:
                continue

            results.update(
                {str(card_num): secret_lair_name for card_num in card_numbers}
            )

        return results

    @staticmethod
    def __convert_range_to_page_style(range_string: str) -> List[int]:
        range_string = "".join(filter("0123456789-,".__contains__, range_string))
        if not range_string:
            return []

        return sum(
            (
                (
                    list(range(*[int(j) + k for k, j in enumerate(i.split("-"))]))
                    if "-" in i
                    else [int(i)]
                )
                for i in range_string.split(",")
            ),
            [],
        )
