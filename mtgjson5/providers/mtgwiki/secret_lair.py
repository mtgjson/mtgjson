import logging
from typing import Dict, List, Optional, Union

import bs4
from singleton_decorator import singleton

from ...providers.abstract import AbstractProvider


@singleton
class MtgWikiProviderSecretLair(AbstractProvider):
    PAGE_URL = "https://mtg.wiki/page/Secret_Lair/Drop_Series"
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
        Download MTG.Wiki Secret Lair page and parse it out
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
        for index, table_row in enumerate(table_rows[1:]):
            table_cols = table_row.find_all("td")

            extra_card_numbers = ""
            if "rowspan" in str(table_cols[0]):
                # We have multiple segments split up
                next_tr_cols = table_rows[index + 2].find_all("td")
                extra_card_numbers = f",{next_tr_cols[0].text}"
            elif len(table_cols) < 3:
                continue

            secret_lair_name = table_cols[1].text.strip()
            card_numbers = self.__convert_range_to_page_style(
                table_cols[2].text + extra_card_numbers
            )

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
