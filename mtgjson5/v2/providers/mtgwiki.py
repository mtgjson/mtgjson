"""V2 provider for scraping Secret Lair subset data from mtg.wiki."""

import functools
import logging
import operator

import bs4
import requests
import requests.adapters
import urllib3

LOGGER = logging.getLogger(__name__)

SECRET_LAIR_PAGE_URL = "https://mtg.wiki/page/Secret_Lair/Drop_Series"


def _make_session() -> requests.Session:
    session = requests.Session()
    retry = urllib3.util.retry.Retry(
        total=8, backoff_factor=0.3, status_forcelist=(500, 502, 504)
    )
    adapter = requests.adapters.HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; +https://www.mtgjson.com) "
            "Gecko/20100101 Firefox/120.0"
        }
    )
    return session


class SecretLairProvider:
    """Scrapes Secret Lair card number -> drop name mappings from mtg.wiki."""

    def __init__(self) -> None:
        self._session = _make_session()

    def download(self, url: str = "") -> dict[str, str]:
        """Download and parse the Secret Lair page.

        Returns:
            Mapping of card number string -> Secret Lair drop name.
        """
        try:
            response = self._session.get(
                url or SECRET_LAIR_PAGE_URL, timeout=30
            )
            response.raise_for_status()
        except requests.RequestException as e:
            LOGGER.error(f"Failed to fetch Secret Lair data: {e}")
            return {}

        return self._parse_secret_lair_table(response.text)

    @staticmethod
    def _parse_secret_lair_table(page_text: str) -> dict[str, str]:
        """Parse the Secret Lair wikitable into card number -> name mapping."""
        results: dict[str, str] = {}

        soup = bs4.BeautifulSoup(page_text, "html.parser")
        table = soup.find("table", {"class": "wikitable sortable"})
        if not table:
            return results

        table_rows = table.find_all("tr")
        for index, table_row in enumerate(table_rows[1:]):
            table_cols = table_row.find_all("td")

            extra_card_numbers = ""
            if "rowspan" in str(table_cols[0]):
                next_tr_cols = table_rows[index + 2].find_all("td")
                extra_card_numbers = f",{next_tr_cols[0].text}"
            elif len(table_cols) < 3:
                continue

            if not table_cols[0].text.strip().isdigit():
                continue

            secret_lair_name = table_cols[1].text.strip()
            card_numbers = _convert_range_to_page_style(
                table_cols[2].text + extra_card_numbers
            )

            if not secret_lair_name or not card_numbers:
                continue

            results.update(
                {str(card_num): secret_lair_name for card_num in card_numbers}
            )

        return results


def _convert_range_to_page_style(range_string: str) -> list[int]:
    """Convert a range string like '1-5,7,10-12' to a flat list of ints."""
    range_string = "".join(
        filter("0123456789-,".__contains__, range_string)
    )
    if not range_string:
        return []

    return functools.reduce(
        operator.iadd,
        (
            (
                list(
                    range(
                        *[
                            int(j) + k
                            for k, j in enumerate(i.split("-"))
                            if len(j) > 0
                        ]
                    )
                )
                if "-" in i
                else [int(i)]
            )
            for i in range_string.split(",")
        ),
        [],
    )
