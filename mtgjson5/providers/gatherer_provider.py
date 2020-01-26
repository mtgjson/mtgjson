"""
Wizards Gatherer 3rd party provider
"""
import copy
import re
from typing import Dict, List, NamedTuple, Optional, Union

import bs4
import requests
from singleton_decorator import singleton

from ..consts import SYMBOL_MAP
from ..providers.abstract_provider import AbstractProvider


class GathererCard(NamedTuple):
    """
    Response payload for fetching a card from Gatherer
    """

    card_name: str
    original_types: str
    original_text: Optional[str]
    flavor_text: Optional[str]


@singleton
class GathererProvider(AbstractProvider):
    """
    Gatherer Container
    """

    GATHERER_CARD = "http://gatherer.wizards.com/Pages/Card/Details.aspx"
    SETS_TO_REMOVE_PARENTHESES = {"10E"}

    def __init__(self) -> None:

        super().__init__(self._build_http_header())

    def _build_http_header(self) -> Dict[str, str]:
        return {}

    def download(
        self, url: str, params: Dict[str, Union[str, int]] = None
    ) -> requests.Response:
        session = self.session_pool.popleft()
        response = session.get(url, params=params)
        self.session_pool.append(session)
        self.log_download(response)
        return response

    def get_cards(self, multiverse_id: int, set_code: str = "") -> List[GathererCard]:
        """
        Get card(s) matching a given multiverseId
        """
        response = self.download(
            self.GATHERER_CARD, {"multiverseid": str(multiverse_id), "printed": "true"}
        )

        return self.parse_cards(
            response.text, set_code in self.SETS_TO_REMOVE_PARENTHESES
        )

    def parse_cards(
        self, gatherer_data: str, strip_parentheses: bool = False
    ) -> List[GathererCard]:
        """
        Parse all cards from a given gatherer page
        """
        soup = bs4.BeautifulSoup(gatherer_data, "html.parser")
        columns = soup.find_all("td", class_="rightCol")
        return [self._parse_column(c, strip_parentheses) for c in columns]

    def _parse_column(
        self, gatherer_column: bs4.element.Tag, strip_parentheses: bool
    ) -> GathererCard:
        """
        Parse a single gatherer page 'rightCol' entry
        """
        label_to_values = {
            row.find("div", class_="label")
            .getText(strip=True)
            .rstrip(":"): row.find("div", class_="value")
            for row in gatherer_column.find_all("div", class_="row")
        }

        card_name = label_to_values["Card Name"].getText(strip=True)
        card_types = label_to_values["Types"].getText(strip=True)

        flavor_lines = []
        if "Flavor Text" in label_to_values:
            for flavor_box in label_to_values["Flavor Text"].find_all(
                "div", class_="flavortextbox"
            ):
                flavor_lines.append(flavor_box.getText(strip=True))

        text_lines = []
        if "Card Text" in label_to_values:
            for textbox in label_to_values["Card Text"].find_all(
                "div", class_="cardtextbox"
            ):
                text_lines.append(self._replace_symbols(textbox).getText().strip())

        original_text: Optional[str] = "\n".join(text_lines).strip() or None
        if strip_parentheses and original_text:
            original_text = self.strip_parentheses_from_text(original_text)

        return GathererCard(
            card_name=card_name,
            original_types=card_types,
            original_text=original_text,
            flavor_text="\n".join(flavor_lines).strip() or None,
        )

    @staticmethod
    def _replace_symbols(tag: bs4.BeautifulSoup) -> bs4.BeautifulSoup:
        """
        Replace all image tags with their mapped symbol
        """
        tag_copy = copy.copy(tag)
        images = tag_copy.find_all("img")
        for image in images:
            alt = image["alt"]
            symbol = SYMBOL_MAP.get(alt, alt)
            image.replace_with("{" + symbol + "}")
        return tag_copy

    @staticmethod
    def strip_parentheses_from_text(text: str) -> str:
        """
        Remove all text within parentheses from a card, along with
        extra spaces.
        :param text: Text to modify
        :return: Stripped text
        """
        return re.sub(r" \([^)]*\)", "", text).replace("  ", " ").strip()
