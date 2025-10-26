from typing import Dict, List, Optional, Union

import bs4
from singleton_decorator import singleton

from ...providers.abstract import AbstractProvider
from ...providers.scryfall import sf_utils


@singleton
class ScryfallProviderOrientationDetector(AbstractProvider):
    MAIN_PAGE_URL: str = "https://scryfall.com/sets/{}"

    def __init__(self) -> None:
        super().__init__(self._build_http_header())

    def _build_http_header(self) -> Dict[str, str]:
        return sf_utils.build_http_header()

    def get_uuid_to_orientation_map(self, set_code: str) -> Dict[str, str]:
        response = self.download(self.MAIN_PAGE_URL.format(set_code))

        soup = bs4.BeautifulSoup(response, "html.parser")
        orientation_headers = soup.find_all("span", class_="card-grid-header-content")
        scryfall_card_entries_by_orientation = soup.find_all(
            "div", class_="card-grid-inner"
        )

        return_map = dict()
        for orientation_header, scryfall_card_entries in zip(
            orientation_headers, scryfall_card_entries_by_orientation
        ):
            orientation = self._parse_orientation(orientation_header)
            card_uuids = self._parse_card_entries(scryfall_card_entries)
            for card_uuid in card_uuids:
                return_map[card_uuid] = orientation

        return return_map

    def download(
        self, url: str, params: Optional[Dict[str, Union[str, int]]] = None
    ) -> str:
        response = self.session.get(url)
        self.log_download(response)
        return response.text

    @staticmethod
    def _parse_orientation(orientation_header: bs4.Tag) -> str:
        """
        Parse card orientation from bs4 Tag
        """
        a_tags = orientation_header.find("a") or {}
        return str(a_tags.get("id"))

    @staticmethod
    def _parse_card_entries(scryfall_card_ids: bs4.Tag) -> List[str]:
        """
        Parse out all card UUIDs from bs4 Tag
        """
        card_items = scryfall_card_ids.find_all("div", class_="card-grid-item") or []
        return [card_item.get("data-card-id") for card_item in card_items]
