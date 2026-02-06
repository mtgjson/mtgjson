"""Scryfall provider for detecting card orientation in Art Series sets."""

import bs4
from singleton_decorator import singleton

from ...providers.abstract import AbstractProvider
from ...providers.scryfall import sf_utils


@singleton
class ScryfallProviderOrientationDetector(AbstractProvider):
    """Provider to detect card orientation (landscape/portrait) from Scryfall set pages."""

    MAIN_PAGE_URL: str = "https://scryfall.com/sets/{}"

    def __init__(self) -> None:
        super().__init__(self._build_http_header())

    def _build_http_header(self) -> dict[str, str]:
        return sf_utils.build_http_header()

    def get_uuid_to_orientation_map(self, set_code: str) -> dict[str, str]:
        """Build a mapping of Scryfall card IDs to their orientation for a set."""
        response = self.download(self.MAIN_PAGE_URL.format(set_code))

        soup = bs4.BeautifulSoup(response, "html.parser")
        orientation_headers = soup.find_all("span", class_="card-grid-header-content")
        scryfall_card_entries_by_orientation = soup.find_all("div", class_="card-grid-inner")

        return_map = {}
        for orientation_header, scryfall_card_entries in zip(
            orientation_headers, scryfall_card_entries_by_orientation, strict=False
        ):
            orientation = self._parse_orientation(orientation_header)
            card_uuids = self._parse_card_entries(scryfall_card_entries)
            for card_uuid in card_uuids:
                return_map[card_uuid] = orientation

        return return_map

    def download(self, url: str, params: dict[str, str | int] | None = None) -> str:
        response = self.session.get(url)
        self.log_download(response)
        return response.text

    @staticmethod
    def _parse_orientation(orientation_header: bs4.Tag) -> str:
        """
        Parse card orientation from bs4 Tag
        """
        a_tag = orientation_header.find("a")
        if not isinstance(a_tag, bs4.Tag):
            return str(None)
        return str(a_tag.get("id"))

    @staticmethod
    def _parse_card_entries(scryfall_card_ids: bs4.Tag) -> list[str]:
        """
        Parse out all card UUIDs from bs4 Tag
        """
        card_items = scryfall_card_ids.find_all("div", class_="card-grid-item")
        return [str(card_item.get("data-card-id")) for card_item in card_items]
