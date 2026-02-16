"""V2 provider for detecting card orientation in Art Series sets from Scryfall."""

import logging

import bs4
import requests
import requests.adapters
import urllib3

from mtgjson5.mtgjson_config import MtgjsonConfig

LOGGER = logging.getLogger(__name__)

SET_PAGE_URL = "https://scryfall.com/sets/{}"


def _make_session() -> requests.Session:
    session = requests.Session()
    retry = urllib3.util.retry.Retry(
        total=8, backoff_factor=0.3, status_forcelist=(500, 502, 504)
    )
    adapter = requests.adapters.HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    headers: dict[str, str] = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; +https://www.mtgjson.com) "
        "Gecko/20100101 Firefox/120.0"
    }

    try:
        if MtgjsonConfig().has_option("Scryfall", "client_secret"):
            headers["Authorization"] = (
                f"Bearer {MtgjsonConfig().get('Scryfall', 'client_secret')}"
            )
    except Exception:
        pass

    session.headers.update(headers)
    return session


class OrientationDetector:
    """Detects card orientation (landscape/portrait) from Scryfall set pages."""

    def __init__(self) -> None:
        self._session = _make_session()

    def get_uuid_to_orientation_map(self, set_code: str) -> dict[str, str]:
        """Build a mapping of Scryfall card IDs to their orientation for a set."""
        try:
            response = self._session.get(
                SET_PAGE_URL.format(set_code), timeout=15
            )
            response.raise_for_status()
        except requests.RequestException as e:
            LOGGER.warning(f"Failed to fetch orientation for {set_code}: {e}")
            return {}

        soup = bs4.BeautifulSoup(response.text, "html.parser")
        orientation_headers = soup.find_all(
            "span", class_="card-grid-header-content"
        )
        card_grids = soup.find_all("div", class_="card-grid-inner")

        result: dict[str, str] = {}
        for header, grid in zip(orientation_headers, card_grids, strict=False):
            orientation = self._parse_orientation(header)
            card_uuids = self._parse_card_entries(grid)
            for uuid in card_uuids:
                result[uuid] = orientation

        return result

    @staticmethod
    def _parse_orientation(header: bs4.Tag) -> str:
        """Parse card orientation from header tag."""
        a_tag = header.find("a")
        if not isinstance(a_tag, bs4.Tag):
            return str(None)
        return str(a_tag.get("id"))

    @staticmethod
    def _parse_card_entries(grid: bs4.Tag) -> list[str]:
        """Parse all card UUIDs from a card grid tag."""
        card_items = grid.find_all("div", class_="card-grid-item")
        return [str(item.get("data-card-id")) for item in card_items]
