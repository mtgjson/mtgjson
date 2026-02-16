"""V2 provider for fetching Gatherer original text/type data from GitHub."""

import logging

import requests
import requests.adapters
import urllib3

from mtgjson5.mtgjson_config import MtgjsonConfig

LOGGER = logging.getLogger(__name__)

GATHERER_MAPPING_URL = (
    "https://github.com/mtgjson/mtg-sealed-content/raw/main/"
    "outputs/gatherer_mapping.json?raw=True"
)


def _make_session(headers: dict[str, str] | None = None) -> requests.Session:
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
    if headers:
        session.headers.update(headers)
    return session


class GathererProvider:
    """Fetches multiverse_id -> original text/type mappings from GitHub."""

    _multiverse_id_to_data: dict[str, list[dict[str, str]]]

    def __init__(self) -> None:
        headers = self._build_http_header()
        self._session = _make_session(headers)
        self._multiverse_id_to_data = self._download_mapping()

    @staticmethod
    def _build_http_header() -> dict[str, str]:
        try:
            token = MtgjsonConfig().get("GitHub", "api_token")
            return {"Authorization": f"Bearer {token}"}
        except Exception:
            LOGGER.warning("GitHub token not configured, Gatherer fetch may fail")
            return {}

    def _download_mapping(self) -> dict[str, list[dict[str, str]]]:
        """Download the Gatherer ID mapping JSON from GitHub."""
        try:
            response = self._session.get(GATHERER_MAPPING_URL, timeout=30)
            if response.ok:
                return response.json()
            LOGGER.error(
                f"Error downloading Gatherer mapping: {response.status_code}"
            )
        except requests.RequestException as e:
            LOGGER.error(f"Failed to download Gatherer mapping: {e}")
        return {}

    def get_cards(self, multiverse_id: str) -> list[dict[str, str]]:
        """Get card(s) matching a given multiverseId."""
        return self._multiverse_id_to_data.get(multiverse_id, [])
