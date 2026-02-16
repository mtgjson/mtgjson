"""V2 provider for fetching Standard-legal set codes from whatsinstandard.com."""

import datetime
import logging

import dateutil.parser
import requests
import requests.adapters
import urllib3

LOGGER = logging.getLogger(__name__)

API_ENDPOINT = "https://whatsinstandard.com/api/v6/standard.json"


def _make_session() -> requests.Session:
    """Create a requests session with retry logic."""
    session = requests.Session()
    retry = urllib3.util.retry.Retry(total=8, backoff_factor=0.3, status_forcelist=(500, 502, 504))
    adapter = requests.adapters.HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64; +https://www.mtgjson.com) Gecko/20100101 Firefox/120.0"}
    )
    return session


class WhatsInStandardProvider:
    """Fetches current Standard-legal set codes from whatsinstandard.com."""

    set_codes: set[str]

    def __init__(self) -> None:
        self._session = _make_session()
        self.set_codes = self._fetch_standard_legal_set_codes()

    def _fetch_standard_legal_set_codes(self) -> set[str]:
        """Fetch set codes currently legal in Standard."""
        try:
            response = self._session.get(API_ENDPOINT, timeout=15)
            response.raise_for_status()
        except requests.RequestException as e:
            LOGGER.error(f"Failed to fetch Standard data: {e}")
            return set()

        api_response = response.json()
        now = datetime.datetime.now()

        return {
            str(set_obj.get("code")).upper()
            for set_obj in api_response.get("sets", [])
            if (
                dateutil.parser.parse(set_obj["enterDate"]["exact"] or "9999")
                <= now
                <= dateutil.parser.parse(set_obj["exitDate"]["exact"] or "9999")
            )
        }
