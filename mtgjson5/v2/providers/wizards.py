"""V2 provider for downloading Magic Comprehensive Rules from Wizards website."""

import logging
import re

import requests
import requests.adapters
import urllib3

LOGGER = logging.getLogger(__name__)

MAGIC_RULES_URL = "https://magic.wizards.com/en/rules"


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


class WizardsProvider:
    """Downloads and caches Magic Comprehensive Rules from Wizards website."""

    def __init__(self) -> None:
        self._session = _make_session()
        self._magic_rules: str = ""

    def get_magic_rules(self) -> str:
        """Download the Comprehensive Rules text from Wizards site.

        Returns:
            The full Comprehensive Rules as a single string.
        """
        if self._magic_rules:
            return self._magic_rules

        try:
            # First, fetch the rules landing page to find the .txt URL
            response = self._session.get(MAGIC_RULES_URL, timeout=30)
            response.raise_for_status()
            page_content = response.content.decode()

            # Extract the rules .txt URL from the page
            txt_urls = re.findall(r'href=".*?\.txt"', page_content)
            if not txt_urls:
                LOGGER.error("Could not find rules .txt link on Wizards page")
                return ""

            rules_url = txt_urls[0][6:-1]  # Strip href=" and trailing "

            # Download the actual rules text
            response = self._session.get(rules_url, timeout=60)
            response.raise_for_status()
            rules_text = response.content.decode("utf-8", "ignore").replace("\u2019", "'")

            self._magic_rules = "\n".join(rules_text.splitlines())
        except requests.RequestException as e:
            LOGGER.error(f"Failed to download Magic rules: {e}")
            return ""

        return self._magic_rules
