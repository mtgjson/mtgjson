"""
Wizards Site 3rd party provider
"""

import logging
import re
import time

import requests
from singleton_decorator import singleton

from ..providers.abstract import AbstractProvider


@singleton
class WizardsProvider(AbstractProvider):
    """
    Wizards Site Container
    """

    TRANSLATION_URL: str = "https://magic.wizards.com/{}/products/card-set-archive"
    magic_rules_url: str = "https://magic.wizards.com/en/rules"
    magic_rules: str = ""
    __one_week_ago: int = int(time.time() - 7 * 86400)

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        super().__init__(self._build_http_header())

    def _build_http_header(self) -> dict[str, str]:
        """
        Construct the Authorization header -- unused
        :return: Authorization header
        """
        return {}

    def download(self, url: str, params: dict[str, str | int] | None = None) -> requests.Response:
        """
        Download from Wizard's website
        :param url: URL to get
        :param params: Not used
        :return: Response
        """
        response = self.session.get(url)
        self.log_download(response)
        return response

    # Handle building up components from rules text
    def get_magic_rules(self) -> str:
        """
        Download the comp rules from Wizards site
        :return Comprehensive Magic Rules
        """
        if self.magic_rules:
            return self.magic_rules

        response = self.download(self.magic_rules_url).content.decode()

        # Get the comp rules from the website (as it changes often)
        # Also split up the regex find so we only have the URL
        self.magic_rules_url = str(re.findall(r"href=\".*\.txt\"", response)[0][6:-1])
        response = self.download(self.magic_rules_url).content.decode("utf-8", "ignore").replace("â€™", "'")

        self.magic_rules = "\n".join(response.splitlines())
        return self.magic_rules
