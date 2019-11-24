"""
Scryfall 3rd party provider
"""
from typing import Optional, Dict


from mtgjson5.providers.abstract_provider import AbstractProvider


class ScryfallProvider(AbstractProvider):
    """
    Scryfall container
    """

    def __init__(self, use_cache: bool = True):
        headers: Optional[Dict[str, str]] = None
        config = self.get_configs()
        if config.get("Scryfall", "client_secret"):
            headers = {
                "Authorization": f"Bearer {config.get('Scryfall', 'client_secret')}"
            }

        super().__init__(headers, use_cache)

    def download(self, url: str) -> str:
        """
        Download content from Scryfall
        :param url: URL to download from
        """
        session = self.session_pool.get()
        response = session.get(url)
        self.session_pool.put(session)

        return response.json()
