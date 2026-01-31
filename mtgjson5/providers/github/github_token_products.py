import logging
import pathlib
from typing import Any, Dict, List, Optional, Union

from singleton_decorator import singleton

from ...mtgjson_config import MtgjsonConfig
from ...providers.abstract import AbstractProvider

LOGGER = logging.getLogger(__name__)


@singleton
class GitHubTokenProductsProvider(AbstractProvider):

    token_components_url: str = (
        "https://github.com/mtgjson/mtg-sealed-content/raw/main/outputs/tokens/{}.json?raw=True"
    )

    def __init__(self) -> None:
        super().__init__(self._build_http_header())

    def _build_http_header(self) -> Dict[str, str]:
        __github_token = MtgjsonConfig().get("GitHub", "api_token")
        return {"Authorization": f"Bearer {__github_token}"}

    def download(
        self, url: str, params: Optional[Dict[str, Union[str, int]]] = None
    ) -> Any:
        response = self.session.get(url)
        self.log_download(response)
        if response.ok:
            return response.json()

        LOGGER.error(f"Error downloading GitHub Cards: {response} --- {response.text}")
        return []

    def get_token_components(self, set_code: str) -> Dict[str, Any]:
        with pathlib.Path(
            f"/Users/zach/Desktop/Development/mtg-sealed-content/outputs/token_products_mappings/{set_code}.json"
        ).open() as fp:
            import json

            return dict(json.load(fp))
        return self.download(self.token_components_url.format(set_code))
