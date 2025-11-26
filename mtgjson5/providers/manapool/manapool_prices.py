import copy
import logging
import pathlib
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Dict, Optional, Union

from singleton_decorator import singleton

from mtgjson5.providers.abstract import AbstractProvider
from mtgjson5.utils import generate_entity_mapping

if TYPE_CHECKING:
    from ...models import MtgjsonPricesObject

LOGGER = logging.getLogger(__name__)


@singleton
class ManapoolPricesProvider(AbstractProvider):
    singles_api_uri: str = "https://manapool.com/api/v1/prices/singles"

    def __init__(self) -> None:
        super().__init__(self._build_http_header())

    def _build_http_header(self) -> Dict[str, str]:
        return {}

    def download(
        self, url: str, params: Optional[Dict[str, Union[str, int]]] = None
    ) -> Any:
        response = self.session.get(url)
        self.log_download(response)

        return response.json()

    def _inner_translate_today_price_dict_pt1(self) -> Dict[str, Dict[str, float]]:
        """
        Convert the single-day price data to a dictionary of Scryfall UUIDs
        """
        mapping: Dict[str, Dict[str, float]] = defaultdict(dict)

        api_response = self.download(self.singles_api_uri).get("data")
        for card in api_response:
            for card_key, finish in zip(
                ("price_cents", "price_cents_foil", "price_cents_etched"),
                ("normal", "foil", "etched"),
            ):
                if card[card_key]:
                    mapping[card["scryfall_id"]][finish] = card[card_key] / 100.0

        return mapping

    def generate_today_price_dict(
        self, all_printings_path: pathlib.Path
    ) -> Dict[str, MtgjsonPricesObject]:
        """
        Generate a single-day price structure for MTGO from Manapool
        :return MTGJSON prices single day structure
        """
        from ...models import MtgjsonPricesObject

        LOGGER.info("Building Manapool retail data")

        scryfall_id_to_mtgjson_id = generate_entity_mapping(
            all_printings_path, ("identifiers", "scryfallId"), ("uuid",)
        )

        default_prices_obj = MtgjsonPricesObject(
            source="paper", provider="manapool", date=self.today_date, currency="USD"
        )

        final_data = {}

        for (
            scryfall_uuid,
            card_finish_to_price,
        ) in self._inner_translate_today_price_dict_pt1().items():
            if scryfall_uuid not in scryfall_id_to_mtgjson_id:
                continue

            for mtgjson_uuid in scryfall_id_to_mtgjson_id[scryfall_uuid]:
                final_data[mtgjson_uuid] = copy.copy(default_prices_obj)
                if "normal" in card_finish_to_price:
                    final_data[mtgjson_uuid].sell_normal = card_finish_to_price.get(
                        "normal"
                    )
                if "foil" in card_finish_to_price:
                    final_data[mtgjson_uuid].sell_foil = card_finish_to_price.get(
                        "foil"
                    )
                if "etched" in card_finish_to_price:
                    final_data[mtgjson_uuid].sell_etched = card_finish_to_price.get(
                        "etched"
                    )

        return final_data
