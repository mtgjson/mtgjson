"""
MTGJSON TcgplayerSkus Object
"""

import logging
import pathlib
from typing import Dict, List, Union

from ..providers import TCGPlayerProvider
from ..providers.tcgplayer import convert_sku_data_enum, get_tcgplayer_sku_data
from ..utils import generate_card_mapping

LOGGER = logging.getLogger(__name__)


class MtgjsonTcgplayerSkusObject:
    """
    MTGJSON TcgplayerSkus Object
    """

    enhanced_tcgplayer_skus: Dict[str, List[Dict[str, Union[int, str]]]]

    def __init__(self, all_printings_path: pathlib.Path) -> None:
        self.enhanced_tcgplayer_skus = {}

        tcg_to_mtgjson_map = generate_card_mapping(
            all_printings_path, ("identifiers", "tcgplayerProductId"), ("uuid",)
        )
        for group in TCGPlayerProvider().get_tcgplayer_magic_set_ids():
            tcgplayer_sku_data = get_tcgplayer_sku_data(group)
            for product in tcgplayer_sku_data:
                product_id = str(product["productId"])
                key = tcg_to_mtgjson_map.get(product_id)
                if not key:
                    LOGGER.debug(f"Unable to translate TCGPlayer product {product_id}")
                    continue

                self.enhanced_tcgplayer_skus[key] = [
                    convert_sku_data_enum(sku) for sku in product["skus"]
                ]

    def to_json(self) -> Dict[str, List[Dict[str, Union[int, str]]]]:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        return self.enhanced_tcgplayer_skus
