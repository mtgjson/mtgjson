"""
MTGJSON AllTcgSkus Object
"""

import logging
import pathlib
from typing import Dict

from ..providers import TCGPlayerProvider
from ..providers.tcgplayer import get_tcgplayer_sku_data
from ..utils import generate_card_mapping

LOGGER = logging.getLogger(__name__)


class MtgjsonAllTcgSkusObject:
    """
    MTGJSON AllTcgSkus Object
    """

    all_tcg_skus_dict: Dict[str, Dict[str, int]]

    def __init__(self, all_printings_path: pathlib.Path) -> None:

        self.all_tcg_skus_dict = {}

        ids_and_names = TCGPlayerProvider().get_tcgplayer_magic_set_ids()

        tcg_to_mtgjson_map = generate_card_mapping(
            all_printings_path, ("identifiers", "tcgplayerProductId"), ("uuid",)
        )
        for group in ids_and_names:
            tcgplayer_sku_data = get_tcgplayer_sku_data(group)
            for product in tcgplayer_sku_data:
                product_id = str(product["productId"])
                key = tcg_to_mtgjson_map.get(product_id)
                if not key:
                    continue

                self.all_tcg_skus_dict[key] = product["skus"]

    def to_json(self) -> Dict[str, Dict[str, int]]:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        return self.all_tcg_skus_dict
