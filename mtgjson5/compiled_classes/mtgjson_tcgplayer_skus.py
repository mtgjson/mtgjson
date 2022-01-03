"""
MTGJSON TcgplayerSkus Object
"""

import logging
import pathlib
from collections import defaultdict
from typing import DefaultDict, Dict, List, Union

from ..providers.tcgplayer import (
    TCGPlayerProvider,
    convert_sku_data_enum,
    get_tcgplayer_sku_data,
)
from ..utils import generate_card_mapping

LOGGER = logging.getLogger(__name__)


class MtgjsonTcgplayerSkusObject:
    """
    MTGJSON TcgplayerSkus Object
    """

    enhanced_tcgplayer_skus: DefaultDict[str, List[Dict[str, Union[int, str]]]]

    def __init__(self, all_printings_path: pathlib.Path) -> None:
        self.enhanced_tcgplayer_skus = defaultdict(list)

        tcg_normal_to_mtgjson_map = generate_card_mapping(
            all_printings_path, ("identifiers", "tcgplayerProductId"), ("uuid",)
        )
        tcg_etched_to_mtgjson_map = generate_card_mapping(
            all_printings_path, ("identifiers", "tcgplayerEtchedProductId"), ("uuid",)
        )

        for group in TCGPlayerProvider().get_tcgplayer_magic_set_ids():
            tcgplayer_sku_data = get_tcgplayer_sku_data(group)
            for product in tcgplayer_sku_data:
                product_id = str(product["productId"])
                normal_key = tcg_normal_to_mtgjson_map.get(product_id)
                etched_key = tcg_etched_to_mtgjson_map.get(product_id)
                if normal_key:
                    self.enhanced_tcgplayer_skus[normal_key].extend(
                        convert_sku_data_enum(product)
                    )
                if etched_key:
                    self.enhanced_tcgplayer_skus[etched_key].extend(
                        convert_sku_data_enum(product)
                    )

    def to_json(self) -> Dict[str, List[Dict[str, Union[int, str]]]]:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        return self.enhanced_tcgplayer_skus
