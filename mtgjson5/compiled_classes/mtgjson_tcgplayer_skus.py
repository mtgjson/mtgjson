"""
MTGJSON TcgplayerSkus Object
"""

import logging
import pathlib
from collections import defaultdict

from ..classes.json_object import JsonObject
from ..providers.tcgplayer import TCGPlayerProvider
from ..utils import generate_entity_mapping


LOGGER = logging.getLogger(__name__)


class MtgjsonTcgplayerSkusObject(JsonObject):
    """
    MTGJSON TcgplayerSkus Object
    """

    enhanced_tcgplayer_skus: defaultdict[str, list[dict[str, int | str]]]

    def __init__(self, all_printings_path: pathlib.Path) -> None:
        self.enhanced_tcgplayer_skus = defaultdict(list)

        tcg_normal_to_mtgjson_map = generate_entity_mapping(
            all_printings_path,
            ("identifiers", "tcgplayerProductId"),
            ("uuid",),
            include_sealed_product=True,
        )
        tcg_etched_to_mtgjson_map = generate_entity_mapping(
            all_printings_path,
            ("identifiers", "tcgplayerEtchedProductId"),
            ("uuid",),
            include_sealed_product=True,
        )

        for group in TCGPlayerProvider().get_tcgplayer_magic_set_ids():
            tcgplayer_sku_data = TCGPlayerProvider().get_tcgplayer_sku_data(group)
            for product in tcgplayer_sku_data:
                product_id = str(product["productId"])
                normal_keys: set[str] = tcg_normal_to_mtgjson_map.get(product_id, set())
                etched_keys: set[str] = tcg_etched_to_mtgjson_map.get(product_id, set())
                for normal_key in normal_keys:
                    self.enhanced_tcgplayer_skus[normal_key].extend(
                        TCGPlayerProvider().convert_sku_data_enum(product)
                    )
                for etched_key in etched_keys:
                    self.enhanced_tcgplayer_skus[etched_key].extend(
                        TCGPlayerProvider().convert_sku_data_enum(product)
                    )

    def to_json(self) -> dict[str, list[dict[str, int | str]]]:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        return self.enhanced_tcgplayer_skus
