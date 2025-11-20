from typing import DefaultDict, Dict, List, Set, Union
from pydantic import Field
import logging
import pathlib
from collections import defaultdict

from ... import providers, utils
from ..mtgjson_base import MTGJsonCompiledModel

TCGPlayerProvider = providers.tcgplayer.TCGPlayerProvider
generate_entity_mapping = utils.generate_entity_mapping

LOGGER = logging.getLogger(__name__)


class MtgjsonTcgplayerSkusObject(MTGJsonCompiledModel):
    """
    MTGJSON TcgplayerSkus Object
    """

    enhanced_tcgplayer_skus: DefaultDict[str, List[Dict[str, Union[int, str]]]] = Field(
        default_factory=lambda: defaultdict(list)
    )

    def __init__(self, all_printings_path: pathlib.Path, **kwargs) -> None:
        """
        Initializer to build up the object
        :param all_printings_path: Path to AllPrintings file
        """
        super().__init__(**kwargs)
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
                normal_keys: Set[str] = tcg_normal_to_mtgjson_map.get(product_id, set())
                etched_keys: Set[str] = tcg_etched_to_mtgjson_map.get(product_id, set())
                for normal_key in normal_keys:
                    self.enhanced_tcgplayer_skus[normal_key].extend(
                        TCGPlayerProvider().convert_sku_data_enum(product)
                    )
                for etched_key in etched_keys:
                    self.enhanced_tcgplayer_skus[etched_key].extend(
                        TCGPlayerProvider().convert_sku_data_enum(product)
                    )

    def to_json(self) -> Dict[str, List[Dict[str, Union[int, str]]]]:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        return self.enhanced_tcgplayer_skus
