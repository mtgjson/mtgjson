"""MTGJSON TCGPlayer SKUs compiled model for product identifiers."""

import logging
import pathlib
from collections import defaultdict
from typing import Any

from pydantic import Field

from ... import providers, utils
from ..mtgjson_base import MTGJsonCompiledModel

TCGPlayerProvider = providers.tcgplayer.TCGPlayerProvider
generate_entity_mapping = utils.generate_entity_mapping

LOGGER = logging.getLogger(__name__)


def _default_sku_dict() -> dict[str, list[dict[str, int | str]]]:
    """Factory for creating typed defaultdict."""
    return {}


class MtgjsonTcgplayerSkusObject(MTGJsonCompiledModel):
    """
    The TCGplayer SKUs compiled output mapping UUIDs to their TCGplayer product SKU data.
    """

    enhanced_tcgplayer_skus: dict[str, list[dict[str, int | str]]] = Field(
        default_factory=dict,
        description="A dictionary mapping UUIDs to their TCGplayer SKU information.",
    )

    def __init__(self, all_printings_path: pathlib.Path, **kwargs: Any) -> None:
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
