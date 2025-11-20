from typing import Dict, Optional

from ..mtgjson_base import MTGJsonModel


class MtgjsonIdentifiersObject(MTGJsonModel):
    """
    MTGJSON Singular Card.Identifiers Object
    """

    # Fields with empty string defaults (initialized in parent __init__)
    multiverse_id: str = ""
    card_kingdom_id: str = ""
    tcgplayer_product_id: str = ""

    # Optional fields
    card_kingdom_etched_id: Optional[str] = None
    card_kingdom_foil_id: Optional[str] = None
    cardsphere_foil_id: Optional[str] = None
    cardsphere_id: Optional[str] = None
    mcm_id: Optional[str] = None
    mcm_meta_id: Optional[str] = None
    mtg_arena_id: Optional[str] = None
    mtgjson_foil_version_id: Optional[str] = None
    mtgjson_non_foil_version_id: Optional[str] = None
    mtgjson_v4_id: Optional[str] = None
    mtgo_foil_id: Optional[str] = None
    mtgo_id: Optional[str] = None
    scryfall_id: Optional[str] = None
    scryfall_illustration_id: Optional[str] = None
    scryfall_card_back_id: Optional[str] = None
    scryfall_oracle_id: Optional[str] = None
    tcgplayer_etched_product_id: Optional[str] = None

    def to_json(self) -> Dict[str, str]:
        """
        Custom JSON serialization that filters out empty values
        :return: JSON object with non-empty values only
        """
        parent = super().to_json()
        return {key: value for key, value in parent.items() if value}
