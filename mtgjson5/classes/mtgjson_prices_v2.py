"""
MTGJSON Prices Record V2 Object

This module defines the v2 price record schema that supports multiple providers,
price variants, and platforms while remaining aggregated by MTGJSON UUID.

The v2 schema provides a more normalized and flexible structure compared to the
legacy MtgjsonPricesObject, enabling downstream consumers to access richer
price datasets with better provider and variant granularity.
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional

from .json_object import JsonObject


@dataclass
class MtgjsonPricesRecordV2(JsonObject):
    """
    MTGJSON Prices Record V2 Object

    Represents a single price data point with full context about provider,
    treatment (foil/non-foil/etched), platform (paper/mtgo), and price type
    (retail/buy_list).

    Schema Fields:
    - provider: Price provider name (e.g., 'cardkingdom', 'tcgplayer')
    - treatment: Card finish type ('normal', 'foil', 'etched')
    - subtype: Optional additional categorization (e.g., price tier)
    - currency: Currency code (e.g., 'USD', 'EUR')
    - price_value: The actual price value as a float
    - price_variant: Provider-specific price tier/type:
        * 'legacy': Used for conversions from legacy format where variant is unknown
        * TCGPlayer: 'market', 'low', 'mid', 'high', 'tcgdirect_low'
        * Other providers may have their own variants
    - uuid: MTGJSON card UUID this price applies to
    - platform: Trading platform ('paper', 'mtgo')
    - price_type: Transaction type ('retail', 'buy_list')
    - date: ISO date string for when this price was recorded
    """

    provider: str
    treatment: str
    currency: str
    price_value: float
    price_variant: str
    uuid: str
    platform: str
    price_type: str
    date: str
    subtype: Optional[str] = None

    def to_json(self) -> Dict[str, Any]:
        """
        Serialize the price record to JSON format.

        Returns a dictionary with camelCase keys for JSON output.
        Excludes None values for optional fields.

        :return: JSON-serializable dictionary
        """
        result = {
            "provider": self.provider,
            "treatment": self.treatment,
            "currency": self.currency,
            "priceValue": self.price_value,
            "priceVariant": self.price_variant,
            "uuid": self.uuid,
            "platform": self.platform,
            "priceType": self.price_type,
            "date": self.date,
        }

        if self.subtype is not None:
            result["subtype"] = self.subtype

        return result
