"""
MTGJSON Prices Record V2 Object

This module defines the v2 price record schema that supports multiple providers,
price variants, and platforms while remaining aggregated by MTGJSON UUID.

The v2 schema provides a more normalized and flexible structure compared to the
legacy MtgjsonPricesObject, enabling downstream consumers to access richer
price datasets with better provider and variant granularity.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

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
        * 'default': Used for legacy conversions where variant is unknown
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


class MtgjsonPricesV2Container:
    """
    Container for organizing v2 price records by provider.

    This class manages the aggregation of price records and provides
    serialization to the {provider: [records...]} format expected
    by the AllPricesv2.json output.
    """

    def __init__(self) -> None:
        """Initialize an empty price records container."""
        self._records: Dict[str, List[MtgjsonPricesRecordV2]] = {}

    def add_record(self, record: MtgjsonPricesRecordV2) -> None:
        """
        Add a price record to the container.

        :param record: Price record to add
        """
        if record.provider not in self._records:
            self._records[record.provider] = []
        self._records[record.provider].append(record)

    def add_records(self, records: List[MtgjsonPricesRecordV2]) -> None:
        """
        Add multiple price records to the container.

        :param records: List of price records to add
        """
        for record in records:
            self.add_record(record)

    def to_json(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Serialize the container to JSON format.

        Returns a dictionary mapping provider names to lists of
        serialized price records.

        :return: JSON-serializable dictionary in {provider: [records...]} format
        """
        return {
            provider: [record.to_json() for record in records]
            for provider, records in sorted(self._records.items())
        }

    def get_providers(self) -> List[str]:
        """
        Get list of all providers in the container.

        :return: Sorted list of provider names
        """
        return sorted(self._records.keys())

    def get_record_count(self) -> int:
        """
        Get total count of all price records.

        :return: Total number of records across all providers
        """
        return sum(len(records) for records in self._records.values())
