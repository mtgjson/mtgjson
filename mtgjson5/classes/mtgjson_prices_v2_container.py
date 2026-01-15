"""
MTGJSON Prices V2 Container

This module defines the container class for organizing v2 price records by provider.
"""

from typing import Any, Dict, List

from .mtgjson_prices_v2 import MtgjsonPricesRecordV2


class MtgjsonPricesV2Container:
    """
    Container for organizing v2 price records by provider.

    This class manages the aggregation of price records and provides
    serialization to the {provider: [records...]} format expected
    by the AllPricesv2.json output.
    """

    _records: Dict[str, List[MtgjsonPricesRecordV2]]

    def __init__(self) -> None:
        """Initialize an empty price records container."""
        self._records = {}

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

    def get_records_for_provider(self, provider: str) -> List[MtgjsonPricesRecordV2]:
        """
        Get all records for a specific provider.

        :param provider: Provider name
        :return: List of records for the provider, empty list if provider not found
        """
        return self._records.get(provider, [])

    def get_all_records(self) -> Dict[str, List[MtgjsonPricesRecordV2]]:
        """
        Get all records from the container.

        :return: Dictionary mapping provider names to lists of records
        """
        return self._records

    def set_records(self, records: Dict[str, List[MtgjsonPricesRecordV2]]) -> None:
        """
        Replace all records in the container.

        :param records: Dictionary mapping provider names to lists of records
        """
        self._records = records
