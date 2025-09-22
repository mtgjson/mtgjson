"""
TCGCSV 3rd party provider

Provider for fetching pricing data from tcgcsv.com API endpoints.
This provider focuses on pricing data integration with MTGJSON.
"""

import logging
from typing import Any, Dict, List, Optional, Union

from singleton_decorator import singleton

from ..classes import MtgjsonPricesObject
from ..providers.abstract import AbstractProvider

LOGGER = logging.getLogger(__name__)


@singleton
class TcgCsvProvider(AbstractProvider):
    """
    TCGCSV provider for pricing data

    Fetches pricing information from tcgcsv.com public endpoints.
    This provider is focused on enriching MTGJSON price data with
    additional pricing points from tcgcsv's comprehensive database.
    """

    # TCGCSV API configuration
    base_url: str = "https://tcgcsv.com/tcgplayer/1"

    def __init__(self) -> None:
        """
        Initialize the TcgCsvProvider
        """
        super().__init__(self._build_http_header())

    def _build_http_header(self) -> Dict[str, str]:
        """
        Construct HTTP headers for TCGCSV API requests

        TCGCSV appears to use public endpoints without authentication
        based on the tcgcsv-etl implementation.

        :return: HTTP headers for requests
        """
        return {
            "Content-Type": "application/json",
            "User-Agent": "MTGJSON-TcgCsvProvider/1.0",
        }

    def download(
        self, url: str, params: Optional[Dict[str, Union[str, int]]] = None
    ) -> Any:
        """
        Download content from TCGCSV API

        :param url: URL to download from
        :param params: Options for URL download
        :return: JSON response from API
        """
        response = self.session.get(url, params=params)
        self.log_download(response)

        if not response.ok:
            LOGGER.error(f"TCGCSV API error ({response.status_code}): {response.text}")
            response.raise_for_status()

        return response.json()

    def fetch_set_prices(self, set_code: str, group_id: str) -> List[Dict[str, Any]]:
        """
        Fetch pricing data for a specific set using its group ID

        :param set_code: MTGJSON set code (for logging)
        :param group_id: TCGCSV group ID for the set
        :return: List of price data records
        """
        url = f"{self.base_url}/{group_id}/prices"

        try:
            response_data = self.download(url)

            # Validate response structure based on tcgcsv-etl
            if not response_data.get("success", False):
                errors = response_data.get("errors", [])
                LOGGER.warning(
                    f"TCGCSV API returned success=false for {set_code}: {errors}"
                )
                return []

            results = response_data.get("results", [])
            LOGGER.info(f"Fetched {len(results)} price records for {set_code}")

            return results

        except Exception as e:
            LOGGER.error(
                f"Failed to fetch prices for {set_code} (group {group_id}): {e}"
            )
            return []

    def convert_to_mtgjson_prices(
        self, price_data: List[Dict[str, Any]], set_code: str
    ) -> Dict[str, MtgjsonPricesObject]:
        """
        Convert TCGCSV price data to MTGJSON price objects

        :param price_data: Raw price data from TCGCSV API
        :param set_code: Set code for logging
        :return: Mapping of card keys to price objects
        """
        price_objects: Dict[str, MtgjsonPricesObject] = {}

        for price_record in price_data:
            try:
                # Extract price information
                product_id = price_record.get("productId")
                if not product_id:
                    continue

                # Create price object for this product
                # Use product_id as the key for now (will need mapping later)
                key = str(product_id)

                # Create MTGJSON price object
                price_obj = MtgjsonPricesObject(
                    source="paper",
                    provider="tcgcsv",
                    date=self.today_date,
                    currency="USD",
                )

                # Map price fields from TCGCSV to MTGJSON
                market_price = price_record.get("marketPrice")
                sub_type_name = price_record.get("subTypeName", "").lower()

                # Determine if this is foil, etched, or normal
                if market_price is not None:
                    if "etched" in sub_type_name:
                        price_obj.sell_etched = float(market_price)
                    elif "foil" in sub_type_name:
                        price_obj.sell_foil = float(market_price)
                    else:
                        price_obj.sell_normal = float(market_price)

                # Add additional price points if available
                low_price = price_record.get("lowPrice")
                mid_price = price_record.get("midPrice")
                high_price = price_record.get("highPrice")
                direct_low_price = price_record.get("directLowPrice")

                # Store additional pricing info in object attributes for potential future use
                # Note: Current MtgjsonPricesObject doesn't have fields for these,
                # but we can extend it or use them for enhanced pricing logic

                price_objects[key] = price_obj

            except Exception as e:
                LOGGER.warning(f"Failed to convert price record for {set_code}: {e}")
                continue

        LOGGER.info(f"Converted {len(price_objects)} price records for {set_code}")
        return price_objects

    def generate_today_price_dict_for_set(
        self, set_code: str, group_id: str
    ) -> Dict[str, MtgjsonPricesObject]:
        """
        Generate MTGJSON price dictionary for a specific set

        This is the main entry point for getting pricing data for a set.

        :param set_code: MTGJSON set code
        :param group_id: TCGCSV group ID for the set
        :return: Mapping of card identifiers to price objects
        """
        LOGGER.info(f"Building TCGCSV price data for set {set_code}")

        # Fetch raw price data
        price_data = self.fetch_set_prices(set_code, group_id)

        if not price_data:
            LOGGER.warning(f"No price data available for {set_code}")
            return {}

        # Convert to MTGJSON format
        return self.convert_to_mtgjson_prices(price_data, set_code)
