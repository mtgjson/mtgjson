"""
TCGCSV 3rd party provider

Provider for fetching pricing data from tcgcsv.com API endpoints.
This provider focuses on pricing data integration with MTGJSON.
"""

import logging
import pathlib
from collections import defaultdict
from typing import Any, Dict, List, Optional, Union

from singleton_decorator import singleton

from ..classes import MtgjsonPricesObject
from ..providers.abstract import AbstractProvider
from ..utils import generate_entity_mapping

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

    def fetch_set_products(self, set_code: str, group_id: str) -> List[Dict[str, Any]]:
        """
        Fetch product data for a specific set using its group ID

        This fetches product metadata including display names, variants,
        collector numbers, and other product information.

        :param set_code: MTGJSON set code (for logging)
        :param group_id: TCGCSV group ID for the set
        :return: List of product data records
        """
        url = f"{self.base_url}/{group_id}/products"

        try:
            response_data = self.download(url)

            # Validate response structure
            if not response_data.get("success", False):
                errors = response_data.get("errors", [])
                LOGGER.warning(
                    f"TCGCSV API returned success=false for {set_code} products: {errors}"
                )
                return []

            results = response_data.get("results", [])
            LOGGER.info(f"Fetched {len(results)} product records for {set_code}")

            return results

        except Exception as e:
            LOGGER.error(
                f"Failed to fetch products for {set_code} (group {group_id}): {e}"
            )
            return []

    def _inner_translate_today_price_dict(
        self, set_code: str, group_id: str
    ) -> Dict[str, Dict[str, float]]:
        """
        Fetch pricing data and convert it to a dictionary of product IDs
        with their pricing information by finish type

        :param set_code: Set code for logging
        :param group_id: TCGCSV group ID for the set
        :return: Dictionary of product IDs to price objects by finish type
        """
        mapping: Dict[str, Dict[str, float]] = defaultdict(dict)

        # Fetch raw price data
        price_data = self.fetch_set_prices(set_code, group_id)
        if not price_data:
            return mapping

        # Process each price record
        for price_record in price_data:
            try:
                product_id = price_record.get("productId")
                if not product_id:
                    continue

                # Get the market price and determine finish type
                market_price = price_record.get("marketPrice")
                if market_price is None:
                    continue

                sub_type_name = price_record.get("subTypeName", "").lower()
                key = str(product_id)

                # Map to the appropriate finish type
                if "etched" in sub_type_name:
                    mapping[key]["etched"] = float(market_price)
                elif "foil" in sub_type_name:
                    mapping[key]["foil"] = float(market_price)
                else:
                    mapping[key]["normal"] = float(market_price)

            except Exception as e:
                LOGGER.warning(f"Failed to process price record for {set_code}: {e}")
                continue

        LOGGER.info(f"Processed {len(mapping)} unique product IDs for {set_code}")
        return mapping

    def fetch_set_enrichment_data(
        self, set_code: str, group_id: str
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch and combine both product and pricing data for a set

        This method fetches both product metadata and pricing data,
        then combines them into a single enrichment dataset indexed by product ID.

        :param set_code: MTGJSON set code
        :param group_id: TCGCSV group ID for the set
        :return: Dictionary mapping product IDs to enrichment data
        """
        enrichment_data: Dict[str, Dict[str, Any]] = defaultdict(dict)

        # Fetch product data for display names and metadata
        product_data = self.fetch_set_products(set_code, group_id)
        LOGGER.info(f"Processing {len(product_data)} products for {set_code}")

        # Index products by product ID
        for product in product_data:
            try:
                product_id = product.get("productId")
                if not product_id:
                    continue

                key = str(product_id)

                # Extract key product information
                enrichment_data[key]["tcgplayer_display_name"] = product.get("name", "")
                enrichment_data[key]["clean_name"] = product.get("cleanName", "")
                enrichment_data[key]["image_url"] = product.get("imageUrl", "")
                enrichment_data[key]["tcgplayer_url"] = product.get("url", "")

                # Extract collector number from extendedData
                extended_data = product.get("extendedData", [])
                collector_number = None
                rarity = None

                for data_item in extended_data:
                    if data_item.get("name") == "Number":
                        collector_number = data_item.get("value")
                    elif data_item.get("name") == "Rarity":
                        rarity = data_item.get("value")

                if collector_number:
                    enrichment_data[key]["collector_number"] = collector_number
                if rarity:
                    enrichment_data[key]["rarity"] = rarity

            except Exception as e:
                LOGGER.warning(
                    f"Failed to process product {product.get('productId', 'unknown')}: {e}"
                )
                continue

        # Now add pricing data
        price_mapping = self._inner_translate_today_price_dict(set_code, group_id)
        for product_id, prices in price_mapping.items():
            if product_id in enrichment_data:
                enrichment_data[product_id]["prices"] = prices
            else:
                # Product has pricing but no product metadata
                enrichment_data[product_id] = {"prices": prices}

        LOGGER.info(
            f"Generated enrichment data for {len(enrichment_data)} products in {set_code}"
        )
        return enrichment_data

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

        # Get pricing data by product ID and finish type
        product_price_mapping = self._inner_translate_today_price_dict(
            set_code, group_id
        )
        if not product_price_mapping:
            LOGGER.warning(f"No price data available for {set_code}")
            return {}

        # Build final price map - each card gets its own price object
        final_data = {}
        for product_id, finish_to_price in product_price_mapping.items():
            # Currently using product_id as the key (will need UUID mapping later)
            # This matches the structure in ManapoolPricesProvider
            final_data[product_id] = MtgjsonPricesObject(
                source="paper", provider="tcgcsv", date=self.today_date, currency="USD"
            )

            if "normal" in finish_to_price:
                final_data[product_id].sell_normal = finish_to_price.get("normal")
            if "foil" in finish_to_price:
                final_data[product_id].sell_foil = finish_to_price.get("foil")
            if "etched" in finish_to_price:
                final_data[product_id].sell_etched = finish_to_price.get("etched")

        LOGGER.info(f"Generated {len(final_data)} price entries for {set_code}")
        return final_data

    def generate_today_price_dict(
        self, all_printings_path: pathlib.Path
    ) -> Dict[str, MtgjsonPricesObject]:
        """
        Generate a single-day price structure for all sets

        Note: This is a placeholder for integration with the price_builder system.
        It will need to map product IDs to MTGJSON UUIDs using proper mapping tables.

        :param all_printings_path: Path to AllPrintings.json for UUID mapping
        :return: MTGJSON prices single day structure
        """
        LOGGER.info("Building TCGCSV retail data")
        # TODO: Implement UUID mapping from tcgcsv product IDs to MTGJSON UUIDs
        # TODO: Implement set code to group ID mapping

        # This is a placeholder implementation for a single set (FIC)
        # In the future, this will iterate over all available sets with mappings
        final_data = self.generate_today_price_dict_for_set("FIC", "24220")

        return final_data
