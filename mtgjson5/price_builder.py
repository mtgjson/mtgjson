"""
Construct Prices for MTGJSON
"""

import datetime
import json
import logging
import lzma
import pathlib
import subprocess
from typing import Any, Dict, List, Optional, Tuple

import dateutil.relativedelta
import mergedeep
import requests

from . import constants
from .classes import MtgjsonPricesRecordV2, MtgjsonPricesV2Container
from .mtgjson_config import MtgjsonConfig
from .mtgjson_s3_handler import MtgjsonS3Handler
from .providers import (
    CardHoarderProvider,
    CardKingdomProvider,
    CardMarketProvider,
    ManapoolPricesProvider,
    MultiverseBridgeProvider,
    TCGPlayerProvider,
)
from .providers.abstract import AbstractProvider

LOGGER = logging.getLogger(__name__)


class PriceBuilder:
    """
    Build Daily Prices for defined providers
    """

    providers: List[AbstractProvider]
    all_printings_path: pathlib.Path

    def __init__(
        self,
        *providers: AbstractProvider,
        all_printings_path: Optional[pathlib.Path] = None,
    ) -> None:
        if providers:
            self.providers = list(providers)
        else:
            self.providers = [
                CardHoarderProvider(),
                TCGPlayerProvider(),
                CardMarketProvider(),
                CardKingdomProvider(),
                MultiverseBridgeProvider(),
                ManapoolPricesProvider(),
            ]

        self.all_printings_path = (
            all_printings_path
            if all_printings_path
            else MtgjsonConfig().output_path.joinpath("AllPrintings.json")
        )

    @staticmethod
    def prune_prices_archive(content: Dict[str, Any], months: int = 3) -> None:
        """
        Prune entries from the MTGJSON database that are older than `months` old
        :param content: Dataset to modify
        :param months: How many months back should we keep (default = 3)
        """
        prune_date_str = (
            datetime.date.today() + dateutil.relativedelta.relativedelta(months=-months)
        ).strftime("%Y-%m-%d")
        keys_pruned = 0

        def prune_recursive(obj: Dict[str, Any], depth: int = 0) -> None:
            """
            Recursive pruner to pluck out bad dates and empty fields
            """
            nonlocal keys_pruned
            if depth == 5:
                for date in list(obj.keys()):
                    if date < prune_date_str:
                        del obj[date]
                        keys_pruned += 1
            elif isinstance(obj, dict):
                for key, value in list(obj.items()):
                    prune_recursive(value, depth + 1)
                    if not value:
                        del obj[key]
                        keys_pruned += 1

        LOGGER.info("Determining keys to prune")
        prune_recursive(content)
        LOGGER.info(f"Pruned {keys_pruned} structs")

    def build_today_prices(self) -> Dict[str, Any]:
        """
        Get today's prices from upstream sources and combine them together
        :return: Today's prices (to be merged into archive)
        """
        if not self.all_printings_path.is_file():
            LOGGER.error(
                f"Unable to build prices. AllPrintings not found in {MtgjsonConfig().output_path}"
            )
            return {}

        final_results: Dict[str, Any] = {}
        mergedeep.merge(
            final_results,
            *[self._generate_prices(provider) for provider in self.providers],
        )

        return final_results

    def _generate_prices(self, provider: Any) -> Dict[str, Any]:
        """
        Generate the prices for a source and prepare them for
        merging with other entities
        :param provider: MTGJSON Provider that implements generate_today_price_dict
        :return Manageable data for MTGJSON prices
        """
        try:
            preprocess_prices = provider.generate_today_price_dict(
                self.all_printings_path
            )

            final_prices: Dict[str, Any] = json.loads(
                json.dumps(preprocess_prices, default=lambda o: o.to_json())
            )
            return final_prices
        except Exception as exception:
            LOGGER.error(
                f"Failed to compile for {type(provider).__name__} with error: {exception}"
            )
            return {}

    @staticmethod
    def get_price_archive_data(
        bucket_name: str,
        bucket_object_path: str,
    ) -> Dict[str, Dict[str, float]]:
        """
        Download compiled MTGJSON price data
        :return: MTGJSON price data
        """
        LOGGER.info("Downloading Current Price Data File")

        constants.CACHE_PATH.mkdir(parents=True, exist_ok=True)
        temp_zip_file = constants.CACHE_PATH.joinpath("temp.tar.xz")

        downloaded_successfully = MtgjsonS3Handler().download_file(
            bucket_name, bucket_object_path, str(temp_zip_file)
        )
        if not downloaded_successfully:
            LOGGER.warning("Download of current price data failed")
            return {}

        with lzma.open(temp_zip_file) as file:
            contents = dict(json.load(file))

        temp_zip_file.unlink()
        return contents

    @staticmethod
    def write_price_archive_data(
        local_save_path: pathlib.Path, price_data: Dict[str, Any]
    ) -> None:
        """
        Write price data to a compressed archive file
        :param local_save_path: Where to save compressed archive file
        :param price_data: Data to compress into that archive file
        """
        local_save_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_save_path = local_save_path.parent.joinpath(local_save_path.stem)

        LOGGER.info(f"Dumping price data to {tmp_save_path}")
        with tmp_save_path.open("w") as temp_file:
            json.dump(price_data, temp_file)
        LOGGER.info(
            f"Finished writing to {tmp_save_path} (Size = {tmp_save_path.stat().st_size} bytes)"
        )

        LOGGER.info(f"Compressing {tmp_save_path} for upload")
        subprocess.check_call(["xz", str(tmp_save_path)])
        LOGGER.info(
            f"Finished compressing content to {local_save_path} (Size = {local_save_path.stat().st_size} bytes)"
        )

    @staticmethod
    def convert_legacy_to_v2(legacy_prices: Dict[str, Any]) -> MtgjsonPricesV2Container:
        """
        Convert legacy price format to v2 price record format.

        Legacy format: {uuid: {platform: {provider: {retail/buylist: {treatment: {date: price}}}}}}
        V2 format: {provider: [MtgjsonPricesRecordV2(...), ...]}

        Note: The legacy format does not preserve price variant information (e.g., whether
        a TCGPlayer price is 'market', 'low', 'mid', 'high', or 'tcgdirect_low'). Therefore,
        all converted prices use 'legacy' as the price_variant to indicate they originated
        from the legacy format. In the future, providers can generate v2 records directly
        with actual variant information.

        :param legacy_prices: Legacy price structure
        :return: V2 price container with converted records
        """
        v2_container = MtgjsonPricesV2Container()

        for uuid, platforms in legacy_prices.items():
            for platform, providers in platforms.items():
                for provider, price_data in providers.items():
                    currency = price_data.get("currency", "USD")

                    # Process retail (sell) prices
                    for treatment, dates in price_data.get("retail", {}).items():
                        for date, price_value in dates.items():
                            record = MtgjsonPricesRecordV2(
                                provider=provider,
                                treatment=treatment,
                                currency=currency,
                                price_value=float(price_value),
                                price_variant="legacy",
                                uuid=uuid,
                                platform=platform,
                                price_type="retail",
                                date=date,
                            )
                            v2_container.add_record(record)

                    # Process buylist (buy) prices
                    for treatment, dates in price_data.get("buylist", {}).items():
                        for date, price_value in dates.items():
                            record = MtgjsonPricesRecordV2(
                                provider=provider,
                                treatment=treatment,
                                currency=currency,
                                price_value=float(price_value),
                                price_variant="legacy",
                                uuid=uuid,
                                platform=platform,
                                price_type="buy_list",
                                date=date,
                            )
                            v2_container.add_record(record)

        return v2_container

    def download_old_all_printings(self) -> None:
        """
        Download the hosted version of AllPrintings from MTGJSON
        for future consumption
        """
        file_bytes = b""
        file_data = requests.get(
            "https://mtgjson.com/api/v5/AllPrintings.json.xz", stream=True, timeout=60
        )
        for chunk in file_data.iter_content(chunk_size=1024 * 36):
            if chunk:
                file_bytes += chunk

        MtgjsonConfig().output_path.mkdir(parents=True, exist_ok=True)
        with self.all_printings_path.open("w", encoding="utf8") as f:
            f.write(lzma.decompress(file_bytes).decode())

    def build_prices(
        self,
    ) -> Tuple[
        Tuple[Dict[str, Any], Dict[str, Any]],
        Tuple[MtgjsonPricesV2Container, MtgjsonPricesV2Container],
    ]:
        """
        The full build prices operation.
        Prune & Update remote database.

        Returns both legacy and v2 price formats:
        - Legacy: (all_prices, today_prices) as nested dictionaries
        - V2: (all_prices_v2, today_prices_v2) as MtgjsonPricesV2Container objects

        :return: Tuple of ((legacy_all, legacy_today), (v2_all, v2_today))
        """
        LOGGER.info("Prices Build - Building Prices")

        # We'll need AllPrintings.json to handle this
        if not self.all_printings_path.is_file():
            LOGGER.info("AllPrintings not found, attempting to download")
            self.download_old_all_printings()

        # Get today's price database
        LOGGER.info("Building new price data")
        today_prices = self.build_today_prices()
        if not today_prices:
            LOGGER.warning("Pricing information failed to generate")
            empty_v2 = MtgjsonPricesV2Container()
            return ({}, {}), (empty_v2, empty_v2)

        if not MtgjsonConfig().has_section("Prices"):
            # Convert to v2 format
            today_prices_v2 = self.convert_legacy_to_v2(today_prices)
            return (today_prices, today_prices), (today_prices_v2, today_prices_v2)

        bucket_name = MtgjsonConfig().get("Prices", "bucket_name")
        bucket_object_path = MtgjsonConfig().get("Prices", "bucket_object_path")

        archive_prices = self.get_price_archive_data(bucket_name, bucket_object_path)

        # Update local copy of database
        LOGGER.info("Merging old and new price data")
        mergedeep.merge(archive_prices, today_prices)

        # Prune local copy of database
        LOGGER.info("Pruning price data")
        self.prune_prices_archive(archive_prices)

        LOGGER.info("Compressing price data")
        local_zip_file = constants.CACHE_PATH.joinpath(bucket_object_path)
        self.write_price_archive_data(local_zip_file, archive_prices)

        # Push changes to remote database
        LOGGER.info("Uploading price data")
        MtgjsonS3Handler().upload_file(
            str(local_zip_file), bucket_name, bucket_object_path
        )
        local_zip_file.unlink()

        # Convert both legacy formats to v2
        LOGGER.info("Converting legacy prices to v2 format")
        archive_prices_v2 = self.convert_legacy_to_v2(archive_prices)
        today_prices_v2 = self.convert_legacy_to_v2(today_prices)

        return (archive_prices, today_prices), (archive_prices_v2, today_prices_v2)
