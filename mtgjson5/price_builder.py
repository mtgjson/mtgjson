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
from .classes import MtgjsonPricesV2Container
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

    def build_today_prices_v2(self) -> MtgjsonPricesV2Container:
        """
        Build v2 price records by calling build_v2_prices() on each provider.

        Aggregates v2 records from providers that support the v2 interface.
        Providers without v2 support will return empty lists.

        :return: Container with all v2 price records from today
        """
        if not self.all_printings_path.is_file():
            LOGGER.error(
                f"Unable to build v2 prices. AllPrintings not found in {MtgjsonConfig().output_path}"
            )
            return MtgjsonPricesV2Container()

        v2_container = MtgjsonPricesV2Container()

        for provider in self.providers:
            provider_name = type(provider).__name__
            try:
                LOGGER.info(f"Building v2 prices for {provider_name}")
                records = provider.build_v2_prices(self.all_printings_path)
                if records:
                    v2_container.add_records(records)
                    LOGGER.info(f"Added {len(records)} v2 records from {provider_name}")
                else:
                    LOGGER.info(f"{provider_name} returned no v2 records")
            except Exception as exception:
                LOGGER.error(
                    f"Failed to build v2 prices for {provider_name} with error: {exception}"
                )

        total_records = v2_container.get_record_count()
        LOGGER.info(f"Built {total_records} total v2 price records")
        return v2_container

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
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        The full build prices operation for LEGACY format only.
        Prune & Update remote database.

        Returns legacy price format:
        - (all_prices, today_prices) as nested dictionaries

        For v2 format, use build_prices_v2() instead.

        :return: Tuple of (archive_prices, today_prices)
        """
        LOGGER.info("Prices Build - Building Prices (Legacy Format)")

        # We'll need AllPrintings.json to handle this
        if not self.all_printings_path.is_file():
            LOGGER.info("AllPrintings not found, attempting to download")
            self.download_old_all_printings()

        # Get today's price database
        LOGGER.info("Building new price data")
        today_prices = self.build_today_prices()
        if not today_prices:
            LOGGER.warning("Pricing information failed to generate")
            return {}, {}

        if not MtgjsonConfig().has_section("Prices"):
            return today_prices, today_prices

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

        return archive_prices, today_prices

    def build_prices_v2(
        self,
    ) -> Tuple[MtgjsonPricesV2Container, MtgjsonPricesV2Container]:
        """
        Build v2 format prices with proper archive workflow.
        Downloads existing v2 archive, merges today's v2 data, prunes old records,
        and uploads updated v2 archive.

        Returns v2 price format:
        - (all_prices_v2, today_prices_v2) as MtgjsonPricesV2Container objects

        :return: Tuple of (archive_prices_v2, today_prices_v2)
        """
        LOGGER.info("Prices Build - Building Prices (V2 Format)")

        # Get today's v2 price database
        LOGGER.info("Building new v2 price data from providers")
        today_prices_v2 = self.build_today_prices_v2()
        if not today_prices_v2 or today_prices_v2.get_record_count() == 0:
            LOGGER.warning("V2 pricing information failed to generate")
            empty_v2 = MtgjsonPricesV2Container()
            return empty_v2, empty_v2

        if not MtgjsonConfig().has_section("Prices"):
            return today_prices_v2, today_prices_v2

        bucket_name = MtgjsonConfig().get("Prices", "bucket_name")
        bucket_object_path_v2 = MtgjsonConfig().get(
            "Prices", "bucket_object_path_v2", fallback="AllPricesv2.json.tar.xz"
        )

        # Download existing v2 archive (dict format: {provider: [records...]})
        archive_dict = self.get_price_archive_data(bucket_name, bucket_object_path_v2)

        # Reconstruct container from downloaded archive
        archive_prices_v2 = self._reconstruct_v2_container_from_dict(archive_dict)

        # Merge today's v2 data into archive
        LOGGER.info("Merging new v2 price data into archive")
        self._merge_v2_prices(archive_prices_v2, today_prices_v2)

        # Prune old v2 records
        LOGGER.info("Pruning v2 price archive")
        self._prune_v2_prices_archive(archive_prices_v2)

        # Write and upload v2 archive
        LOGGER.info("Compressing v2 price data")
        local_zip_file_v2 = constants.CACHE_PATH.joinpath(bucket_object_path_v2)
        self.write_price_archive_data(local_zip_file_v2, archive_prices_v2.to_json())

        LOGGER.info("Uploading v2 price data")
        MtgjsonS3Handler().upload_file(
            str(local_zip_file_v2), bucket_name, bucket_object_path_v2
        )
        local_zip_file_v2.unlink()

        return archive_prices_v2, today_prices_v2

    def _reconstruct_v2_container_from_dict(
        self, archive_dict: Dict[str, Any]
    ) -> MtgjsonPricesV2Container:
        """
        Reconstruct a MtgjsonPricesV2Container from archived dict format.
        Archive format is {provider: [record_dicts...]}

        :param archive_dict: Downloaded archive data
        :return: Reconstructed container
        """
        from .classes.mtgjson_prices_v2 import MtgjsonPricesRecordV2

        container = MtgjsonPricesV2Container()
        if not archive_dict:
            return container

        LOGGER.info("Loading existing v2 archive data")
        # Archive is {provider: [records...]} format
        for provider, records_list in archive_dict.items():
            for record_dict in records_list:
                # Reconstruct MtgjsonPricesRecordV2 from dict
                record = MtgjsonPricesRecordV2(
                    provider=record_dict.get("provider", provider),
                    treatment=record_dict["treatment"],
                    currency=record_dict["currency"],
                    price_value=record_dict["priceValue"],
                    price_variant=record_dict["priceVariant"],
                    uuid=record_dict["uuid"],
                    platform=record_dict["platform"],
                    price_type=record_dict["priceType"],
                    date=record_dict["date"],
                    subtype=record_dict.get("subtype"),
                )
                container.add_record(record)

        return container

    def _merge_v2_prices(
        self,
        archive: MtgjsonPricesV2Container,
        today: MtgjsonPricesV2Container,
    ) -> None:
        """
        Merge today's v2 price records into the archive.
        Adds all of today's records to the archive (records are timestamped).

        :param archive: Archive container to merge into
        :param today: Today's prices to merge from
        """
        # Simply add all today's records to archive
        # Each record is timestamped with today's date, so we preserve history
        # pylint: disable=import-outside-toplevel
        from .classes.mtgjson_prices_v2 import MtgjsonPricesRecordV2

        today_serialized = today.to_json()
        for _, records_list in today_serialized.items():
            for record_dict in records_list:
                # Reconstruct record and add to archive
                record = MtgjsonPricesRecordV2(
                    provider=record_dict["provider"],
                    treatment=record_dict["treatment"],
                    currency=record_dict["currency"],
                    price_value=record_dict["priceValue"],
                    price_variant=record_dict["priceVariant"],
                    uuid=record_dict["uuid"],
                    platform=record_dict["platform"],
                    price_type=record_dict["priceType"],
                    date=record_dict["date"],
                    subtype=record_dict.get("subtype"),
                )
                archive.add_record(record)

    def _prune_v2_prices_archive(self, archive: MtgjsonPricesV2Container) -> None:
        """
        Prune old v2 price records similar to legacy pruning.
        Removes records older than configured cutoff date.

        :param archive: Archive container to prune
        """
        LOGGER.info("Pruning v2 archive data")
        cutoff_date = datetime.date.today() - datetime.timedelta(
            weeks=int(MtgjsonConfig().get("Prices", "max_weeks", fallback="104"))
        )
        cutoff_date_str = cutoff_date.isoformat()

        # Build a new container with only non-expired records
        pruned_container = MtgjsonPricesV2Container()

        # Iterate through all providers and their records
        # pylint: disable=protected-access
        for provider in archive.get_providers():
            # Access internal _records directly since there's no public getter
            if provider in archive._records:
                for record in archive._records[provider]:
                    # Keep record if date is after cutoff
                    if record.date >= cutoff_date_str:
                        pruned_container.add_record(record)

        # Replace archive contents with pruned data
        archive._records = pruned_container._records  # pylint: disable=protected-access
