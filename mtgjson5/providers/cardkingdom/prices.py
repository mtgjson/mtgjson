"""Card Kingdom price processing for MTGJSON format."""

import datetime
import hashlib
import logging
from typing import Any

import polars as pl

LOGGER = logging.getLogger(__name__)

CK_URL_PREFIX = "https://www.cardkingdom.com"


def url_keygen(seed: str) -> str:
    """Generate MTGJSON redirect URL hash from seed string."""
    hash_val = hashlib.sha256(seed.encode()).hexdigest()[:16]
    return f"https://mtgjson.com/links/{hash_val}"


def generate_purchase_url(url_path: str | None, uuid: str) -> str | None:
    """
    Generate MTGJSON-style purchase URL for Card Kingdom.

    Args:
        url_path: CK URL path (e.g., "/mtg/card/name")
        uuid: MTGJSON card UUID

    Returns:
        Hashed redirect URL or None if url_path is None
    """
    if not url_path:
        return None
    return url_keygen(f"{CK_URL_PREFIX}{url_path}{uuid}")


class CardKingdomPriceProcessor:
    """
    Processes CK pricing data into MTGJSON format.

    Handles:
    - Price dictionary generation for PriceBuilder
    - UUID mapping from scryfall_id
    - Foil/etched/normal price categorization
    """

    def __init__(self, pricing_df: pl.DataFrame):
        """
        Initialize with pricing DataFrame.

        Expected columns: ck_id, scryfall_id, is_foil, is_etched,
                         price_retail, price_buy, qty_retail, qty_buying
        """
        self._pricing_df = pricing_df

    def build_ck_id_to_uuid_map(
        self,
        scryfall_to_uuid: pl.DataFrame,
    ) -> dict[str, set[str]]:
        """
        Build CK ID -> MTGJSON UUID mapping.

        Args:
            scryfall_to_uuid: DataFrame with 'scryfall_id' and 'uuid' columns

        Returns:
            Dict mapping CK product ID to set of MTGJSON UUIDs
        """
        joined = (
            self._pricing_df.select(["ck_id", "scryfall_id"])
            .unique()
            .join(
                scryfall_to_uuid.select(["scryfall_id", "uuid"]),
                on="scryfall_id",
                how="inner",
            )
        )

        result: dict[str, set[str]] = {}
        for row in joined.iter_rows(named=True):
            ck_id = row["ck_id"]
            uuid = row["uuid"]
            if ck_id not in result:
                result[ck_id] = set()
            result[ck_id].add(uuid)

        LOGGER.info(f"Built CK ID -> UUID mapping: {len(result):,} entries")
        return result

    def join_uuids(self, scryfall_to_uuid: pl.DataFrame) -> pl.DataFrame:
        """
        Join pricing data with MTGJSON UUIDs.

        Returns DataFrame with uuid replacing scryfall_id.
        """
        return self._pricing_df.join(
            scryfall_to_uuid.select(["scryfall_id", "uuid"]),
            on="scryfall_id",
            how="inner",
        ).select(
            [
                "uuid",
                "ck_id",
                "is_foil",
                "is_etched",
                "price_retail",
                "price_buy",
                "qty_retail",
                "qty_buying",
            ]
        )

    def generate_today_prices(
        self,
        ck_id_to_uuid: dict[str, set[str]],
    ) -> dict[str, Any]:
        """
        Generate today's prices in MTGJSON format.

        Compatible with PriceBuilder's expected format.

        Args:
            ck_id_to_uuid: Mapping from CK product ID to MTGJSON UUID(s)

        Returns:
            Dict mapping UUID -> MtgjsonPriceEntry
        """
        from mtgjson5.models.containers import MtgjsonPriceEntry

        today_date = datetime.datetime.today().strftime("%Y-%m-%d")
        result: dict[str, MtgjsonPriceEntry] = {}

        for row in self._pricing_df.iter_rows(named=True):
            ck_id = row["ck_id"]
            if ck_id not in ck_id_to_uuid:
                continue

            uuids = ck_id_to_uuid[ck_id]
            is_foil = row["is_foil"]
            is_etched = row["is_etched"]
            price_retail = row["price_retail"]
            price_buy = row["price_buy"]
            qty_retail = row["qty_retail"]
            qty_buying = row["qty_buying"]

            for uuid in uuids:
                if uuid not in result:
                    result[uuid] = MtgjsonPriceEntry("paper", "cardkingdom", today_date, "USD")

                prices_obj = result[uuid]

                # Retail price (in stock only)
                if price_retail is not None and qty_retail > 0:
                    if is_etched:
                        prices_obj.sell_etched = price_retail
                    elif is_foil:
                        prices_obj.sell_foil = price_retail
                    else:
                        prices_obj.sell_normal = price_retail

                # Buylist price (buying only)
                if price_buy is not None and qty_buying > 0:
                    if is_etched:
                        prices_obj.buy_etched = price_buy
                    elif is_foil:
                        prices_obj.buy_foil = price_buy
                    else:
                        prices_obj.buy_normal = price_buy

        LOGGER.info(f"Generated prices for {len(result):,} UUIDs")
        return result
