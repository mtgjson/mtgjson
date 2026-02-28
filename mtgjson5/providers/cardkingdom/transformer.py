"""Card Kingdom data transformation and normalization."""

import logging

import polars as pl

from .models import CKRecord, ConditionValues

LOGGER = logging.getLogger(__name__)


def parse_price(price_str: str | None) -> float | None:
    """Parse price string to float, returning None for empty/invalid."""
    if not price_str:
        return None
    try:
        return float(price_str)
    except ValueError:
        return None


class CardKingdomTransformer:
    """
    Transforms raw CK API records into normalized DataFrames.

    Responsibilities:
    - Record to DataFrame conversion
    - Column normalization and type casting
    - Pivoting (one row per scryfall_id with foil/non-foil columns)
    """

    @staticmethod
    def records_to_dataframe(records: list[CKRecord]) -> pl.DataFrame:
        """
        Convert CK API records to flat DataFrame.

        One row per SKU (foil and non-foil are separate rows).
        Handles union type fields with getattr fallbacks.
        """
        rows = []

        for card in records:
            cv = getattr(card, "condition_values", ConditionValues())

            rows.append(
                {
                    "id": card.id,
                    "sku": getattr(card, "sku", ""),
                    "name": card.name,
                    "edition": card.edition,
                    "variation": getattr(card, "variation", None),
                    "is_foil": card.is_foil,
                    "scryfall_id": getattr(card, "scryfall_id", None),
                    "url": card.url,
                    "price_retail": parse_price(card.price_retail),
                    "qty_retail": card.qty_retail,
                    "price_buy": parse_price(card.price_buy),
                    "qty_buying": card.qty_buying,
                    # Condition-specific pricing (V2 only)
                    "condition_nm_price": parse_price(cv.nm_price),
                    "condition_nm_qty": cv.nm_qty,
                    "condition_ex_price": parse_price(cv.ex_price),
                    "condition_ex_qty": cv.ex_qty,
                    "condition_vg_price": parse_price(cv.vg_price),
                    "condition_vg_qty": cv.vg_qty,
                    "condition_g_price": parse_price(cv.g_price),
                    "condition_g_qty": cv.g_qty,
                }
            )

        LOGGER.info(f"Transformed {len(rows):,} CK records to DataFrame")
        return pl.DataFrame(rows)

    @staticmethod
    def add_derived_columns(df: pl.DataFrame) -> pl.DataFrame:
        """
        Add derived columns for foil/etched detection.

        - is_foil_bool: True if is_foil == 'true' (don't use SKU - set codes like FDN start with F)
        - is_etched: True if variation contains 'Foil Etched'
        """
        return df.with_columns(
            [
                (pl.col("is_foil").str.to_lowercase() == "true").alias("is_foil_bool"),
                pl.col("variation").fill_null("").str.contains("Foil Etched").alias("is_etched"),
            ]
        )

    @staticmethod
    def pivot_by_scryfall_id(df: pl.DataFrame) -> pl.DataFrame:
        """
        Pivot to one row per scryfall_id with foil/non-foil/etched columns.

        Output columns:
        - id (scryfall_id renamed for joins)
        - cardKingdomId (non-foil CK product ID)
        - cardKingdomUrl (non-foil URL path)
        - cardKingdomFoilId (foil CK product ID)
        - cardKingdomFoilUrl (foil URL path)
        - cardKingdomEtchedId (etched CK product ID)
        - cardKingdomEtchedUrl (etched URL path)

        Cards without scryfall_id are excluded.
        """
        df_with_flags = CardKingdomTransformer.add_derived_columns(df)

        return (
            df_with_flags.filter(pl.col("scryfall_id").is_not_null())
            .sort("id")
            .group_by("scryfall_id")
            .agg(
                [
                    pl.col("id")
                    .filter(~pl.col("is_foil_bool") & ~pl.col("is_etched"))
                    .last()
                    .cast(pl.String)
                    .alias("cardKingdomId"),
                    pl.col("url").filter(~pl.col("is_foil_bool") & ~pl.col("is_etched")).last().alias("cardKingdomUrl"),
                    pl.col("id")
                    .filter(pl.col("is_foil_bool") & ~pl.col("is_etched"))
                    .last()
                    .cast(pl.String)
                    .alias("cardKingdomFoilId"),
                    pl.col("url")
                    .filter(pl.col("is_foil_bool") & ~pl.col("is_etched"))
                    .last()
                    .alias("cardKingdomFoilUrl"),
                    pl.col("id").filter(pl.col("is_etched")).last().cast(pl.String).alias("cardKingdomEtchedId"),
                    pl.col("url").filter(pl.col("is_etched")).last().alias("cardKingdomEtchedUrl"),
                ]
            )
            .rename({"scryfall_id": "id"})
        )

    @staticmethod
    def to_pricing_df(df: pl.DataFrame) -> pl.DataFrame:
        """
        Extract pricing columns for price processing.

        Output columns:
        - ck_id, scryfall_id, is_foil, is_etched
        - price_retail, price_buy, qty_retail, qty_buying
        """
        df_with_flags = CardKingdomTransformer.add_derived_columns(df)

        return (
            df_with_flags.filter(pl.col("scryfall_id").is_not_null())
            .select(
                [
                    pl.col("id").cast(pl.String).alias("ck_id"),
                    "scryfall_id",
                    pl.col("is_foil_bool").alias("is_foil"),
                    "is_etched",
                    "price_retail",
                    "price_buy",
                    "qty_retail",
                    "qty_buying",
                ]
            )
            .unique(subset=["ck_id"], keep="first")
        )
