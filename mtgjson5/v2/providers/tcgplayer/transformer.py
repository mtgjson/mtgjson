"""TCGPlayer data transformation and normalization."""

import logging

import polars as pl

from .models import (
    CONDITION_MAP,
    ENGLISH,
    FOIL,
    LANGUAGE_MAP,
    NEAR_MINT,
    NON_FOIL,
    PRINTING_MAP,
)

LOGGER = logging.getLogger(__name__)


class TcgPlayerTransformer:
    """
    Transforms raw TCGPlayer API data into normalized DataFrames.

    Responsibilities:
    - API response to DataFrame conversion
    - SKU flattening (nested -> flat rows)
    - SKU map building for MTGJSON joins
    - Filtering by language/condition/printing
    """

    @staticmethod
    def products_to_dataframe(products: list[dict]) -> pl.DataFrame:
        """
        Convert raw API products to DataFrame with nested SKUs.

        Preserves the nested SKU structure for efficient storage.
        """
        rows = []
        for product in products:
            rows.append(
                {
                    "productId": product["productId"],
                    "name": product.get("name", ""),
                    "cleanName": product.get("cleanName", ""),
                    "groupId": product.get("groupId"),
                    "url": product.get("url", ""),
                    "skus": [
                        {
                            "skuId": sku["skuId"],
                            "languageId": sku["languageId"],
                            "printingId": sku["printingId"],
                            "conditionId": sku["conditionId"],
                        }
                        for sku in product.get("skus", [])
                    ],
                }
            )

        return pl.DataFrame(
            rows,
            schema={
                "productId": pl.Int64(),
                "name": pl.String(),
                "cleanName": pl.String(),
                "groupId": pl.Int64(),
                "url": pl.String(),
                "skus": pl.List(
                    pl.Struct(
                        {
                            "skuId": pl.Int64(),
                            "languageId": pl.Int64(),
                            "printingId": pl.Int64(),
                            "conditionId": pl.Int64(),
                        }
                    )
                ),
            },
        )

    @staticmethod
    def flatten_skus(df: pl.DataFrame) -> pl.DataFrame:
        """
        Flatten nested SKUs to one row per SKU.

        Output columns:
        - productId, name, cleanName, groupId, url (from product)
        - skuId, languageId, printingId, conditionId (from SKU)
        - language, printing, condition (human-readable)
        """
        return (
            df.explode("skus")
            .unnest("skus")
            .with_columns(
                [
                    pl.col("languageId")
                    .replace(LANGUAGE_MAP, default="UNKNOWN")
                    .alias("language"),
                    pl.col("printingId")
                    .replace(PRINTING_MAP, default="UNKNOWN")
                    .alias("printing"),
                    pl.col("conditionId")
                    .replace(CONDITION_MAP, default="UNKNOWN")
                    .alias("condition"),
                ]
            )
        )

    @staticmethod
    def filter_skus(
        df: pl.DataFrame,
        language_id: int | None = ENGLISH,
        condition_id: int | None = NEAR_MINT,
        printing_id: int | None = None,
    ) -> pl.DataFrame:
        """
        Filter flattened SKUs by criteria.

        Args:
            language_id: Filter by language (None = all)
            condition_id: Filter by condition (None = all)
            printing_id: Filter by printing (None = all)
        """
        expr = pl.lit(True)

        if language_id is not None:
            expr = expr & (pl.col("languageId") == language_id)
        if condition_id is not None:
            expr = expr & (pl.col("conditionId") == condition_id)
        if printing_id is not None:
            expr = expr & (pl.col("printingId") == printing_id)

        return df.filter(expr)

    @staticmethod
    def build_sku_map(df: pl.DataFrame) -> pl.DataFrame:
        """
        Build SKU map for MTGJSON joins.

        Groups by productId and extracts:
        - tcgplayer_sku_id (non-foil, English, NM)
        - tcgplayer_sku_foil_id (foil, English, NM)

        Output: productId, tcgplayerSkuId, tcgplayerSkuFoilId
        """
        # Flatten if needed
        if "skuId" not in df.columns:
            df = TcgPlayerTransformer.flatten_skus(df)

        # Filter to English, Near Mint
        filtered = TcgPlayerTransformer.filter_skus(
            df,
            language_id=ENGLISH,
            condition_id=NEAR_MINT,
            printing_id=None,  # Get both foil and non-foil
        )

        return filtered.group_by("productId").agg(
            [
                # Non-foil SKU
                pl.col("skuId")
                .filter(pl.col("printingId") == NON_FOIL)
                .first()
                .cast(pl.String)
                .alias("tcgplayerSkuId"),
                # Foil SKU
                pl.col("skuId")
                .filter(pl.col("printingId") == FOIL)
                .first()
                .cast(pl.String)
                .alias("tcgplayerSkuFoilId"),
            ]
        )

    @staticmethod
    def build_product_id_map(
        df: pl.DataFrame,
        tcg_to_uuid: pl.DataFrame,
    ) -> pl.DataFrame:
        """
        Join SKU map to MTGJSON UUIDs.

        Args:
            df: Products DataFrame (with or without flattened SKUs)
            tcg_to_uuid: DataFrame with 'tcgplayerProductId' and 'uuid'

        Returns:
            DataFrame with uuid, tcgplayerSkuId, tcgplayerSkuFoilId
        """
        sku_map = TcgPlayerTransformer.build_sku_map(df)

        return sku_map.join(
            tcg_to_uuid.select(
                [
                    pl.col("tcgplayerProductId").cast(pl.Int64).alias("productId"),
                    "uuid",
                ]
            ),
            on="productId",
            how="inner",
        ).select(["uuid", "tcgplayerSkuId", "tcgplayerSkuFoilId"])

    @staticmethod
    def to_join_data(df: pl.DataFrame) -> pl.DataFrame:
        """
        Transform to format ready for MTGJSON card joins.

        Output columns:
        - tcgplayerProductId (string, for joining)
        - tcgplayerSkuId
        - tcgplayerSkuFoilId
        """
        sku_map = TcgPlayerTransformer.build_sku_map(df)

        return sku_map.rename(
            {
                "productId": "tcgplayerProductId",
            }
        ).with_columns(pl.col("tcgplayerProductId").cast(pl.String))
