"""Rebuild a TCGPlayer catalog from MTGJSON's last published TcgplayerSkus.json.

The nightly build runs in a fresh container, so the previous run's
``tcg_skus.parquet`` is never on disk when a fetch fails. The last published
TcgplayerSkus.json is the one copy of yesterday's catalog that the build machine
can actually reach, so it doubles as the fallback catalog and as the baseline for
the day-over-day regression check.

What comes back is a partial catalog: TcgplayerSkus.json carries productIds and
SKUs, but not product names, group IDs, or URLs. Every SKU still lands in
TcgplayerSkus.json; alternative-foil detection, which matches on product names,
finds nothing and is skipped for that build.
"""

from __future__ import annotations

import logging
import lzma
from typing import IO, cast

import ijson
import polars as pl
import requests

from .models import CONDITION_MAP, LANGUAGE_MAP, PRINTING_MAP, PRODUCT_SCHEMA

LOGGER = logging.getLogger(__name__)

PUBLISHED_SKUS_URL = "https://mtgjson.com/api/v5/TcgplayerSkus.json.xz"
DOWNLOAD_TIMEOUT = 300
# Rows are handed to polars in batches so the whole catalog is never held as
# Python ints; the published file carries roughly five million SKUs.
BATCH_ROWS = 500_000
# A catalog this small is a truncated or misrouted download, not yesterday's data.
MIN_PUBLISHED_PRODUCTS = 1_000

_SKU_FIELDS = ["skuId", "languageId", "printingId", "conditionId"]
_LANGUAGE_IDS = {name: value for value, name in LANGUAGE_MAP.items()}
_PRINTING_IDS = {name.replace("_", " "): value for value, name in PRINTING_MAP.items()}
_CONDITION_IDS = {name: value for value, name in CONDITION_MAP.items()}


class PublishedCatalogError(RuntimeError):
    """Raised when the last published catalog could not be rebuilt."""


def download_published_catalog(url: str = PUBLISHED_SKUS_URL, timeout: int = DOWNLOAD_TIMEOUT) -> pl.DataFrame:
    """Download the last published TcgplayerSkus.json and rebuild it as a catalog."""
    LOGGER.info(f"Downloading the last published TCGPlayer catalog from {url}")
    with requests.get(
        url,
        stream=True,
        timeout=timeout,
        headers={"User-Agent": "MTGJSON/5.0 (https://mtgjson.com)"},
    ) as response:
        response.raise_for_status()
        response.raw.decode_content = True

        with lzma.open(cast("IO[bytes]", response.raw), "rb") as stream:
            return catalog_from_skus_json(stream)


def catalog_from_skus_json(stream: IO[bytes]) -> pl.DataFrame:
    """Rebuild a products frame from a TcgplayerSkus.json byte stream.

    The published file is keyed by MTGJSON UUID, so the same product shows up
    once per card it maps to. Products are collected the first time they appear
    and their repeats skipped, which keeps the row count near the SKU count
    rather than the (much larger) UUID-times-SKU count.
    """
    seen_products: set[int] = set()
    batches: list[pl.DataFrame] = []
    rows: dict[str, list[int]] = _empty_rows()
    unmapped = 0

    for _uuid, skus in ijson.kvitems(stream, "data"):
        if not isinstance(skus, list):
            continue

        fresh = {sku["productId"] for sku in skus if isinstance(sku, dict) and "productId" in sku} - seen_products
        if not fresh:
            continue

        for sku in skus:
            if not isinstance(sku, dict) or sku.get("productId") not in fresh:
                continue

            language = _LANGUAGE_IDS.get(sku.get("language", ""))
            printing = _PRINTING_IDS.get(sku.get("printing", ""))
            condition = _CONDITION_IDS.get(sku.get("condition", ""))
            if language is None or printing is None or condition is None or sku.get("skuId") is None:
                unmapped += 1
                continue

            rows["productId"].append(sku["productId"])
            rows["skuId"].append(sku["skuId"])
            rows["languageId"].append(language)
            rows["printingId"].append(printing)
            rows["conditionId"].append(condition)

        seen_products |= fresh

        if len(rows["skuId"]) >= BATCH_ROWS:
            batches.append(_to_frame(rows))
            rows = _empty_rows()

    if rows["skuId"]:
        batches.append(_to_frame(rows))

    if unmapped:
        LOGGER.warning(f"Skipped {unmapped:,} published SKUs with an unrecognized condition, language, or printing")

    if not batches:
        raise PublishedCatalogError("The published TcgplayerSkus.json held no usable SKUs")

    catalog = _group_into_products(pl.concat(batches, rechunk=False))
    if catalog.height < MIN_PUBLISHED_PRODUCTS:
        raise PublishedCatalogError(
            f"The published TCGPlayer catalog held only {catalog.height:,} products; refusing to trust it"
        )

    LOGGER.info(
        f"Rebuilt {catalog.height:,} products and {catalog['skus'].list.len().sum():,} SKUs from the published catalog"
    )
    return catalog


def _empty_rows() -> dict[str, list[int]]:
    return {"productId": [], "skuId": [], "languageId": [], "printingId": [], "conditionId": []}


def _to_frame(rows: dict[str, list[int]]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=dict.fromkeys(rows, pl.Int64))


def _group_into_products(flat: pl.DataFrame) -> pl.DataFrame:
    """Fold flat SKU rows into one row per product, matching PRODUCT_SCHEMA.

    Names, group IDs, and URLs are not published, so they come back empty. Every
    consumer of those columns treats a blank name as "nothing to match on".
    """
    return (
        flat.unique(subset=["productId", "skuId"], keep="first")
        .sort(["productId", "skuId"])
        .group_by("productId", maintain_order=True)
        .agg(pl.struct(_SKU_FIELDS).alias("skus"))
        .with_columns(
            pl.lit("").alias("name"),
            pl.lit("").alias("cleanName"),
            pl.lit(None, dtype=pl.Int64).alias("groupId"),
            pl.lit("").alias("url"),
        )
        .select(list(PRODUCT_SCHEMA))
        .cast(cast("dict", PRODUCT_SCHEMA))
    )
