"""
Price output writers: JSON streaming, SQL, CSV.
"""

from __future__ import annotations

import contextlib
import datetime
import logging
import sqlite3
from pathlib import Path
from typing import Any, BinaryIO

import orjson
import polars as pl

from mtgjson5.mtgjson_config import MtgjsonConfig

LOGGER = logging.getLogger(__name__)

# SQL column definitions
_PRICE_SQL_COLUMNS = (
    '"uuid" TEXT',
    '"date" TEXT',
    '"source" TEXT',
    '"provider" TEXT',
    '"priceType" TEXT',
    '"finish" TEXT',
    '"price" REAL',
    '"currency" TEXT',
)

_PRICE_MYSQL_COLUMNS = (
    "`uuid` TEXT",
    "`date` TEXT",
    "`source` TEXT",
    "`provider` TEXT",
    "`priceType` TEXT",
    "`finish` TEXT",
    "`price` FLOAT",
    "`currency` TEXT",
)

_PRICE_INDEXES = (
    ("uuid", "uuid"),
    ("date", "date"),
    ("provider", "provider"),
)


def stream_write_all_prices_json(lf: pl.LazyFrame, path: Path, today_date: str) -> None:
    """
    Stream-write AllPrices.json using Prefix Partitioning.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    prefixes = "0123456789abcdef"

    opt_lf = lf.with_columns(
        [
            pl.col("source").cast(pl.Categorical),
            pl.col("provider").cast(pl.Categorical),
            pl.col("price_type").cast(pl.Categorical),
            pl.col("finish").cast(pl.Categorical),
            pl.col("currency").cast(pl.Categorical),
        ]
    )

    with open(path, "wb") as f:
        f.write(b'{"meta":')
        meta = {
            "date": today_date,
            "version": MtgjsonConfig().mtgjson_version,
        }
        f.write(orjson.dumps(meta))
        f.write(b',"data":{')

        total_processed = 0
        first_chunk_written = False

        for prefix in prefixes:
            chunk_lf = opt_lf.filter(pl.col("uuid").str.starts_with(prefix))

            try:
                df_chunk = chunk_lf.collect()
            except Exception as e:
                LOGGER.error(f"Failed to collect chunk {prefix}: {e}")
                continue

            if df_chunk.height == 0:
                del df_chunk
                continue

            # We sort here because we need grouped UUIDs for the iterator
            df_chunk = df_chunk.sort(["uuid", "source", "provider", "price_type", "finish", "date"])

            # Handle comma between chunks
            if first_chunk_written:
                f.write(b",")

            items_written = _process_chunk_to_json(f, df_chunk)

            if items_written > 0:
                first_chunk_written = True
                total_processed += items_written

            # Explicitly release memory before next iteration
            del df_chunk

            LOGGER.info(f"  Processed prefix '{prefix}' (Total: {total_processed:,})")

        f.write(b"}}")

    LOGGER.info(f"Finished streaming AllPrices.json. Total UUIDs: {total_processed:,}")


def _process_chunk_to_json(f: BinaryIO, df: pl.DataFrame) -> int:
    """
    Aggregates a materialized DataFrame and writes to the open file handle.
    Returns number of UUIDs written.
    """
    aggregated = (
        df.group_by(["uuid", "source", "provider", "currency", "price_type", "finish"])
        .agg([pl.col("date"), pl.col("price")])
        .sort("uuid")
    )

    current_uuid = None
    uuid_data: dict[str, dict[str, dict[str, Any]]] = {}
    written_count = 0
    first_in_chunk = True

    rows = aggregated.iter_rows(named=True)

    for row in rows:
        uuid = row["uuid"]

        # Switch to new UUID
        if uuid != current_uuid:
            if current_uuid is not None:
                # Flush previous UUID data
                if not first_in_chunk:
                    f.write(b",")

                f.write(f'"{current_uuid}":'.encode())
                f.write(orjson.dumps(uuid_data))
                first_in_chunk = False
                written_count += 1

            current_uuid = uuid
            uuid_data = {}

        source = row["source"]
        provider = row["provider"]
        currency = row["currency"]
        p_type = row["price_type"]
        finish = row["finish"]

        date_prices = dict(zip(row["date"], row["price"], strict=False))

        if source not in uuid_data:
            uuid_data[source] = {}
        if provider not in uuid_data[source]:
            uuid_data[source][provider] = {
                "buylist": {},
                "retail": {},
                "currency": currency,
            }

        if p_type in uuid_data[source][provider]:
            uuid_data[source][provider][p_type][finish] = date_prices

    # Flush the final UUID of the chunk
    if current_uuid is not None:
        if not first_in_chunk:
            f.write(b",")
        f.write(f'"{current_uuid}":'.encode())
        f.write(orjson.dumps(uuid_data))
        written_count += 1

    return written_count


def stream_write_today_prices_json(df: pl.DataFrame, path: Path, today_date: str) -> None:
    """
    Stream-write AllPricesToday.json for today's prices only.

    Args:
        df: DataFrame with today's price data
        path: Output path for AllPricesToday.json
        today_date: Date string in YYYY-MM-DD format
    """
    stream_write_all_prices_json(df.lazy(), path, today_date)


def _prepare_price_df_for_sql(df: pl.DataFrame) -> pl.DataFrame:
    """Rename snake_case columns to camelCase for SQL output."""
    renames = {c: c for c in df.columns}
    if "price_type" in df.columns:
        renames["price_type"] = "priceType"
    return df.rename({k: v for k, v in renames.items() if k != v})


def write_prices_sqlite(df: pl.DataFrame, path: Path) -> None:
    """Write price data to a SQLite binary database.

    Creates a ``prices`` table and a ``meta`` table with indexes
    on uuid, date, and provider.
    """
    from mtgjson5.models.containers import MtgjsonMeta

    prepared = _prepare_price_df_for_sql(df)

    if path.exists():
        path.unlink()

    conn = sqlite3.connect(str(path))
    cursor = conn.cursor()

    cols = ", ".join(_PRICE_SQL_COLUMNS)
    cursor.execute(f'CREATE TABLE "prices" ({cols})')

    placeholders = ", ".join(["?" for _ in prepared.columns])
    col_names = ", ".join([f'"{c}"' for c in prepared.columns])

    batch_size = 10_000
    rows = prepared.rows()
    for i in range(0, len(rows), batch_size):
        cursor.executemany(
            f'INSERT INTO "prices" ({col_names}) VALUES ({placeholders})',
            rows[i : i + batch_size],
        )

    for idx_name, col in _PRICE_INDEXES:
        with contextlib.suppress(Exception):
            cursor.execute(f'CREATE INDEX "idx_prices_{idx_name}" ON "prices" ("{col}")')

    meta = MtgjsonMeta()
    cursor.execute('CREATE TABLE "meta" ("date" TEXT, "version" TEXT)')
    cursor.execute('INSERT INTO "meta" VALUES (?, ?)', (meta.date, meta.version))

    conn.commit()
    conn.close()
    LOGGER.info(f"Wrote {path.name} ({len(prepared):,} rows)")


def write_prices_sql(df: pl.DataFrame, path: Path) -> None:
    """Write price data as a MySQL text dump with INSERT statements."""
    from mtgjson5.models.containers import MtgjsonMeta

    from .serializers import escape_mysql

    prepared = _prepare_price_df_for_sql(df)
    meta = MtgjsonMeta()

    with open(path, "w", encoding="utf-8") as f:
        f.write(f"-- MTGJSON Price SQL Dump\n-- Generated: {datetime.date.today().isoformat()}\n")
        f.write("SET names 'utf8mb4';\n")
        f.write("START TRANSACTION;\n\n")

        cols = ",\n    ".join(["`id` INTEGER PRIMARY KEY AUTO_INCREMENT", *_PRICE_MYSQL_COLUMNS])
        f.write("DROP TABLE IF EXISTS `prices`;\n")
        f.write(f"CREATE TABLE `prices` (\n    {cols}\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n\n")

        col_names = ", ".join([f"`{c}`" for c in prepared.columns])
        for row in prepared.rows():
            values = ", ".join(escape_mysql(v) for v in row)
            f.write(f"INSERT INTO `prices` ({col_names}) VALUES ({values});\n")

        for idx_name, col in _PRICE_INDEXES:
            f.write(f"CREATE INDEX `idx_prices_{idx_name}` ON `prices` (`{col}`);\n")

        f.write("\n")
        f.write(
            "DROP TABLE IF EXISTS `meta`;\n"
            "CREATE TABLE `meta` (\n"
            "    `id` INTEGER PRIMARY KEY AUTO_INCREMENT,\n"
            "    `date` TEXT,\n"
            "    `version` TEXT\n"
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n"
        )
        f.write(
            f"INSERT INTO `meta` (`date`, `version`) VALUES ({escape_mysql(meta.date)}, {escape_mysql(meta.version)});\n"
        )

        f.write("\nCOMMIT;\n")

    LOGGER.info(f"Wrote {path.name} ({len(prepared):,} rows)")


def write_prices_psql(df: pl.DataFrame, path: Path) -> None:
    """Write price data as a PostgreSQL dump with COPY format."""
    from .serializers import escape_postgres

    prepared = _prepare_price_df_for_sql(df)

    with open(path, "w", encoding="utf-8") as f:
        f.write(f"-- MTGJSON Price PostgreSQL Dump\n-- Generated: {datetime.date.today().isoformat()}\n")
        f.write("BEGIN;\n\n")

        cols = ",\n    ".join(_PRICE_SQL_COLUMNS)
        f.write(f'CREATE TABLE IF NOT EXISTS "prices" (\n    {cols}\n);\n\n')

        col_names = ", ".join([f'"{c}"' for c in prepared.columns])
        f.write(f'COPY "prices" ({col_names}) FROM stdin;\n')

        for row in prepared.rows():
            escaped = [escape_postgres(v) for v in row]
            f.write("\t".join(escaped) + "\n")

        f.write("\\.\n\n")

        for idx_name, col in _PRICE_INDEXES:
            f.write(f'CREATE INDEX IF NOT EXISTS "idx_prices_{idx_name}" ON "prices" ("{col}");\n')

        f.write("\nCOMMIT;\n")

    LOGGER.info(f"Wrote {path.name} ({len(prepared):,} rows)")


def write_prices_csv(df: pl.DataFrame, path: Path) -> None:
    """Write price data as CSV matching v1 cardPrices.csv format.

    Renames columns to match legacy format:
    - finish -> cardFinish
    - source -> gameAvailability
    - provider -> priceProvider
    - price_type -> providerListing
    """
    # Rename columns to match v1 format
    column_renames = {
        "finish": "cardFinish",
        "source": "gameAvailability",
        "provider": "priceProvider",
        "price_type": "providerListing",
    }

    renamed = df.rename({k: v for k, v in column_renames.items() if k in df.columns})

    # Reorder columns to match v1 format
    v1_column_order = [
        "cardFinish",
        "currency",
        "date",
        "gameAvailability",
        "price",
        "priceProvider",
        "providerListing",
        "uuid",
    ]

    # Select columns in order, only including those that exist
    available_cols = [c for c in v1_column_order if c in renamed.columns]
    ordered = renamed.select(available_cols)

    ordered.write_csv(path)
    LOGGER.info(f"Wrote {path.name} ({len(ordered):,} rows)")
