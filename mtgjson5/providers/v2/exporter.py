"""Export utilities for MTGJSON data."""

import json
import logging
import sqlite3
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from pathlib import Path
from typing import Any, Literal

import orjson
import polars as pl

from mtgjson5 import constants
from mtgjson5.cache import GLOBAL_CACHE
from mtgjson5.classes import MtgjsonMetaObject
from mtgjson5.constants import TOKEN_LAYOUTS
from mtgjson5.mtgjson_models import clean_nested, dataframe_to_cards_list


LOGGER = logging.getLogger(__name__)


def read_hive_partitioned(
    parquet_dir: Path | str,
    set_codes: list[str] | None = None,
) -> pl.LazyFrame:
    """Read hive-partitioned parquet data."""
    parquet_dir = Path(parquet_dir)

    if set_codes:
        paths: list[Path] = []
        for code in set_codes:
            set_path = parquet_dir / f"setCode={code}"
            if set_path.exists():
                paths.extend(set_path.glob("*.parquet"))
        if not paths:
            raise FileNotFoundError(f"No parquet files found for sets: {set_codes}")
        return pl.scan_parquet(paths, hive_partitioning=True)
    return pl.scan_parquet(parquet_dir / "**/*.parquet", hive_partitioning=True)


class TableType(Enum):
    """Enumeration of normalized table types for export."""

    CARDS = auto()
    TOKENS = auto()
    SETS = auto()
    IDENTIFIERS = auto()
    TOKEN_IDENTIFIERS = auto()
    LEGALITIES = auto()
    RULINGS = auto()
    FOREIGN_DATA = auto()
    PURCHASE_URLS = auto()
    META = auto()
    SET_TRANSLATIONS = auto()


@dataclass
class TableSchema:
    """Definition for a normalized table."""

    name: str
    schema: dict[str, pl.DataType]

    def empty(self) -> pl.DataFrame:
        """Create an empty DataFrame with this schema."""
        return pl.DataFrame(schema=self.schema)


# Canonical schemas for all tables
SCHEMAS = {
    TableType.IDENTIFIERS: TableSchema(
        "cardIdentifiers",
        {
            "uuid": pl.String(),
            "setCode": pl.String(),
            "scryfallId": pl.String(),
            "mtgjsonV4Id": pl.String(),
            "multiverseId": pl.String(),
        },
    ),
    TableType.LEGALITIES: TableSchema(
        "cardLegalities",
        {
            "uuid": pl.String(),
            "setCode": pl.String(),
            "format": pl.String(),
            "status": pl.String(),
        },
    ),
    TableType.RULINGS: TableSchema(
        "cardRulings",
        {
            "uuid": pl.String(),
            "setCode": pl.String(),
            "date": pl.String(),
            "text": pl.String(),
        },
    ),
    TableType.FOREIGN_DATA: TableSchema(
        "cardForeignData",
        {
            "uuid": pl.String(),
            "setCode": pl.String(),
            "language": pl.String(),
            "name": pl.String(),
            "text": pl.String(),
            "type": pl.String(),
        },
    ),
    TableType.PURCHASE_URLS: TableSchema(
        "cardPurchaseUrls",
        {
            "uuid": pl.String(),
            "setCode": pl.String(),
        },
    ),
    TableType.SET_TRANSLATIONS: TableSchema(
        "setTranslations",
        {
            "setCode": pl.String(),
            "language": pl.String(),
            "translation": pl.String(),
        },
    ),
}


def extract_struct_table(
    df: pl.DataFrame,
    struct_col: str,
    key_cols: list[str] | None = None,
    exclude_fields: set[str] | None = None,
) -> pl.DataFrame:
    """Generic extraction of struct column to flat table."""
    if struct_col not in df.columns:
        return pl.DataFrame()

    key_cols = key_cols or ["uuid", "setCode"]
    exclude_fields = exclude_fields or set()

    dtype = df.schema[struct_col]
    if not isinstance(dtype, pl.Struct):
        return pl.DataFrame()

    field_names = [f.name for f in dtype.fields if f.name not in exclude_fields]

    return df.select(
        [pl.col(c) for c in key_cols if c in df.columns]
        + [pl.col(struct_col).struct.field(f).alias(f) for f in field_names]
    )


def extract_list_struct_table(
    df: pl.DataFrame,
    list_col: str,
    key_cols: list[str] | None = None,
    exclude_fields: set[str] | None = None,
) -> pl.DataFrame:
    """Generic extraction of list[struct] column to normalized table."""
    if list_col not in df.columns:
        return pl.DataFrame()

    key_cols = key_cols or ["uuid", "setCode"]
    exclude_fields = exclude_fields or set()

    with_data = df.filter(
        pl.col(list_col).is_not_null() & (pl.col(list_col).list.len() > 0)
    )

    if len(with_data) == 0:
        return pl.DataFrame()

    dtype = df.schema[list_col]
    if not isinstance(dtype, pl.List):
        return pl.DataFrame()
    inner = dtype.inner
    if not isinstance(inner, pl.Struct):
        return pl.DataFrame()

    field_names = [f.name for f in inner.fields if f.name not in exclude_fields]

    return (
        with_data.select([c for c in key_cols if c in df.columns] + [list_col])
        .explode(list_col)
        .with_columns([pl.col(list_col).struct.field(f).alias(f) for f in field_names])
        .drop(list_col)
    )


def extract_legalities_unpivot(df: pl.DataFrame) -> pl.DataFrame:
    """Extract legalities struct to long format via unpivot."""
    if "legalities" not in df.columns:
        return SCHEMAS[TableType.LEGALITIES].empty()

    dtype = df.schema["legalities"]
    if not isinstance(dtype, pl.Struct):
        return SCHEMAS[TableType.LEGALITIES].empty()

    formats = [f.name for f in dtype.fields]

    with_formats = df.select(
        ["uuid", "setCode"]
        + [pl.col("legalities").struct.field(fmt).alias(fmt) for fmt in formats]
    )

    result = with_formats.unpivot(
        index=["uuid", "setCode"],
        on=formats,
        variable_name="format",
        value_name="status",
    ).filter(pl.col("status").is_not_null())

    return result if len(result) > 0 else SCHEMAS[TableType.LEGALITIES].empty()


def _to_json_batch(series: pl.Series) -> pl.Series:
    """Batch convert Series to JSON strings using orjson (3-10x faster than json)."""
    return pl.Series(
        [
            orjson.dumps(x).decode("utf-8") if x is not None else None
            for x in series.to_list()
        ],
        dtype=pl.String,
    )


def flatten_cards_table(
    df: pl.DataFrame,
    exclude_cols: set[str] | None = None,
) -> pl.DataFrame:
    """Flatten cards DataFrame for SQL tables."""
    exclude_cols = exclude_cols or {
        "identifiers",
        "legalities",
        "rulings",
        "foreignData",
    }
    result = df.select([c for c in df.columns if c not in exclude_cols])

    json_exprs = []
    simple_cols = []

    for col_name in result.columns:
        dtype = result.schema[col_name]
        if isinstance(dtype, pl.Struct):
            json_exprs.append(pl.col(col_name).struct.json_encode().alias(col_name))
        elif isinstance(dtype, pl.List):
            json_exprs.append(
                pl.col(col_name)
                .map_batches(_to_json_batch, return_dtype=pl.String)
                .alias(col_name)
            )
        else:
            simple_cols.append(col_name)

    if json_exprs:
        result = result.select([pl.col(c) for c in simple_cols] + json_exprs)

    return result


def serialize_complex_types(df: pl.DataFrame) -> pl.DataFrame:
    """Convert List and Struct columns to JSON strings.

    (vectorized in Rust) and orjson for List columns. This avoids Python loops
    and is 2-3x faster than the previous approach.
    """
    schema = df.schema
    struct_cols = [c for c in df.columns if isinstance(schema[c], pl.Struct)]
    list_cols = [c for c in df.columns if isinstance(schema[c], pl.List)]

    if not struct_cols and not list_cols:
        return df

    result = df

    # Struct columns: use native Polars json_encode
    if struct_cols:
        struct_exprs = [
            pl.col(col_name).struct.json_encode().alias(col_name)
            for col_name in struct_cols
        ]
        result = result.with_columns(struct_exprs)

    # List columns: use orjson via map_batches for efficient batch processing
    if list_cols:
        for col_name in list_cols:
            result = result.with_columns(
                pl.col(col_name)
                .map_batches(_to_json_batch, return_dtype=pl.String)
                .alias(col_name)
            )

    return result


@dataclass
class TableBuilder:
    """Builds all normalized tables from card data."""

    cards_df: pl.DataFrame
    sets_metadata: dict[str, dict] | None = None
    rulings_df: pl.DataFrame | None = None
    foreign_data_df: pl.DataFrame | None = None
    uuid_to_oracle_df: pl.DataFrame | None = None
    boosters_df: pl.DataFrame | None = None

    _tables: dict[str, pl.DataFrame] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        self._non_tokens = self.cards_df.filter(pl.col("layout") != "token")
        self._tokens = self.cards_df.filter(pl.col("layout") == "token")

    @contextmanager
    def _timed(self, label: str) -> Iterator[None]:
        """Context manager for timing operations."""
        start = time.perf_counter()
        try:
            yield
        finally:
            elapsed = time.perf_counter() - start
            LOGGER.debug(f"{label}: {elapsed:.2f}s")

    def build_all(self) -> dict[str, pl.DataFrame]:
        """Build all tables and return as dict."""
        return {
            "cards": self.get_cards(),
            "tokens": self.get_tokens(),
            "sets": self.get_sets(),
            "cardIdentifiers": self.get_identifiers(),
            "tokenIdentifiers": self.get_token_identifiers(),
            "cardLegalities": self.get_legalities(),
            "cardRulings": self.get_rulings(),
            "cardForeignData": self.get_foreign_data(),
            "cardPurchaseUrls": self.get_purchase_urls(),
            "meta": self.get_meta(),
        }

    def get_cards(self) -> pl.DataFrame:
        """Get flattened cards table."""
        return self._cached("cards", lambda: flatten_cards_table(self._non_tokens))

    def get_tokens(self) -> pl.DataFrame:
        """Get flattened tokens table."""
        return self._cached("tokens", lambda: flatten_cards_table(self._tokens))

    def get_sets(self) -> pl.DataFrame:
        """Get sets metadata table."""
        return self._cached("sets", self._build_sets)

    def get_identifiers(self) -> pl.DataFrame:
        """Get card identifiers table."""
        return self._cached(
            "cardIdentifiers",
            lambda: extract_struct_table(self._non_tokens, "identifiers"),
        )

    def get_token_identifiers(self) -> pl.DataFrame:
        """Get token identifiers table."""
        return self._cached(
            "tokenIdentifiers",
            lambda: extract_struct_table(self._tokens, "identifiers"),
        )

    def get_legalities(self) -> pl.DataFrame:
        """Get card legalities table."""
        return self._cached(
            "cardLegalities", lambda: extract_legalities_unpivot(self.cards_df)
        )

    def get_rulings(self) -> pl.DataFrame:
        """Get card rulings table."""
        return self._cached("cardRulings", self._build_rulings)

    def get_foreign_data(self) -> pl.DataFrame:
        """Get card foreign data table."""
        return self._cached("cardForeignData", self._build_foreign_data)

    def get_purchase_urls(self) -> pl.DataFrame:
        """Get card purchase URLs table."""
        return self._cached(
            "cardPurchaseUrls",
            lambda: extract_struct_table(self.cards_df, "purchaseUrls"),
        )

    def get_meta(self) -> pl.DataFrame:
        """Get metadata table with version and date."""
        return pl.DataFrame(
            {
                "date": [int(datetime.now().strftime("%Y%m%d"))],
                "version": ["5.2.1+polars"],
            }
        ).cast({"date": pl.Int64, "version": pl.String})

    def get_set_translations(self) -> pl.DataFrame:
        """Get set translations table."""
        if not self.sets_metadata:
            return SCHEMAS[TableType.SET_TRANSLATIONS].empty()

        records = []
        for code, meta in self.sets_metadata.items():
            translations = meta.get("translations", {})
            for lang, name in translations.items():
                if name:
                    records.append(
                        {"setCode": code, "language": lang, "translation": name}
                    )

        return (
            pl.DataFrame(records)
            if records
            else SCHEMAS[TableType.SET_TRANSLATIONS].empty()
        )

    def get_booster_tables(self) -> dict[str, pl.DataFrame]:
        """Build normalized booster tables."""
        if self.boosters_df is None:
            return self._empty_booster_tables()

        df = (
            self.boosters_df.collect()
            if isinstance(self.boosters_df, pl.LazyFrame)
            else self.boosters_df
        )

        content_weights, contents, sheets, sheet_cards = [], [], [], []

        for row in df.iter_rows(named=True):
            set_code, config_raw = row["setCode"], row["config"]
            if config_raw is None:
                continue

            config = (
                json.loads(config_raw) if isinstance(config_raw, str) else config_raw
            )

            for booster_name, booster_config in config.items():
                if not isinstance(booster_config, dict):
                    continue

                for idx, booster in enumerate(booster_config.get("boosters", [])):
                    content_weights.append(
                        {
                            "setCode": set_code,
                            "boosterName": booster_name,
                            "boosterIndex": idx,
                            "boosterWeight": booster.get("weight", 1),
                        }
                    )
                    for sheet_name, picks in booster.get("contents", {}).items():
                        contents.append(
                            {
                                "setCode": set_code,
                                "boosterName": booster_name,
                                "boosterIndex": idx,
                                "sheetName": sheet_name,
                                "sheetPicks": picks,
                            }
                        )

                for sheet_name, sheet_config in booster_config.get(
                    "sheets", {}
                ).items():
                    if not isinstance(sheet_config, dict):
                        continue
                    sheets.append(
                        {
                            "setCode": set_code,
                            "boosterName": booster_name,
                            "sheetName": sheet_name,
                            "sheetIsFoil": sheet_config.get("foil", False),
                            "sheetHasBalanceColors": sheet_config.get(
                                "balanceColors", False
                            ),
                        }
                    )
                    for card_uuid, weight in sheet_config.get("cards", {}).items():
                        sheet_cards.append(
                            {
                                "setCode": set_code,
                                "boosterName": booster_name,
                                "sheetName": sheet_name,
                                "cardUuid": card_uuid,
                                "cardWeight": weight,
                            }
                        )

        return {
            "setBoosterContentWeights": (
                pl.DataFrame(content_weights)
                if content_weights
                else self._empty_booster_tables()["setBoosterContentWeights"]
            ),
            "setBoosterContents": (
                pl.DataFrame(contents)
                if contents
                else self._empty_booster_tables()["setBoosterContents"]
            ),
            "setBoosterSheets": (
                pl.DataFrame(sheets)
                if sheets
                else self._empty_booster_tables()["setBoosterSheets"]
            ),
            "setBoosterSheetCards": (
                pl.DataFrame(sheet_cards)
                if sheet_cards
                else self._empty_booster_tables()["setBoosterSheetCards"]
            ),
        }

    def _cached(self, key: str, builder: Callable[[], pl.DataFrame]) -> pl.DataFrame:
        """Cache and return table built by builder function."""
        if key not in self._tables:
            with self._timed(f"build_{key}"):
                self._tables[key] = builder()
        return self._tables[key]

    def _build_sets(self) -> pl.DataFrame:
        """Build sets table with metadata and card counts."""
        set_stats = self.cards_df.group_by("setCode").agg(pl.len().alias("cardCount"))

        if not self.sets_metadata:
            return set_stats.rename({"setCode": "code"})

        records = []
        for code in set_stats["setCode"].to_list():
            meta = self.sets_metadata.get(code, {})
            records.append(
                {
                    "code": code,
                    "name": meta.get("name", code),
                    "type": meta.get("type"),
                    "releaseDate": meta.get("releaseDate"),
                    "baseSetSize": meta.get("baseSetSize"),
                    "totalSetSize": meta.get("totalSetSize"),
                    "block": meta.get("block"),
                    "isFoilOnly": meta.get("isFoilOnly", False),
                    "isOnlineOnly": meta.get("isOnlineOnly", False),
                    "keyruneCode": meta.get("keyruneCode"),
                }
            )

        return pl.DataFrame(records).join(
            set_stats.rename({"setCode": "code"}), on="code", how="left"
        )

    def _build_rulings(self) -> pl.DataFrame:
        """Build card rulings table from oracle data."""
        if self.rulings_df is not None and self.uuid_to_oracle_df is not None:
            card_uuids = self.cards_df.select("uuid").unique()
            filtered_uuid_oracle = self.uuid_to_oracle_df.join(
                card_uuids, on="uuid", how="inner"
            )

            oracle_col = (
                "oracleId" if "oracleId" in self.rulings_df.columns else "oracle_id"
            )
            joined = filtered_uuid_oracle.join(
                self.rulings_df, left_on="oracleId", right_on=oracle_col, how="inner"
            )
            if len(joined) > 0:
                return (
                    joined.explode("rulings")
                    .with_columns(
                        [
                            pl.col("rulings").struct.field("date").alias("date"),
                            pl.col("rulings").struct.field("text").alias("text"),
                        ]
                    )
                    .select(["uuid", "setCode", "date", "text"])
                )

        return extract_list_struct_table(self.cards_df, "rulings")

    def _build_foreign_data(self) -> pl.DataFrame:
        """Build foreign data table from provider data."""
        if self.foreign_data_df is not None and len(self.foreign_data_df) > 0:
            uuid_key = self.cards_df.select(["uuid", "setCode", "number"]).unique()
            set_col = (
                "setCode" if "setCode" in self.foreign_data_df.columns else "set_code"
            )
            num_col = (
                "collectorNumber"
                if "collectorNumber" in self.foreign_data_df.columns
                else "collector_number"
            )

            joined = uuid_key.join(
                self.foreign_data_df,
                left_on=["setCode", "number"],
                right_on=[set_col, num_col],
                how="inner",
            )
            if len(joined) > 0:
                fd_col = (
                    "foreignData" if "foreignData" in joined.columns else "foreign_data"
                )
                return extract_list_struct_table(
                    joined.rename({fd_col: "foreignData"}), "foreignData"
                )

        return extract_list_struct_table(
            self.cards_df, "foreignData", exclude_fields={"identifiers"}
        )

    @staticmethod
    def _empty_booster_tables() -> dict[str, pl.DataFrame]:
        """Return empty booster tables with correct schemas."""
        return {
            "setBoosterContentWeights": pl.DataFrame(
                schema={
                    "setCode": pl.String(),
                    "boosterName": pl.String(),
                    "boosterIndex": pl.Int64(),
                    "boosterWeight": pl.Int64(),
                }
            ),
            "setBoosterContents": pl.DataFrame(
                schema={
                    "setCode": pl.String(),
                    "boosterName": pl.String(),
                    "boosterIndex": pl.Int64(),
                    "sheetName": pl.String(),
                    "sheetPicks": pl.Int64(),
                }
            ),
            "setBoosterSheets": pl.DataFrame(
                schema={
                    "setCode": pl.String(),
                    "boosterName": pl.String(),
                    "sheetName": pl.String(),
                    "sheetIsFoil": pl.Boolean(),
                    "sheetHasBalanceColors": pl.Boolean(),
                }
            ),
            "setBoosterSheetCards": pl.DataFrame(
                schema={
                    "setCode": pl.String(),
                    "boosterName": pl.String(),
                    "sheetName": pl.String(),
                    "cardUuid": pl.String(),
                    "cardWeight": pl.Int64(),
                }
            ),
        }


class ExportFormat(Enum):
    """Supported export formats for MTGJSON data."""

    PARQUET = "parquet"
    CSV = "csv"
    SQLITE = "sqlite"
    POSTGRESQL = "postgresql"


@dataclass
class ExportConfig:
    """Configuration for export operations."""

    format: ExportFormat
    compression: Literal["zstd"] = "zstd"
    compression_level: int = 9
    include_booster_tables: bool = True
    include_translations: bool = True
    enable_profiling: bool = False


def _escape_postgres(value: Any) -> str:
    """Escape value for PostgreSQL COPY format."""
    if value is None:
        return "\\N"
    if isinstance(value, bool):
        return "t" if value else "f"
    if isinstance(value, (int, float)):
        return str(value)
    s = json.dumps(value) if isinstance(value, (list, dict)) else str(value)
    return (
        s.replace("\\", "\\\\")
        .replace("\t", "\\t")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )


def _batched(iterable: Any, n: int) -> Iterator[list[Any]]:
    """Yield batches of n items."""
    batch: list[Any] = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= n:
            yield batch
            batch = []
    if batch:
        yield batch


def export_tables(
    parquet_dir: Path | str,
    output_path: Path | str,
    config: ExportConfig,
    sets_metadata: dict[str, dict] | None = None,
    boosters_df: pl.DataFrame | None = None,
    rulings_df: pl.DataFrame | None = None,
    foreign_data_df: pl.DataFrame | None = None,
    uuid_to_oracle_df: pl.DataFrame | None = None,
    cards_df: pl.DataFrame | None = None,
) -> dict[str, Path] | Path:
    """Unified export function for all formats."""
    parquet_dir = Path(parquet_dir)
    output_path = Path(output_path)

    try:
        cards_df = read_hive_partitioned(parquet_dir).collect()
        LOGGER.info(f"Loaded {len(cards_df):,} cards")

        builder = TableBuilder(
            cards_df=cards_df,
            sets_metadata=sets_metadata,
            rulings_df=rulings_df,
            foreign_data_df=foreign_data_df,
            uuid_to_oracle_df=uuid_to_oracle_df,
            boosters_df=boosters_df,
        )

        tables = {}
        tables["cards"] = builder.get_cards()
        tables["sets"] = builder.get_sets()
        tables["cardIdentifiers"] = builder.get_identifiers()
        tables["cardLegalities"] = builder.get_legalities()
        tables["meta"] = builder.get_meta()

        for name, df in [
            ("tokens", builder.get_tokens()),
            ("tokenIdentifiers", builder.get_token_identifiers()),
            ("cardRulings", builder.get_rulings()),
            ("cardForeignData", builder.get_foreign_data()),
            ("cardPurchaseUrls", builder.get_purchase_urls()),
        ]:
            if len(df) > 0:
                tables[name] = df

        if config.include_translations:
            trans = builder.get_set_translations()
            if len(trans) > 0:
                tables["setTranslations"] = trans

        if config.include_booster_tables and boosters_df is not None:
            for name, df in builder.get_booster_tables().items():
                if len(df) > 0:
                    tables[name] = df

        # Export
        result: dict[str, Path] | Path
        if config.format == ExportFormat.PARQUET:
            result = _export_parquet(tables, output_path, config)
        elif config.format == ExportFormat.CSV:
            result = _export_csv(tables, output_path)
        elif config.format == ExportFormat.SQLITE:
            result = _export_sqlite(tables, output_path)
        elif config.format == ExportFormat.POSTGRESQL:
            result = _export_postgresql(tables, output_path)
        else:
            raise ValueError(f"Unknown format: {config.format}")

        return result
    finally:
        GLOBAL_CACHE.clear()


def _export_parquet(
    tables: dict[str, pl.DataFrame],
    output_dir: Path,
    config: ExportConfig,
) -> dict[str, Path]:
    """Export tables to individual parquet files."""
    if output_dir.exists() and output_dir.is_file():
        output_dir.unlink()
    output_dir.mkdir(parents=True, exist_ok=True)
    outputs = {}

    for name, df in tables.items():
        path = output_dir / f"{name}.parquet"
        df.write_parquet(
            path,
            compression=config.compression,
            compression_level=config.compression_level,
        )
        outputs[f"{name}.parquet"] = path
        LOGGER.info(f"  {name}.parquet: {len(df):,} rows")

    return outputs


def _export_csv(tables: dict[str, pl.DataFrame], output_dir: Path) -> dict[str, Path]:
    """Export tables to individual CSV files."""
    output_dir.mkdir(parents=True, exist_ok=True)
    outputs = {}

    for name, df in tables.items():
        path = output_dir / f"{name}.csv"
        df.write_csv(path)
        outputs[f"{name}.csv"] = path
        LOGGER.info(f"  {name}.csv: {len(df):,} rows")

    return outputs


def _export_sqlite(tables: dict[str, pl.DataFrame], output_path: Path) -> Path:
    """Export tables to SQLite database."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        output_path.unlink()

    conn = sqlite3.connect(str(output_path))
    cursor = conn.cursor()

    for name, df in tables.items():
        if len(df) == 0:
            continue

        serialized = serialize_complex_types(df)

        cols = ", ".join([f'"{c}" TEXT' for c in serialized.columns])
        cursor.execute(f'CREATE TABLE "{name}" ({cols})')

        placeholders = ", ".join(["?" for _ in serialized.columns])
        col_names = ", ".join([f'"{c}"' for c in serialized.columns])

        for batch in _batched(serialized.rows(), 10000):
            cursor.executemany(
                f'INSERT INTO "{name}" ({col_names}) VALUES ({placeholders})', batch
            )

        LOGGER.info(f"  {name}: {len(serialized):,} rows")

    _create_sqlite_indexes(cursor)
    conn.commit()
    conn.close()

    return output_path


def _export_postgresql(tables: dict[str, pl.DataFrame], output_path: Path) -> Path:
    """Export tables to PostgreSQL COPY format dump."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(
            f"-- MTGJSON PostgreSQL Dump\n-- Generated: {datetime.now().strftime('%Y-%m-%d')}\n"
        )
        f.write("BEGIN;\n\n")

        for name, df in tables.items():
            if len(df) == 0:
                continue

            serialized = serialize_complex_types(df)

            cols = ",\n    ".join([f'"{c}" TEXT' for c in serialized.columns])
            f.write(f'CREATE TABLE "{name}" (\n    {cols}\n);\n\n')

            col_names = ", ".join([f'"{c}"' for c in serialized.columns])
            f.write(f'COPY "{name}" ({col_names}) FROM stdin;\n')

            for row in serialized.rows():
                escaped = [_escape_postgres(v) for v in row]
                f.write("\t".join(escaped) + "\n")

            f.write("\\.\n\n")
            LOGGER.info(f"  {name}: {len(serialized):,} rows")

        f.write("-- Indexes\n")
        _write_postgres_indexes(f)
        f.write("\nCOMMIT;\n")

    return output_path


def _create_sqlite_indexes(cursor: Any) -> None:
    """Create common indexes for SQLite."""
    indexes = [
        ("idx_cards_uuid", "cards", "uuid"),
        ("idx_cards_name", "cards", "name"),
        ("idx_cards_setCode", "cards", "setCode"),
        ("idx_cardIdentifiers_uuid", "cardIdentifiers", "uuid"),
        ("idx_cardLegalities_uuid", "cardLegalities", "uuid"),
        ("idx_sets_code", "sets", "code"),
    ]
    for idx_name, table, col in indexes:
        try:
            cursor.execute(f'CREATE INDEX "{idx_name}" ON "{table}" ("{col}")')
        except Exception:
            pass


def _write_postgres_indexes(f: Any) -> None:
    """Write PostgreSQL index statements."""
    indexes = [
        ("idx_cards_uuid", "cards", "uuid"),
        ("idx_cards_name", "cards", "name"),
        ("idx_cards_setCode", "cards", "setCode"),
        ("idx_cardIdentifiers_uuid", "cardIdentifiers", "uuid"),
        ("idx_cardLegalities_uuid", "cardLegalities", "uuid"),
        ("idx_sets_code", "sets", "code"),
    ]
    for idx_name, table, col in indexes:
        f.write(f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON "{table}" ("{col}");\n')


def build_all_printings_json(
    output_path: Path | str,
    cards_df: pl.DataFrame | None = None,
) -> Path:
    """
    Build AllPrintings.json with complete MTGJSON structure.

    Args:
        output_path: Where to write AllPrintings.json
        cards_df: Pre-renamed DataFrame with setCode column and camelCase fields.
        enable_profiling: If True, print timing report at end.
    """
    # Lazy imports to avoid circular dependency with pipeline
    from mtgjson5.context import PipelineContext
    from mtgjson5.pipeline import (
        build_decks_expanded,
        build_sealed_products_df,
        build_set_metadata_df,
        rename_all_the_things,
    )

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Create minimal context from GLOBAL_CACHE
    ctx = PipelineContext(
        sets_df=GLOBAL_CACHE.sets_df,
        sealed_products_lf=GLOBAL_CACHE.sealed_products_df,
        sealed_contents_lf=GLOBAL_CACHE.sealed_contents_df,
        decks_lf=GLOBAL_CACHE.decks_df,
        boosters_lf=GLOBAL_CACHE.boosters_df,
    )

    try:
        LOGGER.info("Building AllPrintings.json...")

        # Load/prepare cards
        if cards_df is None:
            if GLOBAL_CACHE.final_cards_lf is None:
                raise RuntimeError(
                    "final_cards_lf not cached. Run build_cards() first."
                )
            cards_df = rename_all_the_things(
                GLOBAL_CACHE.final_cards_lf, "card_set"
            ).collect()

        # Get set metadata
        set_meta_df = build_set_metadata_df(ctx)
        set_meta = {row["code"]: row for row in set_meta_df.to_dicts()}

        # Load booster configs
        booster_configs: dict[str, dict] = {}
        if GLOBAL_CACHE.boosters_df is not None:
            booster_df_lazy = GLOBAL_CACHE.boosters_df
            booster_df: pl.DataFrame = (
                booster_df_lazy.collect()
                if isinstance(booster_df_lazy, pl.LazyFrame)
                else booster_df_lazy
            )
            for row in booster_df.iter_rows(named=True):
                set_code = row.get("setCode", "").upper()
                config_raw = row.get("config")
                if config_raw and set_code:
                    if isinstance(config_raw, str):
                        try:
                            booster_configs[set_code] = json.loads(config_raw)
                        except json.JSONDecodeError:
                            pass
                    elif isinstance(config_raw, dict):
                        booster_configs[set_code] = config_raw

        # Load sealed products
        sealed_by_set: dict[str, list[dict]] = {}
        try:
            sealed_df = build_sealed_products_df(ctx)
            if (
                sealed_df is not None
                and len(sealed_df) > 0
                and "setCode" in sealed_df.columns
            ):
                for set_code in set_meta:
                    set_sealed_df = sealed_df.filter(pl.col("setCode") == set_code)
                    if len(set_sealed_df) > 0:
                        sealed_list = []
                        for row in set_sealed_df.drop("setCode").to_dicts():
                            sealed_obj = {
                                "category": row.get("category"),
                                "contents": row.get("contents"),
                                "identifiers": row.get("identifiers", {}),
                                "name": row.get("name", ""),
                                "purchaseUrls": row.get("purchaseUrls", {}),
                                "subtype": row.get("subtype"),
                                "uuid": row.get("uuid", ""),
                            }
                            if row.get("cardCount"):
                                sealed_obj["cardCount"] = row["cardCount"]
                            if row.get("releaseDate"):
                                sealed_obj["releaseDate"] = row["releaseDate"]
                            sealed_list.append(clean_nested(sealed_obj))
                        sealed_by_set[set_code] = sealed_list
        except Exception as e:
            LOGGER.warning(f"Failed to load sealed products: {e}")

        # Load decks
        decks_by_set: dict[str, list[dict]] = {}
        try:
            decks_df = build_decks_expanded(ctx)
            if (
                decks_df is not None
                and len(decks_df) > 0
                and "setCode" in decks_df.columns
            ):
                for set_code in set_meta:
                    set_decks_df = decks_df.filter(pl.col("setCode") == set_code)
                    if len(set_decks_df) > 0:
                        deck_list = []
                        for row in set_decks_df.to_dicts():
                            deck_obj = {
                                "code": row.get("code", set_code),
                                "commander": row.get("commander", []),
                                "displayCommander": row.get("displayCommander", []),
                                "mainBoard": row.get("mainBoard", []),
                                "name": row.get("name", ""),
                                "planes": row.get("planes", []),
                                "releaseDate": row.get("releaseDate"),
                                "schemes": row.get("schemes", []),
                                "sealedProductUuids": row.get("sealedProductUuids"),
                                "sideBoard": row.get("sideBoard", []),
                                "sourceSetCodes": row.get("sourceSetCodes", []),
                                "tokens": row.get("tokens", []),
                                "type": row.get("type", ""),
                            }
                            deck_list.append(clean_nested(deck_obj, omit_empty=False))
                        decks_by_set[set_code] = deck_list
        except Exception as e:
            LOGGER.warning(f"Failed to load decks: {e}")

        # Load translations
        translations_by_name: dict[str, dict] = {}
        translations_path = constants.RESOURCE_PATH / "mkm_set_name_translations.json"
        if translations_path.exists():
            with translations_path.open(encoding="utf-8") as f:
                raw = json.load(f)
                for set_name_or_code, langs in raw.items():
                    translations_by_name[set_name_or_code] = {
                        "Chinese Simplified": langs.get("zhs"),
                        "Chinese Traditional": langs.get("zht"),
                        "French": langs.get("fr"),
                        "German": langs.get("de"),
                        "Italian": langs.get("it"),
                        "Japanese": langs.get("ja"),
                        "Korean": langs.get("ko"),
                        "Portuguese (Brazil)": langs.get("pt"),
                        "Russian": langs.get("ru"),
                        "Spanish": langs.get("es"),
                    }

        # Build languages per set
        languages_by_set_temp: dict[str, set[str]] = {}
        languages_by_set: dict[str, list[str]] = {}
        if "foreignData" in cards_df.columns:
            try:
                fd_check = cards_df.select(
                    pl.col("setCode"),
                    pl.col("foreignData")
                    .list.eval(pl.element().struct.field("language"))
                    .list.unique()
                    .alias("_langs"),
                ).filter(pl.col("_langs").list.len() > 0)
                if len(fd_check) > 0:
                    for row in fd_check.iter_rows(named=True):
                        set_code = row["setCode"]
                        langs = row["_langs"] or []
                        if set_code not in languages_by_set_temp:
                            languages_by_set_temp[set_code] = set()
                        languages_by_set_temp[set_code].update(langs)
                    for code in list(languages_by_set_temp.keys()):
                        languages_by_set_temp[code].add("English")
                        languages_by_set[code] = sorted(languages_by_set_temp[code])
            except Exception:
                pass

        cards_by_set: dict[str, pl.DataFrame] = cards_df.partition_by(  # type: ignore[assignment]
            "setCode", as_dict=True, include_key=True
        )
        set_codes = sorted(cards_by_set.keys())

        # Pre-partition token sets for efficient lookup
        token_sets_by_code: dict[str, pl.DataFrame] = {}
        for code, df in cards_by_set.items():
            token_df = df.filter(pl.col("layout").is_in(TOKEN_LAYOUTS))
            if len(token_df) > 0:
                token_sets_by_code[code] = token_df

        # Build set objects
        data: dict[str, dict[str, Any]] = {}
        for set_code in set_codes:
            meta_row = set_meta.get(set_code, {})
            set_cards_df = cards_by_set[set_code]
            set_cards_only = set_cards_df.filter(~pl.col("layout").is_in(TOKEN_LAYOUTS))
            set_tokens_df = set_cards_df.filter(pl.col("layout").is_in(TOKEN_LAYOUTS))
            token_set_code: str = meta_row.get("tokenSetCode", f"T{set_code}")
            if token_set_code != set_code and token_set_code in token_sets_by_code:
                extra_tokens = token_sets_by_code[token_set_code]
                if len(extra_tokens) > 0:
                    set_tokens_df = pl.concat([set_tokens_df, extra_tokens])
            cards_list = dataframe_to_cards_list(set_cards_only)
            tokens_list = dataframe_to_cards_list(set_tokens_df)
            cards = [clean_nested(c) for c in cards_list]
            tokens = [clean_nested(t) for t in tokens_list]
            booster = booster_configs.get(set_code)
            if booster and isinstance(booster, dict):
                booster = {k: v for k, v in booster.items() if v is not None}
            else:
                booster = None
            set_sealed_products: list[dict] = sealed_by_set.get(set_code, [])
            set_deck_list: list[dict] = decks_by_set.get(set_code, [])
            set_name = meta_row.get("name", "")
            set_translations = translations_by_name.get(
                set_name
            ) or translations_by_name.get(
                set_code,
                {
                    "Chinese Simplified": None,
                    "Chinese Traditional": None,
                    "French": None,
                    "German": None,
                    "Italian": None,
                    "Japanese": None,
                    "Korean": None,
                    "Portuguese (Brazil)": None,
                    "Russian": None,
                    "Spanish": None,
                },
            )
            languages = languages_by_set.get(set_code, ["English"])
            base_set_size = meta_row.get("baseSetSize")
            if base_set_size is None:
                base_set_size = len([c for c in cards if not c.get("isReprint")])
                if base_set_size == 0:
                    base_set_size = len(cards)
            total_set_size = meta_row.get("totalSetSize") or len(cards)
            set_obj = {
                "baseSetSize": base_set_size,
                "cards": cards,
                "code": set_code,
                "isFoilOnly": meta_row.get("isFoilOnly", False),
                "isOnlineOnly": meta_row.get("isOnlineOnly", False),
                "keyruneCode": meta_row.get("keyruneCode", set_code),
                "languages": languages,
                "name": meta_row.get("name", set_code),
                "releaseDate": meta_row.get("releaseDate", ""),
                "tokens": tokens,
                "totalSetSize": total_set_size,
                "translations": set_translations,
                "type": meta_row.get("type", ""),
            }

            if booster:
                set_obj["booster"] = booster
            if set_sealed_products:
                set_obj["sealedProduct"] = set_sealed_products
            if set_deck_list:
                set_obj["decks"] = set_deck_list
            if meta_row.get("mtgoCode"):
                set_obj["mtgoCode"] = meta_row["mtgoCode"]
            if meta_row.get("parentCode"):
                set_obj["parentCode"] = meta_row["parentCode"]
            if meta_row.get("block"):
                set_obj["block"] = meta_row["block"]
            if meta_row.get("tcgplayerGroupId"):
                set_obj["tcgplayerGroupId"] = meta_row["tcgplayerGroupId"]
            if meta_row.get("tokenSetCode"):
                set_obj["tokenSetCode"] = meta_row["tokenSetCode"]
            if meta_row.get("cardsphereSetId"):
                set_obj["cardsphereSetId"] = meta_row["cardsphereSetId"]
            if meta_row.get("mcmId"):
                set_obj["mcmId"] = meta_row["mcmId"]
            if meta_row.get("mcmName"):
                set_obj["mcmName"] = meta_row["mcmName"]
            data[set_code] = set_obj
        # Write JSON
        meta = MtgjsonMetaObject()
        output = {
            "meta": {"date": meta.date, "version": meta.version},
            "data": data,
        }
        with open(output_path, "wb") as f:
            f.write(
                orjson.dumps(output, option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS)
            )
            return output_path
        LOGGER.info(f"Wrote AllPrintings.json to {output_path}")
    finally:
        LOGGER.info("Completed AllPrintings.json build.")
