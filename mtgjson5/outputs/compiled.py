"""Compiled output formats (AllPrintings.json, SQLite, PostgreSQL, CSV, Parquet)."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Literal

import orjson
import polars as pl

from mtgjson5 import constants
from mtgjson5.classes import MtgjsonMetaObject
from mtgjson5.mtgjson_models import clean_nested, dataframe_to_cards_list
from mtgjson5.utils import LOGGER, deep_sort_keys

from .base import (
    ExportFormat,
    register_export_format,
)
from .utils import escape_postgres, serialize_complex_types


if TYPE_CHECKING:
    from mtgjson5.context import PipelineContext


@register_export_format
class JsonOutput(ExportFormat):
    """AllPrintings.json - Complete set data with all cards."""

    NAME: ClassVar[str] = "json"
    ALIASES: ClassVar[frozenset[str]] = frozenset()
    FILE_NAME: ClassVar[str] = "AllPrintings.json"

    def write(self, ctx: PipelineContext, output_path: Path) -> Path | None:
        """Build AllPrintings.json from parquet cache."""
        from mtgjson5.pipeline import (
            build_decks_expanded,
            build_sealed_products_df,
            build_set_metadata_df,
        )

        parquet_dir = constants.CACHE_PATH / "_parquet"
        if not parquet_dir.exists():
            LOGGER.error("No parquet cache found. Run build_cards() first.")
            return None

        LOGGER.info("Building AllPrintings.json...")

        set_codes = sorted(
            [
                p.name.replace("setCode=", "")
                for p in parquet_dir.iterdir()
                if p.is_dir() and p.name.startswith("setCode=")
            ]
        )

        set_meta_df = build_set_metadata_df(ctx)
        set_meta = {row["code"]: row for row in set_meta_df.to_dicts()}

        sealed_df = build_sealed_products_df(ctx)
        if isinstance(sealed_df, pl.LazyFrame):
            sealed_df = sealed_df.collect()

        decks_df = build_decks_expanded(ctx)
        if isinstance(decks_df, pl.LazyFrame):
            decks_df = decks_df.collect()

        meta = MtgjsonMetaObject()
        meta_dict = {"date": meta.date, "version": meta.version}

        all_sets = {}
        for set_code in set_codes:
            cards_path = parquet_dir / f"setCode={set_code}"
            if not cards_path.exists():
                continue

            cards_df = pl.read_parquet(cards_path / "*.parquet")
            cards = dataframe_to_cards_list(cards_df)

            meta_row = set_meta.get(set_code, {})

            # Sealed products
            set_sealed = []
            if not sealed_df.is_empty() and "setCode" in sealed_df.columns:
                set_sealed = [
                    clean_nested(sp)
                    for sp in sealed_df.filter(pl.col("setCode") == set_code)
                    .drop("setCode")
                    .to_dicts()
                ]

            # Decks
            set_decks = []
            if not decks_df.is_empty() and "setCode" in decks_df.columns:
                set_decks = [
                    clean_nested(d, omit_empty=False)
                    for d in decks_df.filter(pl.col("setCode") == set_code)
                    .drop("setCode")
                    .to_dicts()
                ]

            # Booster config
            booster_raw = meta_row.get("booster")
            booster = None
            if booster_raw and isinstance(booster_raw, str):
                try:
                    booster = json.loads(booster_raw)
                except json.JSONDecodeError:
                    pass

            # Translations
            raw_translations = meta_row.get("translations", {})
            translations = {
                lang_name: raw_translations.get(lang_name)
                for lang_name in constants.LANGUAGE_MAP.values()
            }

            set_data = {
                "baseSetSize": meta_row.get("baseSetSize", len(cards)),
                "cards": cards,
                "code": set_code,
                "isFoilOnly": meta_row.get("isFoilOnly", False),
                "isOnlineOnly": meta_row.get("isOnlineOnly", False),
                "keyruneCode": meta_row.get("keyruneCode", set_code),
                "name": meta_row.get("name", set_code),
                "releaseDate": meta_row.get("releaseDate", ""),
                "tokens": [],
                "totalSetSize": meta_row.get("totalSetSize", len(cards)),
                "translations": translations,
                "type": meta_row.get("type", ""),
            }

            if booster:
                set_data["booster"] = booster
            if set_sealed:
                set_data["sealedProduct"] = set_sealed
            if set_decks:
                set_data["decks"] = set_decks
            for field in [
                "mtgoCode",
                "parentCode",
                "block",
                "tcgplayerGroupId",
                "tokenSetCode",
                "cardsphereSetId",
                "mcmId",
                "mcmName",
            ]:
                if meta_row.get(field):
                    set_data[field] = meta_row[field]
            if meta_row.get("isNonFoilOnly"):
                set_data["isNonFoilOnly"] = True

            all_sets[set_code] = deep_sort_keys(set_data)

        output_file = output_path / self.FILE_NAME
        output = {"meta": meta_dict, "data": all_sets}

        with output_file.open("wb") as f:
            f.write(orjson.dumps(output))

        LOGGER.info(f"Wrote {self.FILE_NAME} ({len(all_sets)} sets)")
        return output_file


@register_export_format
class SqliteOutput(ExportFormat):
    """SQLite database export."""

    NAME: ClassVar[str] = "sql"
    ALIASES: ClassVar[frozenset[str]] = frozenset()
    FILE_NAME: ClassVar[str] = "AllPrintings.sqlite"

    def write(self, ctx: PipelineContext, output_path: Path) -> Path | None:
        parquet_dir = constants.CACHE_PATH / "_parquet"
        if not parquet_dir.exists():
            LOGGER.error("No parquet cache found.")
            return None

        LOGGER.info("Building AllPrintings.sqlite...")
        cards_df = pl.read_parquet(parquet_dir / "**/*.parquet")

        db_path = output_path / self.FILE_NAME
        if db_path.exists():
            db_path.unlink()

        connection_uri = f"sqlite:///{db_path}"

        # Serialize complex types and flatten for SQL
        flat_df = self._flatten_cards(cards_df)
        serialized = serialize_complex_types(flat_df)

        # Use Polars native write_database with ADBC
        serialized.write_database(
            table_name="cards",
            connection=connection_uri,
            if_table_exists="replace",
            engine="adbc",
        )
        LOGGER.info(f"  cards: {len(serialized):,} rows")

        # Create indexes via raw SQL
        import sqlite3

        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        self._create_indexes(cursor)
        conn.commit()
        conn.close()

        LOGGER.info(f"Wrote {self.FILE_NAME}")
        return db_path

    def _flatten_cards(self, df: pl.DataFrame) -> pl.DataFrame:
        """Flatten cards for SQL table (exclude nested columns)."""
        exclude = {"identifiers", "legalities", "rulings", "foreignData"}
        return df.select([c for c in df.columns if c not in exclude])

    def _create_indexes(self, cursor: Any) -> None:
        """Create indexes on SQLite database tables."""
        indexes = [
            ("idx_cards_uuid", "cards", "uuid"),
            ("idx_cards_name", "cards", "name"),
            ("idx_cards_setCode", "cards", "setCode"),
        ]
        for idx_name, table, col in indexes:
            try:
                cursor.execute(f'CREATE INDEX "{idx_name}" ON "{table}" ("{col}")')
            except Exception:
                pass


@register_export_format
class PostgresOutput(ExportFormat):
    """PostgreSQL export - file dump or direct ADBC connection.

    Default: Writes AllPrintings.sql dump file.
    With connection_uri in ctx: Writes directly to PostgreSQL via ADBC.
    """

    NAME: ClassVar[str] = "psql"
    ALIASES: ClassVar[frozenset[str]] = frozenset()
    FILE_NAME: ClassVar[str] = "AllPrintings.psql"

    def write(self, ctx: PipelineContext, output_path: Path) -> Path | None:
        parquet_dir = constants.CACHE_PATH / "_parquet"
        if not parquet_dir.exists():
            LOGGER.error("No parquet cache found.")
            return None

        cards_df = pl.read_parquet(parquet_dir / "**/*.parquet")
        flat_df = self._flatten_cards(cards_df)
        serialized = serialize_complex_types(flat_df)

        # Check if we have a PostgreSQL connection URI
        pg_uri = getattr(ctx.args, "postgres_uri", None) if ctx.args else None

        if pg_uri:
            return self._write_to_database(serialized, pg_uri)
        return self._write_dump_file(serialized, output_path)

    def _write_to_database(  # pylint: disable=useless-return
        self, df: pl.DataFrame, connection_uri: str
    ) -> Path | None:
        """Write directly to PostgreSQL using ADBC driver."""
        LOGGER.info("Writing to PostgreSQL database via ADBC...")

        df.write_database(
            table_name="cards",
            connection=connection_uri,
            if_table_exists="replace",
            engine="adbc",
        )
        LOGGER.info(f"  cards: {len(df):,} rows")

        # Create indexes via raw connection
        try:
            import adbc_driver_postgresql.dbapi as pg_dbapi  # pylint: disable=import-error

            with pg_dbapi.connect(connection_uri) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        'CREATE INDEX IF NOT EXISTS "idx_cards_uuid" ON "cards" ("uuid")'
                    )
                    cursor.execute(
                        'CREATE INDEX IF NOT EXISTS "idx_cards_name" ON "cards" ("name")'
                    )
                    cursor.execute(
                        'CREATE INDEX IF NOT EXISTS "idx_cards_setCode" ON "cards" ("setCode")'
                    )
                conn.commit()
        except Exception as e:
            LOGGER.warning(f"Could not create indexes: {e}")

        LOGGER.info("Wrote to PostgreSQL database")
        return None

    def _write_dump_file(self, df: pl.DataFrame, output_path: Path) -> Path:
        """Write PostgreSQL COPY format dump file."""
        LOGGER.info("Building AllPrintings.sql (PostgreSQL dump)...")
        sql_path = output_path / self.FILE_NAME

        with open(sql_path, "w", encoding="utf-8") as f:
            f.write(
                f"-- MTGJSON PostgreSQL Dump\n-- Generated: {datetime.now().strftime('%Y-%m-%d')}\n"
            )
            f.write("BEGIN;\n\n")

            cols = ",\n    ".join([f'"{c}" TEXT' for c in df.columns])
            f.write(f'CREATE TABLE "cards" (\n    {cols}\n);\n\n')

            col_names = ", ".join([f'"{c}"' for c in df.columns])
            f.write(f'COPY "cards" ({col_names}) FROM stdin;\n')

            for row in df.rows():
                escaped = [escape_postgres(v) for v in row]
                f.write("\t".join(escaped) + "\n")

            f.write("\\.\n\n")
            LOGGER.info(f"  cards: {len(df):,} rows")

            f.write("-- Indexes\n")
            f.write(
                'CREATE INDEX IF NOT EXISTS "idx_cards_uuid" ON "cards" ("uuid");\n'
            )
            f.write(
                'CREATE INDEX IF NOT EXISTS "idx_cards_name" ON "cards" ("name");\n'
            )
            f.write(
                'CREATE INDEX IF NOT EXISTS "idx_cards_setCode" ON "cards" ("setCode");\n'
            )
            f.write("\nCOMMIT;\n")

        LOGGER.info(f"Wrote {self.FILE_NAME}")
        return sql_path

    def _flatten_cards(self, df: pl.DataFrame) -> pl.DataFrame:
        """Flatten cards DataFrame by excluding nested columns."""
        exclude = {"identifiers", "legalities", "rulings", "foreignData"}
        return df.select([c for c in df.columns if c not in exclude])


@register_export_format
class CsvOutput(ExportFormat):
    """CSV files export."""

    NAME: ClassVar[str] = "csv"
    ALIASES: ClassVar[frozenset[str]] = frozenset()
    FILE_NAME: ClassVar[str] = "csv"

    def write(self, ctx: PipelineContext, output_path: Path) -> Path | None:
        parquet_dir = constants.CACHE_PATH / "_parquet"
        if not parquet_dir.exists():
            LOGGER.error("No parquet cache found.")
            return None

        LOGGER.info("Building CSV files...")
        cards_df = pl.read_parquet(parquet_dir / "**/*.parquet")

        csv_dir = output_path / "csv"
        csv_dir.mkdir(parents=True, exist_ok=True)

        cards_path = csv_dir / "cards.csv"
        cards_df.write_csv(cards_path)
        LOGGER.info(f"  cards.csv: {len(cards_df):,} rows")

        return csv_dir


@register_export_format
class ParquetOutput(ExportFormat):
    """Parquet files export."""

    NAME: ClassVar[str] = "parquet"
    ALIASES: ClassVar[frozenset[str]] = frozenset()
    FILE_NAME: ClassVar[str] = "parquet"

    def write(self, ctx: PipelineContext, output_path: Path) -> Path | None:
        parquet_dir = constants.CACHE_PATH / "_parquet"
        if not parquet_dir.exists():
            LOGGER.error("No parquet cache found.")
            return None

        LOGGER.info("Building Parquet files...")
        cards_df = pl.read_parquet(parquet_dir / "**/*.parquet")

        parquet_out = output_path / "parquet"
        parquet_out.mkdir(parents=True, exist_ok=True)

        cards_path = parquet_out / "cards.parquet"
        compression: Literal["zstd"] = "zstd"
        cards_df.write_parquet(cards_path, compression=compression, compression_level=9)
        LOGGER.info(f"  cards.parquet: {len(cards_df):,} rows")

        return parquet_out
