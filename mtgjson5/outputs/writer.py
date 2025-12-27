"""Single output writer for all MTGJSON export formats."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import orjson
import polars as pl

from mtgjson5 import constants
from mtgjson5.classes import MtgjsonMetaObject
from mtgjson5.mtgjson_config import MtgjsonConfig
from mtgjson5.mtgjson_models import clean_nested, dataframe_to_cards_list
from mtgjson5.utils import LOGGER, deep_sort_keys

from .base import EXPORT_SCHEMAS, ExportSchema
from .utils import escape_postgres, escape_sqlite, serialize_complex_types


if TYPE_CHECKING:
    from mtgjson5.context import PipelineContext


class OutputWriter:
    """
    Single writer for all MTGJSON export formats.

    Usage:
        # Write specific format
        OutputWriter(ctx).write(JsonSchema)
        OutputWriter(ctx).write(SqlSchema)

        # Write by format name
        OutputWriter(ctx).write("json")
        OutputWriter(ctx).write("sql")

        # Write multiple formats
        OutputWriter(ctx).write_all(["json", "sql", "parquet"])

        # From args
        OutputWriter.from_args(ctx).write_all()
    """

    def __init__(self, ctx: PipelineContext):
        self.ctx = ctx
        self.output_path = MtgjsonConfig().output_path
        self._formats: list[str] = []

    @classmethod
    def from_args(cls, ctx: PipelineContext) -> OutputWriter:
        """Create writer with formats from ctx.args.export."""
        writer = cls(ctx)
        if ctx.args:
            formats = getattr(ctx.args, "export", None) or []
            writer._formats = [f.lower() for f in formats]
        return writer

    def write(self, schema: type[ExportSchema] | str) -> Path | None:
        """
        Write a single export format.

        Args:
            schema: ExportSchema class or format name string

        Returns:
            Path to written file, or None if failed
        """
        if isinstance(schema, str):
            schema_cls = EXPORT_SCHEMAS.get(schema.lower())
            if not schema_cls:
                LOGGER.error(f"Unknown export format: {schema}")
                return None
        else:
            schema_cls = schema

        fmt = schema_cls.FORMAT
        LOGGER.info(f"Writing {fmt} format...")

        if fmt == "csv":
            return self._write_csv(schema_cls)
        if fmt == "json":
            return self._write_json(schema_cls)
        if fmt == "sql":
            return self._write_sql(schema_cls)
        if fmt == "sqlite":
            return self._write_sqlite(schema_cls)
        if fmt == "psql":
            return self._write_postgresql(schema_cls)
        if fmt == "parquet":
            return self._write_parquet(schema_cls)
        LOGGER.error(f"Unsupported format: {fmt}")
        return None

    def write_all(self, formats: list[str] | None = None) -> dict[str, Path | None]:
        """
        Write multiple export formats.

        Args:
            formats: List of format names, or None to use self._formats

        Returns:
            Dict mapping format name to written path
        """
        formats = formats or self._formats
        if not formats:
            LOGGER.info("No formats to write")
            return {}

        results = {}
        for fmt in formats:
            try:
                results[fmt] = self.write(fmt)
            except Exception as e:
                LOGGER.error(f"Failed to write {fmt}: {e}")
                results[fmt] = None

        return results

    def _load_cards(self) -> pl.DataFrame | None:
        """Load cards from parquet cache."""
        parquet_dir = constants.CACHE_PATH / "_parquet"
        if not parquet_dir.exists():
            LOGGER.error("No parquet cache found. Run build_cards() first.")
            return None
        return pl.read_parquet(parquet_dir / "**/*.parquet")

    def _write_json(self, schema: type[ExportSchema]) -> Path | None:
        """Write AllPrintings.json."""
        from mtgjson5.pipeline import (
            build_sealed_products_df,
            build_set_metadata_df,
        )

        parquet_dir = constants.CACHE_PATH / "_parquet"
        if not parquet_dir.exists():
            LOGGER.error("No parquet cache found.")
            return None

        set_codes = sorted(
            [
                p.name.replace("setCode=", "")
                for p in parquet_dir.iterdir()
                if p.is_dir() and p.name.startswith("setCode=")
            ]
        )

        set_meta_df = build_set_metadata_df(self.ctx)
        if isinstance(set_meta_df, pl.LazyFrame):
            set_meta_df = set_meta_df.collect()
        set_meta = {row["code"]: row for row in set_meta_df.to_dicts()}

        sealed_df = build_sealed_products_df(self.ctx)
        if isinstance(sealed_df, pl.LazyFrame):
            sealed_df = sealed_df.collect()

        # Use raw decks for AllPrintings.json
        decks_df: pl.DataFrame | None = None
        if self.ctx.decks_df is not None:
            if isinstance(self.ctx.decks_df, pl.LazyFrame):
                decks_df = self.ctx.decks_df.collect()
            else:
                decks_df = self.ctx.decks_df

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

            set_sealed = []
            if len(sealed_df) > 0 and "setCode" in sealed_df.columns:
                set_sealed = [
                    clean_nested(sp)
                    for sp in sealed_df.filter(pl.col("setCode") == set_code)
                    .drop("setCode")
                    .to_dicts()
                ]

            set_decks = []
            if (
                decks_df is not None
                and decks_df.height > 0
                and "setCode" in decks_df.columns
            ):
                for d in decks_df.filter(pl.col("setCode") == set_code).to_dicts():
                    deck = self._format_deck_for_set(d, set_code)
                    if deck:
                        set_decks.append(deck)

            booster_raw = meta_row.get("booster")
            booster = None
            if booster_raw and isinstance(booster_raw, str):
                try:
                    booster = json.loads(booster_raw)
                except json.JSONDecodeError:
                    pass

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

        output_file = self.output_path / schema.FILE_NAME
        output = {"meta": meta_dict, "data": all_sets}

        with output_file.open("wb") as f:
            f.write(orjson.dumps(output))

        LOGGER.info(f"Wrote {schema.FILE_NAME} ({len(all_sets)} sets)")
        return output_file

    def _write_sqlite(self, schema: type[ExportSchema]) -> Path | None:
        """Write SQLite database using native sqlite3."""
        import sqlite3

        cards_df = self._load_cards()
        if cards_df is None:
            return None

        db_path = self.output_path / schema.FILE_NAME
        if db_path.exists():
            db_path.unlink()

        flat_df = self._flatten_for_sql(cards_df)
        serialized = serialize_complex_types(flat_df)

        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        # Create table with TEXT columns
        cols = ", ".join([f'"{c}" TEXT' for c in serialized.columns])
        cursor.execute(f'CREATE TABLE "cards" ({cols})')

        # Batch insert rows
        placeholders = ", ".join(["?" for _ in serialized.columns])
        col_names = ", ".join([f'"{c}"' for c in serialized.columns])

        batch_size = 10000
        rows = serialized.rows()
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            cursor.executemany(
                f'INSERT INTO "cards" ({col_names}) VALUES ({placeholders})', batch
            )

        # Create indexes
        for idx, col in [("uuid", "uuid"), ("name", "name"), ("setCode", "setCode")]:
            try:
                cursor.execute(f'CREATE INDEX "idx_cards_{idx}" ON "cards" ("{col}")')
            except Exception:
                pass

        conn.commit()
        conn.close()

        LOGGER.info(f"Wrote {schema.FILE_NAME} ({len(serialized):,} rows)")
        return db_path

    def _write_sql(self, schema: type[ExportSchema]) -> Path | None:
        """Write SQLite text file."""
        cards_df = self._load_cards()
        if cards_df is None:
            return None

        flat_df = self._flatten_for_sql(cards_df)
        serialized = serialize_complex_types(flat_df)

        sql_path = self.output_path / schema.FILE_NAME
        with open(sql_path, "w", encoding="utf-8") as f:
            f.write(
                f"-- MTGJSON SQLite Dump\n-- Generated: {datetime.now().strftime('%Y-%m-%d')}\n"
            )
            f.write("BEGIN TRANSACTION;\n\n")

            cols = ",\n    ".join([f'"{c}" TEXT' for c in serialized.columns])
            f.write(f'CREATE TABLE IF NOT EXISTS "cards" (\n    {cols}\n);\n\n')

            col_names = ", ".join([f'"{c}"' for c in serialized.columns])
            for row in serialized.rows():
                values = ", ".join(escape_sqlite(v) for v in row)
                f.write(f'INSERT INTO "cards" ({col_names}) VALUES ({values});\n')

            f.write('\nCREATE INDEX IF NOT EXISTS "idx_cards_uuid" ON "cards" ("uuid");\n')
            f.write('CREATE INDEX IF NOT EXISTS "idx_cards_name" ON "cards" ("name");\n')
            f.write('CREATE INDEX IF NOT EXISTS "idx_cards_setCode" ON "cards" ("setCode");\n')
            f.write("\nCOMMIT;\n")

        LOGGER.info(f"Wrote {schema.FILE_NAME} ({len(serialized):,} rows)")
        return sql_path

    def _write_postgresql(self, schema: type[ExportSchema]) -> Path | None:
        """Write PostgreSQL - direct ADBC or dump file."""
        cards_df = self._load_cards()
        if cards_df is None:
            return None

        flat_df = self._flatten_for_sql(cards_df)
        serialized = serialize_complex_types(flat_df)

        pg_uri = getattr(self.ctx.args, "postgres_uri", None) if self.ctx.args else None

        if pg_uri:
            # Direct database write
            serialized.write_database(
                table_name="cards",
                connection=pg_uri,
                if_table_exists="replace",
                engine="adbc",
            )
            LOGGER.info(f"Wrote to PostgreSQL ({len(serialized):,} rows)")
            return None
        # Dump file
        sql_path = self.output_path / schema.FILE_NAME
        with open(sql_path, "w", encoding="utf-8") as f:
            f.write(
                f"-- MTGJSON PostgreSQL Dump\n-- Generated: {datetime.now().strftime('%Y-%m-%d')}\n"
            )
            f.write("BEGIN;\n\n")

            cols = ",\n    ".join([f'"{c}" TEXT' for c in serialized.columns])
            f.write(f'CREATE TABLE "cards" (\n    {cols}\n);\n\n')

            col_names = ", ".join([f'"{c}"' for c in serialized.columns])
            f.write(f'COPY "cards" ({col_names}) FROM stdin;\n')

            for row in serialized.rows():
                escaped = [escape_postgres(v) for v in row]
                f.write("\t".join(escaped) + "\n")

            f.write("\\.\n\n")
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

        LOGGER.info(f"Wrote {schema.FILE_NAME} ({len(serialized):,} rows)")
        return sql_path

    def _write_csv(self, _schema: type[ExportSchema]) -> Path | None:
        """Write CSV files - multiple tables."""
        return self._write_split_tables("csv")

    def _write_parquet(self, _schema: type[ExportSchema]) -> Path | None:
        """Write Parquet files - multiple tables."""
        return self._write_split_tables("parquet")

    def _write_split_tables(self, fmt: str) -> Path | None:
        """Write split tables using Polars."""
        from mtgjson5.pipeline import build_set_metadata_df

        parquet_dir = constants.CACHE_PATH / "_parquet"
        if not parquet_dir.exists():
            LOGGER.error("No parquet cache found")
            return None

        out_dir = self.output_path / fmt
        out_dir.mkdir(parents=True, exist_ok=True)

        # Load cards
        card_dirs = [
            d
            for d in parquet_dir.iterdir()
            if d.is_dir() and not d.name.startswith("setCode=T")
        ]
        if not card_dirs:
            LOGGER.error("No card parquet directories found")
            return None
        cards_df = pl.read_parquet(parquet_dir / "setCode=[!T]*/*.parquet")
        schema = cards_df.schema

        # Find ALL nested columns (List or Struct types)
        nested = {
            c for c in cards_df.columns if isinstance(schema[c], (pl.List, pl.Struct))
        }
        scalar_cols = [
            c for c in cards_df.columns if c not in nested and not c.startswith("_")
        ]

        def write_out(df: pl.DataFrame, name: str) -> None:
            path = out_dir / f"{name}.{fmt}"
            if fmt == "csv":
                df.write_csv(path)
            else:
                compression: Literal["zstd"] = "zstd"
                df.write_parquet(path, compression=compression, compression_level=9)
            LOGGER.info(f"  {name}.{fmt}: {df.height:,} rows")

        # cards - scalar columns
        write_out(cards_df.select(scalar_cols), "cards")

        # cardIdentifiers - unnest struct
        if "identifiers" in schema and isinstance(schema["identifiers"], pl.Struct):
            df = (
                cards_df.select("uuid", "identifiers")
                .filter(pl.col("identifiers").is_not_null())
                .unnest("identifiers")
            )
            write_out(df, "cardIdentifiers")

        # cardLegalities - unnest struct
        if "legalities" in schema and isinstance(schema["legalities"], pl.Struct):
            df = (
                cards_df.select("uuid", "legalities")
                .filter(pl.col("legalities").is_not_null())
                .unnest("legalities")
            )
            write_out(df, "cardLegalities")

        # cardForeignData - explode list of structs
        if "foreignData" in schema and isinstance(schema["foreignData"], pl.List):
            df = (
                cards_df.select("uuid", "foreignData")
                .filter(pl.col("foreignData").list.len() > 0)
                .explode("foreignData")
                .unnest("foreignData")
            )
            write_out(df, "cardForeignData")

        # cardRulings - explode list of structs
        if "rulings" in schema and isinstance(schema["rulings"], pl.List):
            df = (
                cards_df.select("uuid", "rulings")
                .filter(pl.col("rulings").list.len() > 0)
                .explode("rulings")
                .unnest("rulings")
            )
            write_out(df, "cardRulings")

        # cardPurchaseUrls - unnest struct
        if "purchaseUrls" in schema and isinstance(schema["purchaseUrls"], pl.Struct):
            df = (
                cards_df.select("uuid", "purchaseUrls")
                .filter(pl.col("purchaseUrls").is_not_null())
                .unnest("purchaseUrls")
            )
            write_out(df, "cardPurchaseUrls")

        # meta
        meta = MtgjsonMetaObject()
        write_out(
            pl.DataFrame({"date": [meta.date], "version": [meta.version]}), "meta"
        )

        # AllPrintings
        if fmt == "parquet":
            write_out(cards_df, "AllPrintings")

        # tokens
        token_dirs = [
            d
            for d in parquet_dir.iterdir()
            if d.is_dir() and d.name.startswith("setCode=T")
        ]
        if token_dirs:
            tokens_df = pl.read_parquet(parquet_dir / "setCode=T*/*.parquet")
            token_scalar = [
                c
                for c in tokens_df.columns
                if c not in nested and not c.startswith("_")
            ]
            write_out(tokens_df.select(token_scalar), "tokens")
            if "identifiers" in tokens_df.schema and isinstance(
                tokens_df.schema["identifiers"], pl.Struct
            ):
                df = (
                    tokens_df.select("uuid", "identifiers")
                    .filter(pl.col("identifiers").is_not_null())
                    .unnest("identifiers")
                )
                write_out(df, "tokenIdentifiers")

        # sets
        sets_df = None
        if self.ctx.sets_df is not None:
            sets_df = build_set_metadata_df(self.ctx)
            if isinstance(sets_df, pl.LazyFrame):
                sets_df = sets_df.collect()
        if sets_df is not None and len(sets_df) > 0:
            # Find nested columns in sets
            sets_schema = sets_df.schema
            sets_nested = {
                c
                for c in sets_df.columns
                if isinstance(sets_schema[c], (pl.List, pl.Struct))
            }
            sets_scalar = [
                c
                for c in sets_df.columns
                if c not in sets_nested and not c.startswith("_")
            ]
            write_out(sets_df.select(sets_scalar), "sets")

            # setTranslations
            if "translations" in sets_schema and isinstance(
                sets_schema["translations"], pl.Struct
            ):
                trans_df = (
                    sets_df.select("code", "translations")
                    .filter(pl.col("translations").is_not_null())
                    .unnest("translations")
                )
                if len(trans_df) > 0:
                    write_out(trans_df, "setTranslations")

        # Booster tables - parse from booster JSON configs
        booster_path = constants.CACHE_PATH / "github_booster.parquet"
        if booster_path.exists():
            booster_tables = self._parse_booster_tables()
            for table_name, table_df in booster_tables.items():
                if table_df is not None and len(table_df) > 0:
                    write_out(table_df, table_name)

        LOGGER.info(f"Wrote {fmt} files to {out_dir}")
        return out_dir

    def _parse_booster_tables(self) -> dict[str, pl.DataFrame]:
        """
        Parse booster JSON configs into relational tables.

        Returns dict with:
          - setBoosterSheets: sheet metadata
          - setBoosterSheetCards: cards in each sheet
          - setBoosterContents: slot configurations per booster
          - setBoosterContentWeights: weight per booster variant
        """
        booster_path = constants.CACHE_PATH / "github_booster.parquet"
        if not booster_path.exists():
            return {}

        booster_df = pl.read_parquet(booster_path)

        sheets_records = []
        sheet_cards_records = []
        contents_records = []
        weights_records = []

        for row in booster_df.iter_rows(named=True):
            set_code = row["setCode"]
            config_str = row["config"]
            if not config_str:
                continue

            config = json.loads(config_str)

            # Each key is a booster type (draft, collector, etc.)
            for booster_name, booster_data in config.items():
                if not isinstance(booster_data, dict):
                    continue

                # Parse sheets
                sheets = booster_data.get("sheets", {})
                for sheet_name, sheet_data in sheets.items():
                    if not isinstance(sheet_data, dict):
                        continue

                    sheets_records.append(
                        {
                            "setCode": set_code,
                            "boosterName": booster_name,
                            "sheetName": sheet_name,
                            "sheetIsFoil": sheet_data.get("foil", False),
                            "sheetHasBalanceColors": sheet_data.get(
                                "balanceColors", False
                            ),
                            "sheetTotalWeight": sheet_data.get("totalWeight", 0),
                        }
                    )

                    # Parse cards in sheet
                    cards = sheet_data.get("cards", {})
                    for card_uuid, weight in cards.items():
                        sheet_cards_records.append(
                            {
                                "setCode": set_code,
                                "boosterName": booster_name,
                                "sheetName": sheet_name,
                                "cardUuid": card_uuid,
                                "cardWeight": weight,
                            }
                        )

                # Parse boosters (contents and weights)
                boosters = booster_data.get("boosters", [])
                for idx, booster_variant in enumerate(boosters):
                    if not isinstance(booster_variant, dict):
                        continue

                    booster_weight = booster_variant.get("weight", 1)
                    weights_records.append(
                        {
                            "setCode": set_code,
                            "boosterName": booster_name,
                            "boosterIndex": idx,
                            "boosterWeight": booster_weight,
                        }
                    )

                    # Parse contents (sheet picks per slot)
                    contents = booster_variant.get("contents", {})
                    for sheet_name, picks in contents.items():
                        contents_records.append(
                            {
                                "setCode": set_code,
                                "boosterName": booster_name,
                                "boosterIndex": idx,
                                "sheetName": sheet_name,
                                "sheetPicks": picks,
                            }
                        )

        return {
            "setBoosterSheets": (
                pl.DataFrame(sheets_records) if sheets_records else pl.DataFrame()
            ),
            "setBoosterSheetCards": (
                pl.DataFrame(sheet_cards_records)
                if sheet_cards_records
                else pl.DataFrame()
            ),
            "setBoosterContents": (
                pl.DataFrame(contents_records) if contents_records else pl.DataFrame()
            ),
            "setBoosterContentWeights": (
                pl.DataFrame(weights_records) if weights_records else pl.DataFrame()
            ),
        }

    def _flatten_for_sql(self, df: pl.DataFrame) -> pl.DataFrame:
        """Flatten cards for SQL table (exclude nested columns)."""
        exclude = {"identifiers", "legalities", "rulings", "foreignData"}
        return df.select([c for c in df.columns if c not in exclude])

    def _format_deck_for_set(self, raw_deck: dict, set_code: str) -> dict | None:
        """Format a raw deck dict for AllPrintings.json (compact card refs)."""

        def compact_card_list(cards: list | None) -> list:
            if not cards:
                return []
            result = []
            for c in cards:
                if isinstance(c, dict) and "uuid" in c:
                    entry = {"count": c.get("count", 1), "uuid": c["uuid"]}
                    if c.get("isFoil"):
                        entry["isFoil"] = True
                    result.append(entry)
            return result

        deck = {
            "code": set_code,
            "commander": compact_card_list(raw_deck.get("commander")),
            "displayCommander": [],
            "mainBoard": compact_card_list(raw_deck.get("mainBoard")),
            "name": raw_deck.get("name", ""),
            "planes": [],
            "releaseDate": raw_deck.get("releaseDate", ""),
            "schemes": [],
            "sealedProductUuids": raw_deck.get("sealedProductUuids"),
            "sideBoard": compact_card_list(raw_deck.get("sideBoard")),
            "sourceSetCodes": raw_deck.get("sourceSetCodes") or [],
            "tokens": [],
            "type": raw_deck.get("type", ""),
        }
        return deck
