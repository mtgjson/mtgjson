"""
Consolidated MTGJSON Output Module.

This module consolidates and replaces:
- mtgjson5/outputs/writer.py (OutputWriter class)
- mtgjson5/pipeline.py (assemble_json_outputs, build_decks_expanded, etc.)
- mtgjson5/providers/v2/exporter.py (dead code - functionality absorbed here)

Architecture:
    Functional API:
        write_json(), write_set(), write_all_printings() - JSON output
        write_parquet(), write_csv(), write_sqlite(), etc. - table exports
        build_*() functions - construct data structures

    Class API (backwards-compatible):
        OutputWriter - orchestrates all export operations
        SetDataLoader - loads set data from parquet cache

Usage:
    # Functional approach (preferred for new code)
    from mtgjson5.outputs.out import generate_all, write_set, build_tables
    results = generate_all(parquet_dir, output_dir, sets_metadata)

    # Class approach (backwards-compatible)
    from mtgjson5.outputs.out import OutputWriter
    writer = OutputWriter(ctx)
    writer.write_set_jsons(parallel=True)
    writer.write_all(["json", "sqlite", "parquet"])
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import orjson
import polars as pl

from mtgjson5 import constants
from mtgjson5.classes import MtgjsonMetaObject
from mtgjson5.mtgjson_models import clean_nested, dataframe_to_cards_list
from mtgjson5.utils import LOGGER, deep_sort_keys


if TYPE_CHECKING:
    from mtgjson5.context import PipelineContext


# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------

ATOMIC_FIELDS = {
    "colorIdentity", "colors", "convertedManaCost", "edhrecRank", "edhrecSaltiness",
    "faceName", "firstPrinting", "foreignData", "hand", "hasAlternativeDeckLimit",
    "identifiers", "keywords", "layout", "leadershipSkills", "legalities", "life",
    "loyalty", "manaCost", "manaValue", "name", "power", "printings", "purchaseUrls",
    "relatedCards", "rulings", "side", "subtypes", "supertypes", "text", "toughness",
    "type", "types",
}

CARD_ENUM_FIELDS = {
    "availability", "borderColor", "colorIdentity", "colors", "finishes",
    "frameEffects", "frameVersion", "layout", "promoTypes", "rarity",
    "securityStamp", "subtypes", "supertypes", "types", "watermark",
}

NESTED_CARD_COLS = {"identifiers", "legalities", "rulings", "foreignData", "purchaseUrls"}


def _get_meta() -> dict[str, str]:
    """Get current metadata dict."""
    meta = MtgjsonMetaObject()
    return {"date": meta.date, "version": meta.version}


# -----------------------------------------------------------------------------
# Core Write Functions
# -----------------------------------------------------------------------------

def write_json(path: Path | str, data: Any, pretty: bool = False, with_hash: bool = True) -> Path:
    """
    Write JSON with {meta, data} wrapper.

    Args:
        path: Output file path
        data: Data to write (will be wrapped in {meta, data})
        pretty: Format with indentation
        with_hash: Write .sha256 hash file

    Returns:
        Path to written file
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    opts = orjson.OPT_SORT_KEYS | (orjson.OPT_INDENT_2 if pretty else 0)

    with path.open("wb") as f:
        f.write(orjson.dumps({"meta": _get_meta(), "data": data}, option=opts))

    if with_hash:
        with path.open("rb") as f:
            Path(f"{path}.sha256").write_text(hashlib.sha256(f.read()).hexdigest())

    return path


def write_set(
    path: Path | str,
    code: str,
    cards_df: pl.DataFrame,
    meta: dict[str, Any],
    tokens_df: pl.DataFrame | None = None,
    sealed: list[dict] | None = None,
    decks: list[dict] | None = None,
    booster: dict | None = None,
) -> Path:
    """
    Write a single set JSON file.

    Args:
        path: Output file path
        code: Set code (e.g., "LEA", "MH3")
        cards_df: DataFrame with card data
        meta: Set metadata dict
        tokens_df: Optional tokens DataFrame
        sealed: Optional sealed products list
        decks: Optional decks list
        booster: Optional booster config dict

    Returns:
        Path to written file
    """
    cards = _df_to_clean_dicts(cards_df)
    tokens = _df_to_clean_dicts(tokens_df) if tokens_df is not None else []

    set_obj: dict[str, Any] = {
        "baseSetSize": meta.get("baseSetSize") or len(cards),
        "cards": cards,
        "code": code,
        "isFoilOnly": meta.get("isFoilOnly", False),
        "isOnlineOnly": meta.get("isOnlineOnly", False),
        "keyruneCode": meta.get("keyruneCode", code),
        "languages": _get_languages(cards_df),
        "name": meta.get("name", code),
        "releaseDate": meta.get("releaseDate", ""),
        "tokens": tokens,
        "totalSetSize": meta.get("totalSetSize") or len(cards),
        "translations": meta.get("translations", {}),
        "type": meta.get("type", ""),
    }

    if booster:
        set_obj["booster"] = booster
    if sealed:
        set_obj["sealedProduct"] = sealed
    if decks:
        set_obj["decks"] = decks

    for k in ("mtgoCode", "parentCode", "block", "tcgplayerGroupId", "tokenSetCode",
              "cardsphereSetId", "mcmId", "mcmName", "isNonFoilOnly"):
        if meta.get(k):
            set_obj[k] = meta[k]

    return write_json(path, deep_sort_keys(set_obj))


def write_all_printings(
    path: Path | str,
    sets_iter: Iterator[tuple[str, dict, pl.DataFrame, pl.DataFrame | None]],
) -> Path:
    """
    Stream AllPrintings.json without loading all sets into memory.

    Args:
        path: Output file path
        sets_iter: Iterator yielding (code, metadata, cards_df, tokens_df)

    Returns:
        Path to written file
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("wb") as f:
        f.write(b'{"meta":')
        f.write(orjson.dumps(_get_meta()))
        f.write(b',"data":{')

        first = True
        for code, meta, cards_df, tokens_df in sets_iter:
            if not first:
                f.write(b",")
            first = False

            cards = _df_to_clean_dicts(cards_df)
            tokens = _df_to_clean_dicts(tokens_df) if tokens_df is not None else []

            set_obj = {
                "baseSetSize": meta.get("baseSetSize") or len(cards),
                "cards": cards,
                "code": code,
                "isFoilOnly": meta.get("isFoilOnly", False),
                "isOnlineOnly": meta.get("isOnlineOnly", False),
                "keyruneCode": meta.get("keyruneCode", code),
                "languages": _get_languages(cards_df),
                "name": meta.get("name", code),
                "releaseDate": meta.get("releaseDate", ""),
                "tokens": tokens,
                "totalSetSize": meta.get("totalSetSize") or len(cards),
                "translations": meta.get("translations", {}),
                "type": meta.get("type", ""),
            }

            if meta.get("booster"):
                booster = meta["booster"]
                if isinstance(booster, str):
                    try:
                        booster = json.loads(booster)
                    except json.JSONDecodeError:
                        booster = None
                if booster:
                    set_obj["booster"] = booster

            for k in ("mtgoCode", "parentCode", "block", "tcgplayerGroupId", "tokenSetCode",
                      "cardsphereSetId", "mcmId", "mcmName", "isNonFoilOnly"):
                if meta.get(k):
                    set_obj[k] = meta[k]

            f.write(f'"{code}":'.encode())
            f.write(orjson.dumps(deep_sort_keys(set_obj), option=orjson.OPT_SORT_KEYS))

        f.write(b"}}")

    return path


def write_deck(
    path: Path | str,
    deck: dict[str, Any],
    pretty: bool = False,
) -> Path:
    """
    Write a single deck JSON file.

    Args:
        path: Output file path
        deck: Deck data dict
        pretty: Format with indentation

    Returns:
        Path to written file
    """
    deck_data = {k: v for k, v in deck.items() if k != "setCode"}
    deck_data = clean_nested(deck_data, omit_empty=True)
    return write_json(path, deep_sort_keys(deck_data), pretty=pretty)


# -----------------------------------------------------------------------------
# Table Export Functions
# -----------------------------------------------------------------------------

def write_parquet(path: Path | str, tables: dict[str, pl.DataFrame]) -> dict[str, Path]:
    """Write tables as Parquet files with zstd compression."""
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    results = {}
    for name, df in tables.items():
        if len(df) == 0:
            continue
        out_path = path / f"{name}.parquet"
        df.write_parquet(out_path, compression="zstd", compression_level=9)
        results[name] = out_path
        LOGGER.info(f"  {name}.parquet: {len(df):,} rows")
    return results


def write_csv(path: Path | str, tables: dict[str, pl.DataFrame]) -> dict[str, Path]:
    """Write tables as CSV files."""
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    results = {}
    for name, df in tables.items():
        if len(df) == 0:
            continue
        out_path = path / f"{name}.csv"
        _serialize_complex(df).write_csv(out_path)
        results[name] = out_path
        LOGGER.info(f"  {name}.csv: {len(df):,} rows")
    return results


def write_sqlite(path: Path | str, tables: dict[str, pl.DataFrame]) -> Path:
    """Write tables to SQLite database."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()

    con = sqlite3.connect(str(path))

    for name, df in tables.items():
        if len(df) == 0:
            continue
        sdf = _serialize_complex(df)

        cols = ", ".join(f'"{c}" TEXT' for c in sdf.columns)
        con.execute(f'CREATE TABLE "{name}" ({cols})')

        placeholders = ", ".join("?" * len(sdf.columns))
        for batch in _batched(sdf.rows(), 10000):
            con.executemany(f'INSERT INTO "{name}" VALUES ({placeholders})', batch)

        LOGGER.info(f"  {name}: {len(sdf):,} rows")

    # Create indexes
    for idx, tbl, col in [("uuid", "cards", "uuid"), ("name", "cards", "name"), ("setCode", "cards", "setCode")]:
        try:
            con.execute(f'CREATE INDEX "idx_{tbl}_{idx}" ON "{tbl}"("{col}")')
        except Exception:
            pass

    con.commit()
    con.close()
    return path


def write_sql(path: Path | str, tables: dict[str, pl.DataFrame]) -> Path:
    """Write SQLite-compatible SQL text dump."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as f:
        f.write(f"-- MTGJSON SQLite Dump\n-- Generated: {datetime.now().strftime('%Y-%m-%d')}\n")
        f.write("BEGIN TRANSACTION;\n\n")

        for name, df in tables.items():
            if len(df) == 0:
                continue
            sdf = _serialize_complex(df)

            cols = ", ".join(f'"{c}" TEXT' for c in sdf.columns)
            f.write(f'CREATE TABLE IF NOT EXISTS "{name}" ({cols});\n')

            for row in sdf.rows():
                vals = ", ".join(_sql_escape(v) for v in row)
                f.write(f'INSERT INTO "{name}" VALUES ({vals});\n')
            f.write("\n")
            LOGGER.info(f"  {name}: {len(sdf):,} rows")

        f.write('CREATE INDEX IF NOT EXISTS "idx_cards_uuid" ON "cards"("uuid");\n')
        f.write('CREATE INDEX IF NOT EXISTS "idx_cards_name" ON "cards"("name");\n')
        f.write('CREATE INDEX IF NOT EXISTS "idx_cards_setCode" ON "cards"("setCode");\n')
        f.write("COMMIT;\n")

    return path


def write_psql(path: Path | str, tables: dict[str, pl.DataFrame]) -> Path:
    """Write PostgreSQL dump (COPY format)."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as f:
        f.write(f"-- MTGJSON PostgreSQL Dump\n-- Generated: {datetime.now().strftime('%Y-%m-%d')}\n")
        f.write("BEGIN;\n\n")

        for name, df in tables.items():
            if len(df) == 0:
                continue
            sdf = _serialize_complex(df)

            cols = ",\n    ".join(f'"{c}" TEXT' for c in sdf.columns)
            f.write(f'DROP TABLE IF EXISTS "{name}" CASCADE;\n')
            f.write(f'CREATE TABLE "{name}" (\n    {cols}\n);\n\n')

            col_names = ", ".join(f'"{c}"' for c in sdf.columns)
            f.write(f'COPY "{name}" ({col_names}) FROM stdin;\n')

            for row in sdf.rows():
                f.write("\t".join(_pg_escape(v) for v in row) + "\n")
            f.write("\\.\n\n")
            LOGGER.info(f"  {name}: {len(sdf):,} rows")

        f.write('CREATE INDEX IF NOT EXISTS "idx_cards_uuid" ON "cards"("uuid");\n')
        f.write('CREATE INDEX IF NOT EXISTS "idx_cards_name" ON "cards"("name");\n')
        f.write('CREATE INDEX IF NOT EXISTS "idx_cards_setCode" ON "cards"("setCode");\n')
        f.write("COMMIT;\n")

    return path


# -----------------------------------------------------------------------------
# Table Builders
# -----------------------------------------------------------------------------

def build_tables(cards_df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    """
    Build normalized tables from cards DataFrame.

    Returns:
        Dict with tables: cards, cardIdentifiers, cardLegalities, cardRulings, cardForeignData
    """
    schema = cards_df.schema

    tables = {
        "cards": cards_df.select([c for c in cards_df.columns if c not in NESTED_CARD_COLS])
    }

    # Identifiers - unnest struct
    if "identifiers" in schema and isinstance(schema["identifiers"], pl.Struct):
        tables["cardIdentifiers"] = (
            cards_df.select("uuid", "identifiers")
            .filter(pl.col("identifiers").is_not_null())
            .unnest("identifiers")
        )

    # Legalities - unpivot to long format
    if "legalities" in schema and isinstance(schema["legalities"], pl.Struct):
        formats = [f.name for f in schema["legalities"].fields]
        tables["cardLegalities"] = (
            cards_df.select("uuid", *[pl.col("legalities").struct.field(f).alias(f) for f in formats])
            .unpivot(index="uuid", on=formats, variable_name="format", value_name="status")
            .filter(pl.col("status").is_not_null())
        )

    # Rulings - explode list of structs
    if "rulings" in schema and isinstance(schema["rulings"], pl.List):
        tables["cardRulings"] = (
            cards_df.select("uuid", "rulings")
            .filter(pl.col("rulings").list.len() > 0)
            .explode("rulings")
            .unnest("rulings")
        )

    # ForeignData - explode list of structs
    if "foreignData" in schema and isinstance(schema["foreignData"], pl.List):
        tables["cardForeignData"] = (
            cards_df.select("uuid", "foreignData")
            .filter(pl.col("foreignData").list.len() > 0)
            .explode("foreignData")
            .unnest("foreignData")
        )

    # PurchaseUrls - unnest struct
    if "purchaseUrls" in schema and isinstance(schema["purchaseUrls"], pl.Struct):
        tables["cardPurchaseUrls"] = (
            cards_df.select("uuid", "purchaseUrls")
            .filter(pl.col("purchaseUrls").is_not_null())
            .unnest("purchaseUrls")
        )

    return tables


def build_booster_tables(booster_df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    """
    Parse booster JSON configs into relational tables.

    Args:
        booster_df: DataFrame with setCode and config columns

    Returns:
        Dict with tables: setBoosterSheets, setBoosterSheetCards,
        setBoosterContents, setBoosterContentWeights
    """
    sheets_records = []
    sheet_cards_records = []
    contents_records = []
    weights_records = []

    for row in booster_df.iter_rows(named=True):
        set_code = row["setCode"]
        config_str = row.get("config")
        if not config_str:
            continue

        try:
            config = json.loads(config_str) if isinstance(config_str, str) else config_str
        except json.JSONDecodeError:
            continue

        for booster_name, booster_data in config.items():
            if not isinstance(booster_data, dict):
                continue

            # Sheets
            for sheet_name, sheet_data in booster_data.get("sheets", {}).items():
                if not isinstance(sheet_data, dict):
                    continue

                sheets_records.append({
                    "setCode": set_code,
                    "boosterName": booster_name,
                    "sheetName": sheet_name,
                    "sheetIsFoil": sheet_data.get("foil", False),
                    "sheetHasBalanceColors": sheet_data.get("balanceColors", False),
                    "sheetTotalWeight": sheet_data.get("totalWeight", 0),
                })

                for card_uuid, weight in sheet_data.get("cards", {}).items():
                    sheet_cards_records.append({
                        "setCode": set_code,
                        "boosterName": booster_name,
                        "sheetName": sheet_name,
                        "cardUuid": card_uuid,
                        "cardWeight": weight,
                    })

            # Boosters
            for idx, variant in enumerate(booster_data.get("boosters", [])):
                if not isinstance(variant, dict):
                    continue

                weights_records.append({
                    "setCode": set_code,
                    "boosterName": booster_name,
                    "boosterIndex": idx,
                    "boosterWeight": variant.get("weight", 1),
                })

                for sheet_name, picks in variant.get("contents", {}).items():
                    contents_records.append({
                        "setCode": set_code,
                        "boosterName": booster_name,
                        "boosterIndex": idx,
                        "sheetName": sheet_name,
                        "sheetPicks": picks,
                    })

    return {
        "setBoosterSheets": pl.DataFrame(sheets_records) if sheets_records else pl.DataFrame(),
        "setBoosterSheetCards": pl.DataFrame(sheet_cards_records) if sheet_cards_records else pl.DataFrame(),
        "setBoosterContents": pl.DataFrame(contents_records) if contents_records else pl.DataFrame(),
        "setBoosterContentWeights": pl.DataFrame(weights_records) if weights_records else pl.DataFrame(),
    }


# -----------------------------------------------------------------------------
# Compiled Output Builders
# -----------------------------------------------------------------------------

def build_all_identifiers(cards_df: pl.DataFrame) -> dict[str, dict]:
    """Build {uuid: card_object} mapping."""
    return {c["uuid"]: _clean(c) for c in cards_df.to_dicts() if c.get("uuid")}


def build_atomic_cards(cards_df: pl.DataFrame, format_filter: str | None = None) -> dict[str, list[dict]]:
    """
    Build {card_name: [atomic_variants]} mapping.

    Args:
        cards_df: Cards DataFrame
        format_filter: Optional format to filter by (standard, modern, etc.)
    """
    if format_filter and "legalities" in cards_df.columns:
        cards_df = cards_df.filter(
            pl.col("legalities").struct.field(format_filter).is_in(["Legal", "Restricted"])
        )

    available = [f for f in ATOMIC_FIELDS if f in cards_df.columns]
    result: dict[str, list[dict]] = {}
    seen: dict[str, set] = {}

    for card in cards_df.select(available + ["setCode", "isReprint"]).to_dicts():
        name = card.get("name", "").split(" (")[0]  # Strip (a), (b) suffixes
        if not name:
            continue

        atomic = {k: v for k, v in card.items() if k in ATOMIC_FIELDS and v is not None}

        # Only keep scryfallOracleId in identifiers
        if "identifiers" in atomic and isinstance(atomic["identifiers"], dict):
            oid = atomic["identifiers"].get("scryfallOracleId")
            atomic["identifiers"] = {"scryfallOracleId": oid} if oid else {}

        text = atomic.get("text")
        if name not in seen:
            seen[name] = set()
            result[name] = []

        if text not in seen[name]:
            seen[name].add(text)
            if not card.get("isReprint") and card.get("setCode"):
                atomic["firstPrinting"] = card["setCode"]
            result[name].append(atomic)

    return result


def build_set_list(sets_meta: dict[str, dict]) -> list[dict]:
    """Build [{set_metadata}, ...] without cards/tokens."""
    exclude = {"cards", "tokens", "booster", "decks", "sealedProduct"}
    return sorted(
        [{k: v for k, v in m.items() if k not in exclude} for m in sets_meta.values()],
        key=lambda x: x.get("name", "")
    )


def build_deck_list(decks_df: pl.DataFrame) -> list[dict]:
    """Build deck headers list."""
    cols = [c for c in ["code", "name", "releaseDate", "type"] if c in decks_df.columns]
    return decks_df.select(cols).to_dicts()


def build_enum_values(cards_df: pl.DataFrame) -> dict[str, dict[str, list]]:
    """Build unique values for enumerable fields."""
    result: dict[str, list] = {}
    schema = cards_df.schema

    for fld in CARD_ENUM_FIELDS:
        if fld not in schema:
            continue
        try:
            if isinstance(schema[fld], pl.List):
                vals = cards_df.select(pl.col(fld).explode()).unique().to_series().drop_nulls().to_list()
            else:
                vals = cards_df.select(fld).unique().to_series().drop_nulls().to_list()
            result[fld] = sorted(str(v) for v in vals)
        except Exception:
            continue

    return {"card": result}


def build_compiled_list() -> list[str]:
    """List of compiled output file names."""
    return sorted([
        "AllPrintings", "AllIdentifiers", "AtomicCards", "CardTypes", "CompiledList",
        "DeckList", "EnumValues", "Keywords", "Legacy", "LegacyAtomic", "Meta",
        "Modern", "ModernAtomic", "PauperAtomic", "Pioneer", "PioneerAtomic",
        "SetList", "Standard", "StandardAtomic", "TcgplayerSkus", "Vintage", "VintageAtomic",
    ])


# -----------------------------------------------------------------------------
# Set Metadata Builder
# -----------------------------------------------------------------------------

def build_set_metadata(
    ctx: PipelineContext,
) -> dict[str, dict[str, Any]]:
    """
    Build set metadata dict from context.

    Returns:
        Dict mapping set code to metadata dict
    """
    sets_lf = ctx.sets_df
    if sets_lf is None:
        return {}

    if not isinstance(sets_lf, pl.LazyFrame):
        sets_lf = sets_lf.lazy()

    # Get booster configs
    booster_lf = ctx.boosters_df
    if booster_lf is not None:
        if not isinstance(booster_lf, pl.LazyFrame):
            booster_lf = booster_lf.lazy()
    else:
        booster_lf = pl.DataFrame({"setCode": [], "config": []}).lazy()

    available_cols = sets_lf.collect_schema().names()

    # Build base expressions
    base_exprs = [
        pl.col("code").str.to_uppercase().alias("code"),
        pl.col("name"),
        pl.col("releasedAt").alias("releaseDate"),
        pl.col("setType").alias("type"),
        pl.col("digital").alias("isOnlineOnly"),
        pl.col("foilOnly").alias("isFoilOnly"),
    ]

    # Add optional columns
    optional_mappings = [
        ("mtgoCode", "mtgoCode", lambda c: pl.col(c).str.to_uppercase()),
        ("tcgplayerId", "tcgplayerGroupId", None),
        ("nonfoilOnly", "isNonFoilOnly", None),
        ("parentSetCode", "parentCode", lambda c: pl.col(c).str.to_uppercase()),
        ("block", "block", None),
        ("cardCount", "totalSetSize", None),
        ("printedSize", "baseSetSize", None),
    ]

    for src_col, dst_col, transform in optional_mappings:
        if src_col in available_cols:
            expr = transform(src_col) if transform else pl.col(src_col)
            base_exprs.append(expr.alias(dst_col))

    # Keyrune code from icon URL
    if "iconSvgUri" in available_cols:
        base_exprs.append(
            pl.col("iconSvgUri")
            .str.extract(r"/([^/]+)\.svg", 1)
            .str.to_uppercase()
            .alias("keyruneCode")
        )

    # Token set code
    token_expr = (
        pl.when(pl.col("code").str.starts_with("T"))
        .then(pl.col("code").str.to_uppercase())
        .otherwise(pl.lit("T") + pl.col("code").str.to_uppercase())
    )
    if "tokenSetCode" in available_cols:
        base_exprs.append(pl.coalesce(pl.col("tokenSetCode"), token_expr).alias("tokenSetCode"))
    else:
        base_exprs.append(token_expr.alias("tokenSetCode"))

    set_meta = sets_lf.select(base_exprs)

    # Join booster configs
    set_meta = set_meta.join(
        booster_lf.with_columns(pl.col("setCode").str.to_uppercase().alias("code")),
        on="code",
        how="left",
    ).rename({"config": "booster"})

    # Collect and add translations
    set_meta_df = set_meta.collect()
    records = set_meta_df.to_dicts()

    translations = _load_translations()
    cardsphere_sets = ctx.multiverse_bridge_sets if ctx.multiverse_bridge_sets else {}

    for record in records:
        set_name = record.get("name", "")
        set_code = record.get("code", "")

        record["cardsphereSetId"] = cardsphere_sets.get(set_code.upper())
        record["translations"] = translations.get(
            set_name,
            {lang: None for lang in constants.LANGUAGE_MAP.values()},
        )

        if record.get("baseSetSize") is None:
            record["baseSetSize"] = record.get("totalSetSize", 0)
        if record.get("totalSetSize") is None:
            record["totalSetSize"] = record.get("baseSetSize", 0)

    return {r["code"]: r for r in records}


def _load_translations() -> dict[str, dict[str, str | None]]:
    """Load set translations from resource file."""
    translations_path = constants.RESOURCE_PATH / "mkm_set_name_translations.json"
    if not translations_path.exists():
        return {}

    with translations_path.open(encoding="utf-8") as f:
        raw = json.load(f)

    result = {}
    for set_name, langs in raw.items():
        result[set_name] = {
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
    return result


# -----------------------------------------------------------------------------
# Deck Builder
# -----------------------------------------------------------------------------

def expand_card_list(
    decks: pl.DataFrame,
    cards_df: pl.DataFrame,
    col: str,
) -> pl.DataFrame:
    """
    Expand a deck card list column by joining with full card data.

    Takes deck DataFrame with _deck_id and a card list column containing
    [{uuid, count, isFoil, isEtched}, ...] and expands each reference to
    a full card object.
    """
    if col not in decks.columns:
        return decks.select("_deck_id").with_columns(pl.lit([]).alias(col))

    exploded = (
        decks.select(["_deck_id", col]).explode(col).filter(pl.col(col).is_not_null())
    )

    if len(exploded) == 0:
        return decks.select("_deck_id").unique().with_columns(pl.lit([]).alias(col))

    exploded = exploded.with_columns(
        pl.col(col).struct.field("uuid").alias("_ref_uuid"),
        pl.col(col).struct.field("count"),
        pl.col(col).struct.field("isFoil"),
        pl.col(col).struct.field("isEtched"),
    ).drop(col)

    joined = exploded.join(
        cards_df,
        left_on="_ref_uuid",
        right_on="uuid",
        how="left",
    ).with_columns(pl.col("_ref_uuid").alias("uuid"))

    card_cols = [c for c in joined.columns if c not in ("_deck_id", "_ref_uuid")]

    result = joined.group_by("_deck_id").agg(pl.struct(card_cols).alias(col))

    all_deck_ids = decks.select("_deck_id").unique()
    return all_deck_ids.join(result, on="_deck_id", how="left").with_columns(
        pl.col(col).fill_null([])
    )


def build_decks_expanded(
    ctx: PipelineContext,
    set_codes: list[str] | str | None = None,
) -> pl.DataFrame:
    """
    Build decks DataFrame with fully expanded card objects.

    Unlike minimal {count, uuid} references, this function joins with card data
    to produce complete card objects in each deck's card lists.
    """
    if ctx.decks_df is None:
        LOGGER.warning("GitHub decks data not loaded in cache")
        return pl.DataFrame()

    # Filter decks by set codes
    filter_codes = set_codes or ctx.sets_to_build
    decks_lf = ctx.decks_df
    if filter_codes:
        if isinstance(filter_codes, str):
            decks_lf = decks_lf.filter(pl.col("setCode") == filter_codes.upper())
        else:
            upper_codes = [s.upper() for s in filter_codes]
            decks_lf = decks_lf.filter(pl.col("setCode").is_in(upper_codes))

    decks_df = decks_lf.collect()

    if len(decks_df) == 0:
        return pl.DataFrame()

    # Collect all UUIDs referenced in decks
    all_uuids: set[str] = set()
    for col in ["mainBoard", "sideBoard", "commander", "tokens"]:
        if col in decks_df.columns:
            for card_list in decks_df[col].to_list():
                if card_list:
                    for card_ref in card_list:
                        if isinstance(card_ref, dict) and card_ref.get("uuid"):
                            all_uuids.add(card_ref["uuid"])

    if not all_uuids:
        return pl.DataFrame()

    # Load cards filtered by UUIDs
    parquet_dir = constants.CACHE_PATH / "_parquet"
    if not parquet_dir.exists():
        return pl.DataFrame()

    cards_df = pl.scan_parquet(parquet_dir / "**/*.parquet").filter(
        pl.col("uuid").is_in(list(all_uuids))
    ).collect()

    # Add deck ID for re-aggregation
    decks_df = decks_df.with_row_index("_deck_id")

    # Expand card lists
    available_cols = decks_df.columns
    expanded_lists = {}
    for col in ["mainBoard", "sideBoard", "commander", "tokens"]:
        expanded_lists[col] = expand_card_list(decks_df, cards_df, col)

    # Build result with deck metadata
    result = decks_df.select(
        "_deck_id",
        "setCode",
        pl.col("setCode").alias("code"),
        "name",
        "type",
        pl.col("releaseDate") if "releaseDate" in available_cols else pl.lit(None).cast(pl.String).alias("releaseDate"),
        pl.col("sealedProductUuids") if "sealedProductUuids" in available_cols else pl.lit(None).cast(pl.List(pl.String)).alias("sealedProductUuids"),
        pl.col("sourceSetCodes").fill_null([]) if "sourceSetCodes" in available_cols else pl.lit([]).cast(pl.List(pl.String)).alias("sourceSetCodes"),
        pl.col("displayCommander").fill_null([]) if "displayCommander" in available_cols else pl.lit([]).cast(pl.List(pl.String)).alias("displayCommander"),
        pl.col("planes").fill_null([]) if "planes" in available_cols else pl.lit([]).cast(pl.List(pl.String)).alias("planes"),
        pl.col("schemes").fill_null([]) if "schemes" in available_cols else pl.lit([]).cast(pl.List(pl.String)).alias("schemes"),
    )

    # Join expanded card lists
    for col in ["mainBoard", "sideBoard", "commander", "tokens"]:
        result = result.join(expanded_lists[col], on="_deck_id", how="left")

    return result.drop("_deck_id")


# -----------------------------------------------------------------------------
# Parquet Iteration
# -----------------------------------------------------------------------------

def iter_sets_from_parquet(
    parquet_dir: Path | str,
    sets_metadata: dict[str, dict],
    tokens_dir: Path | str | None = None,
) -> Iterator[tuple[str, dict, pl.DataFrame, pl.DataFrame | None]]:
    """
    Iterate sets from hive-partitioned parquet.

    Yields: (code, metadata, cards_df, tokens_df)
    """
    parquet_dir = Path(parquet_dir)
    tokens_dir = Path(tokens_dir) if tokens_dir else parquet_dir.parent / "_parquet_tokens"

    for set_dir in sorted(parquet_dir.iterdir()):
        if not set_dir.is_dir() or not set_dir.name.startswith("setCode="):
            continue
        if set_dir.name.startswith("setCode=T"):
            continue

        code = set_dir.name.replace("setCode=", "")
        cards_df = pl.read_parquet(set_dir / "*.parquet")

        tokens_df = None
        token_code = sets_metadata.get(code, {}).get("tokenSetCode", f"T{code}")
        token_path = tokens_dir / f"setCode={token_code}"
        if token_path.exists():
            tokens_df = pl.read_parquet(token_path / "*.parquet")

        yield code, sets_metadata.get(code, {"code": code, "name": code}), cards_df, tokens_df


# -----------------------------------------------------------------------------
# Unified Generation
# -----------------------------------------------------------------------------

def generate_all(
    parquet_dir: Path | str,
    output_dir: Path | str,
    sets_metadata: dict[str, dict],
    tokens_dir: Path | str | None = None,
    formats: list[str] | None = None,
    write_set_files: bool = True,
) -> dict[str, Path]:
    """
    Generate all outputs from parquet cache in a single pass.

    Args:
        parquet_dir: Path to hive-partitioned parquet data
        output_dir: Output directory
        sets_metadata: Dict mapping set code to metadata
        tokens_dir: Optional separate tokens parquet directory
        formats: List of formats to generate (default: all)
        write_set_files: Whether to write individual {SET}.json files

    Returns:
        Dict mapping output name to path
    """
    parquet_dir = Path(parquet_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    formats = formats or ["json", "parquet", "csv", "sqlite", "sql", "psql"]
    results: dict[str, Any] = {}
    needs_tables = bool({"parquet", "csv", "sqlite", "sql", "psql"} & set(formats))
    write_all_printings_json = "json" in formats

    # Single pass: write AllPrintings.json AND individual set files together
    if write_all_printings_json or write_set_files:
        LOGGER.info("Writing JSON outputs (single pass)...")
        all_printings_path = output_dir / "AllPrintings.json"

        with all_printings_path.open("wb") as ap_file:
            ap_file.write(b'{"meta":')
            ap_file.write(orjson.dumps(_get_meta()))
            ap_file.write(b',"data":{')

            first = True
            for code, meta, cards_df, tokens_df in iter_sets_from_parquet(parquet_dir, sets_metadata, tokens_dir):
                # Build set object once, reuse for both outputs
                cards = _df_to_clean_dicts(cards_df)
                tokens = _df_to_clean_dicts(tokens_df) if tokens_df is not None else []
                set_obj = _build_set_object(code, meta, cards, tokens)

                # Write to AllPrintings stream
                if write_all_printings_json:
                    if not first:
                        ap_file.write(b",")
                    first = False
                    ap_file.write(f'"{code}":'.encode())
                    ap_file.write(orjson.dumps(set_obj, option=orjson.OPT_SORT_KEYS))

                # Write individual set file
                if write_set_files:
                    set_path = output_dir / f"{code}.json"
                    with set_path.open("wb") as f:
                        f.write(orjson.dumps({"meta": _get_meta(), "data": set_obj}, option=orjson.OPT_SORT_KEYS))
                    with set_path.open("rb") as f:
                        Path(f"{set_path}.sha256").write_text(hashlib.sha256(f.read()).hexdigest())
                    results[code] = set_path

            ap_file.write(b"}}")

        if write_all_printings_json:
            results["AllPrintings"] = all_printings_path

    # Load cards once for table/compiled outputs (only if needed)
    cards_df = None
    if needs_tables or "json" in formats:
        cards_df = pl.read_parquet(parquet_dir / "**/*.parquet")

    # Compiled JSON outputs
    if "json" in formats and cards_df is not None:
        LOGGER.info("Writing compiled JSON files...")
        results["AllIdentifiers"] = write_json(output_dir / "AllIdentifiers.json", build_all_identifiers(cards_df), with_hash=False)
        results["AtomicCards"] = write_json(output_dir / "AtomicCards.json", build_atomic_cards(cards_df), with_hash=False)
        results["SetList"] = write_json(output_dir / "SetList.json", build_set_list(sets_metadata), with_hash=False)
        results["EnumValues"] = write_json(output_dir / "EnumValues.json", build_enum_values(cards_df), with_hash=False)
        results["CompiledList"] = write_json(output_dir / "CompiledList.json", build_compiled_list(), with_hash=False)
        results["Meta"] = write_json(output_dir / "Meta.json", _get_meta(), with_hash=False)

        for fmt in ("standard", "pioneer", "modern", "legacy", "vintage", "pauper"):
            atomic = build_atomic_cards(cards_df, fmt)
            if atomic:
                results[f"{fmt.capitalize()}Atomic"] = write_json(
                    output_dir / f"{fmt.capitalize()}Atomic.json", atomic, with_hash=False
                )

    # Build tables once, reuse for all SQL formats
    if needs_tables and cards_df is not None:
        LOGGER.info("Building normalized tables...")
        tables = build_tables(cards_df)

        if "parquet" in formats:
            LOGGER.info("Writing parquet tables...")
            results["parquet"] = write_parquet(output_dir / "parquet", tables)

        if "csv" in formats:
            LOGGER.info("Writing CSV tables...")
            results["csv"] = write_csv(output_dir / "csv", tables)

        if "sqlite" in formats:
            LOGGER.info("Writing SQLite database...")
            results["sqlite"] = write_sqlite(output_dir / "AllPrintings.sqlite", tables)

        if "sql" in formats:
            LOGGER.info("Writing SQL dump...")
            results["sql"] = write_sql(output_dir / "AllPrintings.sql", tables)

        if "psql" in formats:
            LOGGER.info("Writing PostgreSQL dump...")
            results["psql"] = write_psql(output_dir / "AllPrintings.psql", tables)

    return results


def _build_set_object(code: str, meta: dict, cards: list[dict], tokens: list[dict]) -> dict[str, Any]:
    """Build a set object dict (shared by single-pass generation)."""
    set_obj: dict[str, Any] = {
        "baseSetSize": meta.get("baseSetSize") or len(cards),
        "cards": cards,
        "code": code,
        "isFoilOnly": meta.get("isFoilOnly", False),
        "isOnlineOnly": meta.get("isOnlineOnly", False),
        "keyruneCode": meta.get("keyruneCode", code),
        "languages": _get_languages_from_cards(cards),
        "name": meta.get("name", code),
        "releaseDate": meta.get("releaseDate", ""),
        "tokens": tokens,
        "totalSetSize": meta.get("totalSetSize") or len(cards),
        "translations": meta.get("translations", {}),
        "type": meta.get("type", ""),
    }

    if meta.get("booster"):
        booster = meta["booster"]
        if isinstance(booster, str):
            try:
                booster = json.loads(booster)
            except json.JSONDecodeError:
                booster = None
        if booster:
            set_obj["booster"] = booster

    for k in ("mtgoCode", "parentCode", "block", "tcgplayerGroupId", "tokenSetCode",
              "cardsphereSetId", "mcmId", "mcmName", "isNonFoilOnly"):
        if meta.get(k):
            set_obj[k] = meta[k]

    return deep_sort_keys(set_obj)


def _get_languages_from_cards(cards: list[dict]) -> list[str]:
    """Extract languages from card foreignData (for pre-converted dicts)."""
    languages_set: set[str] = {"English"}
    for card in cards:
        foreign_data = card.get("foreignData", [])
        if foreign_data:
            for fd in foreign_data:
                if isinstance(fd, dict) and fd.get("language"):
                    languages_set.add(fd["language"])
    return sorted(languages_set)



def _clean(d: dict) -> dict:
    """Remove None values recursively."""
    return {k: (_clean(v) if isinstance(v, dict) else v) for k, v in d.items() if v is not None}


def _df_to_clean_dicts(df: pl.DataFrame | None) -> list[dict]:
    """Convert DataFrame to list of cleaned dicts using serialize module."""
    if df is None or len(df) == 0:
        return []
    return [clean_nested(d) for d in dataframe_to_cards_list(df)]


def _get_languages(cards_df: pl.DataFrame) -> list[str]:
    """Extract languages from foreignData."""
    if "foreignData" not in cards_df.columns:
        return ["English"]
    try:
        langs = (
            cards_df.select(pl.col("foreignData").list.eval(pl.element().struct.field("language")).list.explode())
            .to_series().drop_nulls().unique().to_list()
        )
        return sorted(set(langs) | {"English"})
    except Exception:
        return ["English"]


def _serialize_complex(df: pl.DataFrame) -> pl.DataFrame:
    """Convert Struct/List columns to JSON strings for SQL export."""
    schema = df.schema
    exprs = []
    for col in df.columns:
        if isinstance(schema[col], pl.Struct):
            exprs.append(pl.col(col).struct.json_encode().alias(col))
        elif isinstance(schema[col], pl.List):
            exprs.append(pl.col(col).map_batches(
                lambda s: pl.Series([orjson.dumps(x).decode() if x else None for x in s.to_list()]),
                return_dtype=pl.String
            ).alias(col))
        else:
            exprs.append(pl.col(col))
    return df.select(exprs)


def _sql_escape(v: Any) -> str:
    """Escape value for SQL INSERT."""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (int, float)):
        return str(v)
    s = str(v).replace("'", "''")
    return f"'{s}'"


def _pg_escape(v: Any) -> str:
    """Escape value for PostgreSQL COPY."""
    if v is None:
        return "\\N"
    if isinstance(v, bool):
        return "t" if v else "f"
    if isinstance(v, (int, float)):
        return str(v)
    return str(v).replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")


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


def _collect_referrals(
    cards: list[dict],
    sealed: list[dict],
    regex: re.Pattern,
) -> list[tuple[str, str]]:
    """Collect referral URL entries from cards and sealed products."""
    entries = []
    for item in cards + sealed:
        purchase_urls = item.get("purchaseUrls", {})
        raw_urls = item.get("rawPurchaseUrls", {})
        for service, url in purchase_urls.items():
            if service in raw_urls and url:
                url_id = url.split("/")[-1]
                raw_url = regex.sub("mtgjson", raw_urls[service])
                entries.append((url_id, raw_url))
    return entries
