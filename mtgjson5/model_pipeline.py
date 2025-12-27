"""
Model-based pipeline integration.

Bridges the data pipeline with typed Pydantic models for output generation.
Includes fast-path assembly from cached parquet/metadata files.
"""

from __future__ import annotations

import contextlib
import json
import pathlib
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING, Any

import polars as pl

from mtgjson5 import constants
from mtgjson5.classes import MtgjsonMetaObject
from mtgjson5.mtgjson_config import MtgjsonConfig
from mtgjson5.mtgjson_models import (
    AssemblyContext,
    MtgjsonFileBuilder,
)
from mtgjson5.utils import LOGGER


if TYPE_CHECKING:
    from mtgjson5.context import PipelineContext

# Cache file names for fast-path assembly
CACHE_SET_META = "_assembly_set_meta.json"
CACHE_BOOSTER_CONFIGS = "_assembly_boosters.json"
CACHE_DECKS = "_assembly_decks.parquet"
CACHE_SEALED = "_assembly_sealed.parquet"


def build_assembly_context(ctx: PipelineContext) -> AssemblyContext:
    """
    Build AssemblyContext from PipelineContext.

    Prepares all shared data needed for model-based assembly.
    """
    from mtgjson5.pipeline import build_sealed_products_df, build_set_metadata_df

    parquet_dir = constants.CACHE_PATH / "_parquet"
    tokens_dir = constants.CACHE_PATH / "_parquet_tokens"

    # Build set metadata
    LOGGER.info("Loading set metadata...")
    set_meta_df = build_set_metadata_df(ctx)
    if isinstance(set_meta_df, pl.LazyFrame):
        set_meta_df = set_meta_df.collect()
    set_meta = {row["code"]: row for row in set_meta_df.to_dicts()}

    # Load raw deck data (minimal format for AllPrintings)
    # Note: Use raw ctx.decks_df instead of build_decks_expanded() to keep minimal {count, uuid, isFoil}
    LOGGER.info("Loading deck data...")
    decks_df = ctx.decks_df
    if decks_df is not None:
        if isinstance(decks_df, pl.LazyFrame):
            decks_df = decks_df.collect()

    # Build sealed products
    LOGGER.info("Loading sealed products...")
    sealed_df = build_sealed_products_df(ctx)
    if isinstance(sealed_df, pl.LazyFrame):
        sealed_df = sealed_df.collect()

    # Parse booster configs from JSON strings in metadata
    booster_configs: dict[str, dict] = {}
    for code, meta in set_meta.items():
        booster_raw = meta.get("booster")
        if booster_raw and isinstance(booster_raw, str):
            with contextlib.suppress(json.JSONDecodeError):
                booster_configs[code] = json.loads(booster_raw)
        elif isinstance(booster_raw, dict):
            booster_configs[code] = booster_raw

    # Create meta dict
    meta_obj = MtgjsonMetaObject()
    meta_dict = {"date": meta_obj.date, "version": meta_obj.version}

    return AssemblyContext(
        parquet_dir=parquet_dir,
        tokens_dir=tokens_dir,
        set_meta=set_meta,
        meta=meta_dict,
        decks_df=decks_df,
        sealed_df=sealed_df,
        booster_configs=booster_configs,
    )


def save_assembly_cache(ctx: AssemblyContext, cache_dir: pathlib.Path | None = None) -> None:
    """
    Save assembly context metadata to cache for fast-path loading.

    Saves:
    - set_meta as JSON
    - booster_configs as JSON
    - decks_df as parquet
    - sealed_df as parquet
    """
    if cache_dir is None:
        cache_dir = constants.CACHE_PATH

    start = time.perf_counter()

    # Save set metadata (convert any non-serializable values)
    set_meta_path = cache_dir / CACHE_SET_META
    cleaned_meta: dict[str, Any] = {}
    for code, meta in ctx.set_meta.items():
        cleaned_meta[code] = {
            k: v for k, v in meta.items()
            if v is not None and not isinstance(v, pl.DataFrame | pl.LazyFrame)
        }
    with set_meta_path.open("w", encoding="utf-8") as f:
        json.dump(cleaned_meta, f)

    # Save booster configs
    boosters_path = cache_dir / CACHE_BOOSTER_CONFIGS
    with boosters_path.open("w", encoding="utf-8") as f:
        json.dump(ctx.booster_configs, f)

    # Save decks DataFrame
    if ctx.decks_df is not None and len(ctx.decks_df) > 0:
        decks_path = cache_dir / CACHE_DECKS
        ctx.decks_df.write_parquet(decks_path)

    # Save sealed products DataFrame
    if ctx.sealed_df is not None and len(ctx.sealed_df) > 0:
        sealed_path = cache_dir / CACHE_SEALED
        ctx.sealed_df.write_parquet(sealed_path)

    elapsed = time.perf_counter() - start
    LOGGER.info(f"Saved assembly cache in {elapsed:.2f}s")


def load_assembly_context_from_cache(
    cache_dir: pathlib.Path | None = None,
) -> AssemblyContext | None:
    """
    Load assembly context from cached files (fast path).

    Returns None if cache files don't exist.
    """
    if cache_dir is None:
        cache_dir = constants.CACHE_PATH

    parquet_dir = cache_dir / "_parquet"
    tokens_dir = cache_dir / "_parquet_tokens"
    set_meta_path = cache_dir / CACHE_SET_META
    boosters_path = cache_dir / CACHE_BOOSTER_CONFIGS
    decks_path = cache_dir / CACHE_DECKS
    sealed_path = cache_dir / CACHE_SEALED

    # Check required files exist
    if not set_meta_path.exists():
        LOGGER.warning(f"Assembly cache not found: {set_meta_path}")
        return None
    if not parquet_dir.exists():
        LOGGER.warning(f"Parquet directory not found: {parquet_dir}")
        return None

    start = time.perf_counter()

    # Load set metadata
    with set_meta_path.open("r", encoding="utf-8") as f:
        set_meta: dict[str, dict[str, Any]] = json.load(f)

    # Load booster configs
    booster_configs: dict[str, dict[str, Any]] = {}
    if boosters_path.exists():
        with boosters_path.open("r", encoding="utf-8") as f:
            booster_configs = json.load(f)

    # Load decks DataFrame
    decks_df: pl.DataFrame | None = None
    if decks_path.exists():
        decks_df = pl.read_parquet(decks_path)

    # Load sealed products DataFrame
    sealed_df: pl.DataFrame | None = None
    if sealed_path.exists():
        sealed_df = pl.read_parquet(sealed_path)

    # Create meta dict
    meta_obj = MtgjsonMetaObject()
    meta_dict = {"date": meta_obj.date, "version": meta_obj.version}

    elapsed = time.perf_counter() - start
    LOGGER.info(f"Loaded assembly cache in {elapsed:.2f}s ({len(set_meta)} sets)")

    return AssemblyContext(
        parquet_dir=parquet_dir,
        tokens_dir=tokens_dir,
        set_meta=set_meta,
        meta=meta_dict,
        decks_df=decks_df,
        sealed_df=sealed_df,
        booster_configs=booster_configs,
    )


def assemble_from_cache(
    output_dir: pathlib.Path | None = None,
    cache_dir: pathlib.Path | None = None,
    set_codes: list[str] | None = None,
    streaming: bool = True,
) -> dict[str, int]:
    """
    Assemble MTGJSON output files directly from cached parquet/metadata.

    Fast path that skips the full pipeline. Requires:
    - Parquet card data in cache_dir/_parquet/
    - Assembly metadata from previous build_assembly_context() + save_assembly_cache()

    Args:
        output_dir: Output directory (defaults to config output_path).
        cache_dir: Cache directory with parquet data (defaults to CACHE_PATH).
        set_codes: Optional list of set codes to build. If None, builds all.
        streaming: Use streaming for large files.

    Returns:
        Dict mapping file names to record counts.
    """
    assembly_ctx = load_assembly_context_from_cache(cache_dir)
    if assembly_ctx is None:
        raise RuntimeError(
            "Assembly cache not found. Run a full build first, "
            "or use assemble_with_models() instead."
        )

    if output_dir is None:
        output_dir = MtgjsonConfig().output_path
    output_dir.mkdir(parents=True, exist_ok=True)

    builder = MtgjsonFileBuilder(assembly_ctx)
    results: dict[str, int] = {}

    # Filter set codes if provided
    if set_codes:
        valid_codes = [
            code for code in set_codes
            if code in assembly_ctx.set_meta
        ]
    else:
        valid_codes = None

    # Build AllPrintings (streaming for memory efficiency)
    LOGGER.info("Building AllPrintings.json...")
    all_printings = builder.build_all_printings(
        output_dir / "AllPrintings.json",
        set_codes=valid_codes,
        streaming=streaming,
    )
    results["AllPrintings"] = all_printings if isinstance(all_printings, int) else len(all_printings.data)

    # Build AtomicCards
    LOGGER.info("Building AtomicCards.json...")
    atomic_cards = builder.build_atomic_cards(output_dir / "AtomicCards.json")
    results["AtomicCards"] = len(atomic_cards.data)

    # Build SetList
    LOGGER.info("Building SetList.json...")
    set_list = builder.build_set_list(output_dir / "SetList.json")
    results["SetList"] = len(set_list.data)

    # Build format-specific files
    LOGGER.info("Building format-specific files...")
    if streaming:
        from mtgjson5.mtgjson_models import AllPrintingsFile
        all_printings = AllPrintingsFile.read(output_dir / "AllPrintings.json")

    for fmt in ["legacy", "modern", "pioneer", "standard", "vintage"]:
        fmt_file = builder.build_format_file(
            all_printings, fmt, output_dir / f"{fmt.title()}.json"
        )
        results[fmt.title()] = len(fmt_file.data)

    for fmt in ["legacy", "modern", "pauper", "pioneer", "standard", "vintage"]:
        atomic_file = builder.build_format_atomic(
            atomic_cards, fmt, output_dir / f"{fmt.title()}Atomic.json"
        )
        results[f"{fmt.title()}Atomic"] = len(atomic_file.data)

    # Build individual set files
    LOGGER.info("Building individual set files...")
    set_count = _build_individual_sets(builder, output_dir, valid_codes)
    results["sets"] = set_count

    return results


def assemble_with_models(
    ctx: PipelineContext,
    output_dir: pathlib.Path | None = None,
    streaming: bool = True,
    save_cache: bool = True,
) -> dict[str, int]:
    """
    Assemble all MTGJSON output files using typed models.

    Replacement for assemble_json_outputs() that uses Pydantic models
    for type safety and consistent serialization.

    Args:
        ctx: Pipeline context with loaded data.
        output_dir: Output directory (defaults to config output_path).
        streaming: Use streaming for large files like AllPrintings.
        save_cache: Save assembly metadata for fast-path on subsequent runs.

    Returns:
        Dict mapping file names to record counts.
    """
    if output_dir is None:
        output_dir = MtgjsonConfig().output_path
    output_dir.mkdir(parents=True, exist_ok=True)

    assembly_ctx = build_assembly_context(ctx)

    # Save cache for fast-path on subsequent runs
    if save_cache:
        save_assembly_cache(assembly_ctx)

    builder = MtgjsonFileBuilder(assembly_ctx)

    set_codes = ctx.sets_to_build
    results: dict[str, int] = {}

    # Build AllPrintings (streaming for memory efficiency)
    LOGGER.info("Building AllPrintings.json...")
    all_printings = builder.build_all_printings(
        output_dir / "AllPrintings.json",
        set_codes=set_codes,
        streaming=streaming,
    )
    results["AllPrintings"] = all_printings if isinstance(all_printings, int) else len(all_printings.data)

    # Build AtomicCards
    LOGGER.info("Building AtomicCards.json...")
    atomic_cards = builder.build_atomic_cards(output_dir / "AtomicCards.json")
    results["AtomicCards"] = len(atomic_cards.data)

    # Build SetList
    LOGGER.info("Building SetList.json...")
    set_list = builder.build_set_list(output_dir / "SetList.json")
    results["SetList"] = len(set_list.data)

    # Build format-specific files (can run in parallel)
    LOGGER.info("Building format-specific files...")
    format_printings = ["legacy", "modern", "pioneer", "standard", "vintage"]
    format_atomic = ["legacy", "modern", "pauper", "pioneer", "standard", "vintage"]

    # Note: These need AllPrintings/AtomicCards loaded, so we reload if streaming was used
    if streaming:
        from mtgjson5.mtgjson_models import AllPrintingsFile
        all_printings = AllPrintingsFile.read(output_dir / "AllPrintings.json")

    for fmt in format_printings:
        fmt_file = builder.build_format_file(
            all_printings, fmt, output_dir / f"{fmt.title()}.json"
        )
        results[fmt.title()] = len(fmt_file.data)

    for fmt in format_atomic:
        atomic_file = builder.build_format_atomic(
            atomic_cards, fmt, output_dir / f"{fmt.title()}Atomic.json"
        )
        results[f"{fmt.title()}Atomic"] = len(atomic_file.data)

    # Build individual set files
    LOGGER.info("Building individual set files...")
    set_count = _build_individual_sets(builder, output_dir, set_codes)
    results["sets"] = set_count

    return results


def _build_individual_sets(
    builder: MtgjsonFileBuilder,
    output_dir: pathlib.Path,
    set_codes: list[str] | None = None,
    parallel: bool = True,
    max_workers: int = 4,
) -> int:
    """Build individual set JSON files."""
    from mtgjson5.mtgjson_models import MtgjsonFileBase

    codes = set_codes or sorted(builder.ctx.set_meta.keys())
    valid_codes = [
        code for code in codes
        if (builder.ctx.parquet_dir / f"setCode={code}").exists()
    ]

    if not valid_codes:
        return 0

    def write_set(code: str) -> bool:
        try:
            set_data = builder.set_assembler.assemble(code)
            file = MtgjsonFileBase.with_meta(set_data, builder.ctx.meta)
            file.write(output_dir / f"{code}.json")
            return True
        except Exception as e:
            LOGGER.error(f"Failed to write {code}: {e}")
            return False

    if parallel and len(valid_codes) > 1:
        count = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(write_set, code): code for code in valid_codes}
            for future in as_completed(futures):
                if future.result():
                    count += 1
        return count
    else:
        return sum(1 for code in valid_codes if write_set(code))


def assemble_sets_parallel(
    ctx: PipelineContext,
    output_dir: pathlib.Path | None = None,
    max_workers: int = 8,
) -> dict[str, int]:
    """
    Assemble only individual set files in parallel.

    Lighter weight than assemble_with_models() - skips compiled files.
    Useful for incremental builds.
    """
    if output_dir is None:
        output_dir = MtgjsonConfig().output_path
    output_dir.mkdir(parents=True, exist_ok=True)

    assembly_ctx = build_assembly_context(ctx)
    builder = MtgjsonFileBuilder(assembly_ctx)

    count = _build_individual_sets(
        builder,
        output_dir,
        ctx.sets_to_build,
        parallel=True,
        max_workers=max_workers,
    )

    return {"sets": count}


def build_deck_files(
    ctx: PipelineContext,
    output_dir: pathlib.Path | None = None,
) -> int:
    """
    Build individual deck JSON files.

    Creates DeckList.json and individual deck files.
    """
    from mtgjson5.mtgjson_models import DeckListFile, MtgjsonFileBase
    from mtgjson5.pipeline import build_decks_expanded

    if output_dir is None:
        output_dir = MtgjsonConfig().output_path
    decks_dir = output_dir / "decks"
    decks_dir.mkdir(parents=True, exist_ok=True)

    # Get expanded decks
    decks_df = build_decks_expanded(ctx, set_codes=ctx.sets_to_build)
    if isinstance(decks_df, pl.LazyFrame):
        decks_df = decks_df.collect()

    if len(decks_df) == 0:
        LOGGER.warning("No decks to build")
        return 0

    meta_obj = MtgjsonMetaObject()
    meta = {"date": meta_obj.date, "version": meta_obj.version}

    # Build DeckList (summary without cards)
    deck_list_data = []
    decks_written = 0

    for row in decks_df.to_dicts():
        # DeckList entry (no card data)
        deck_list_data.append({
            "code": row.get("code", row.get("setCode", "")),
            "name": row.get("name", ""),
            "releaseDate": row.get("releaseDate"),
            "type": row.get("type", ""),
        })

        # Individual deck file
        deck_name = row.get("name", "").replace("/", "-").replace("\\", "-")
        set_code = row.get("setCode", row.get("code", "UNK"))
        filename = f"{set_code}_{deck_name}.json"

        deck_file = MtgjsonFileBase.with_meta(row, meta)
        deck_file.write(decks_dir / filename)
        decks_written += 1

    # Write DeckList.json
    deck_list_file = DeckListFile.with_meta(deck_list_data, meta)
    deck_list_file.write(output_dir / "DeckList.json")

    LOGGER.info(f"Built {decks_written} deck files")
    return decks_written
