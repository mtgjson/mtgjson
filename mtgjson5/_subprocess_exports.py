"""Subprocess target for running exports + prices in isolation.

This module is intentionally free of top-level side effects so that
``multiprocessing.spawn`` can import it without re-executing the heavy
init code in ``__main__.py`` (logger setup, urllib3 warnings, etc.).
"""

from __future__ import annotations

import logging
import traceback
from multiprocessing import Queue
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from mtgjson5.build.writer import FormatType, UnifiedOutputWriter

# ------------------------------------------------------------------
# Subprocess targets (top-level functions for pickling)
# ------------------------------------------------------------------


def _run_price_build(
    parquet_output_dir: str | None,
    do_json_prices: bool,
    raw_prices_ready: bool,
    error_queue: Queue[str],
    log_file: str | None = None,
    profile: bool = False,
    profile_queue: Queue[dict[str, Any]] | None = None,
) -> None:
    """Subprocess target for price builds (parquet + JSON).

    Runs a single unified ``build_prices()`` call that handles both
    parquet and JSON output using the memory-efficient partitioned archive.
    Prices are fetched from providers exactly once.
    """
    try:
        from mtgjson5.utils import init_logger

        init_logger(log_file)
        _log = logging.getLogger(__name__)

        from mtgjson5.profiler import SubprocessProfiler

        sp = SubprocessProfiler(label="prices", enabled=profile)
        sp.start()

        import pathlib

        from mtgjson5 import constants
        from mtgjson5.build.price_builder import PolarsPriceBuilder

        parquet_dir = pathlib.Path(parquet_output_dir) if parquet_output_dir else None
        raw_cache = constants.CACHE_PATH if raw_prices_ready else None

        _log.info("Price subprocess: unified price build (parquet=%s, json=%s)", bool(parquet_dir), do_json_prices)
        PolarsPriceBuilder().build_prices(
            parquet_output_dir=parquet_dir,
            write_json=do_json_prices,
            raw_cache_dir=raw_cache,
        )
        sp.checkpoint("price_build_complete")
        _log.info("Price subprocess: complete")

        sp.checkpoint("finish")
        if profile_queue is not None:
            profile_queue.put(sp.to_dict())
    except Exception as exc:
        error_queue.put(f"price build: {exc}\n{traceback.format_exc()}")


# ------------------------------------------------------------------
# Main entry point
# ------------------------------------------------------------------


def run_exports(
    formats: list[str] | None,
    error_queue: Queue[str],
    log_file: str | None = None,
    profile: bool = False,
    profile_queue: Queue[dict[str, Any]] | None = None,
) -> None:
    """Entry point for the format exports subprocess (no prices).

    Prices are handled in a separate subprocess so that jemalloc allocations
    from format exports are fully reclaimed before the price build starts.

    Both code paths are fully disk-backed:
    - ``AssemblyContext.from_cache()`` reads parquet/JSON from the cache dir
    """
    try:
        from mtgjson5.utils import init_logger

        init_logger(log_file)
        _log = logging.getLogger(__name__)

        from mtgjson5.profiler import SubprocessProfiler

        sp = SubprocessProfiler(label="exports", enabled=profile)
        sp.start()

        if not formats:
            return

        from mtgjson5.build.writer import UnifiedOutputWriter

        writer = UnifiedOutputWriter.from_cache(
            skip=frozenset({"decks", "sealed", "token_products", "boosters"}),
        )
        sp.checkpoint("cache_loaded")
        if writer is None:
            _log.warning("Subprocess: no assembly cache found, skipping exports")
            sp.checkpoint("finish")
            if profile_queue is not None:
                profile_queue.put(sp.to_dict())
            return

        has_parquet = bool("parquet" in formats)
        remaining = [f for f in formats if f != "parquet"]

        if has_parquet and remaining:
            _log.info("Exports: parquet + format writes")
            _run_format_exports(writer, formats, remaining, has_parquet, _log, sp)
        else:
            _log.info("Exports: writing all formats")
            writer.write_all(cast("list[FormatType]", formats))
            sp.checkpoint("formats_complete")
            _log.info("Subprocess: exports complete")

        sp.checkpoint("finish")
        if profile_queue is not None:
            profile_queue.put(sp.to_dict())
    except Exception as exc:
        error_queue.put(f"{exc}\n{traceback.format_exc()}")


def _run_format_exports(
    writer: UnifiedOutputWriter,
    formats: list[str],
    remaining: list[str],
    has_parquet: bool,
    _log: logging.Logger,
    sp: Any = None,
) -> None:
    """Export path: parquet data writes then remaining formats.

    Prices are handled in a separate subprocess to avoid jemalloc
    accumulation from format exports.

    Flow:
        1. Parquet data writes (without prices)
        2. Build normalized_tables, release card_data
        3. Run remaining formats in-process (uses normalized_tables)
    """
    # Phase 1: Parquet data writes (without prices)
    if has_parquet:
        from mtgjson5.build.formats.parquet import ParquetBuilder

        parquet_builder = ParquetBuilder(writer.ctx)
        parquet_builder.write(include_prices=False)
        if sp:
            sp.checkpoint("parquet_data_complete")
        _log.info("Exports: parquet data writes complete (prices deferred)")

    # Build normalized_tables while card data is still cached,
    # then release heavy card DataFrames.
    _ = writer.ctx.normalized_tables
    writer.ctx.release_card_data()
    if sp:
        sp.checkpoint("normalized_tables_built")
    _log.info("Exports: normalized_tables built, card data released")

    # Phase 2: Run remaining formats in-process (only need normalized_tables)
    for fmt in remaining:
        writer.write(cast("FormatType", fmt))
    if sp:
        sp.checkpoint("formats_complete")
    _log.info("Exports: format writes complete")
