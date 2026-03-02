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
    error_queue: Queue[str],
    log_file: str | None = None,
    profile: bool = False,
    profile_queue: Queue[dict[str, Any]] | None = None,
) -> None:
    """Subprocess target for price builds (parquet + JSON).

    Runs in parallel with remaining format exports.  Both operations
    are fully independent — they create their own ``PolarsPriceBuilder``
    and load data from the parquet cache.
    """
    try:
        from mtgjson5.utils import init_logger

        init_logger(log_file)
        _log = logging.getLogger(__name__)

        from mtgjson5.profiler import SubprocessProfiler

        sp = SubprocessProfiler(label="prices", enabled=profile)
        sp.start()

        if parquet_output_dir:
            import pathlib

            _log.info("Price subprocess: writing price parquet files")
            from mtgjson5.build.formats.parquet import write_price_parquet

            write_price_parquet(pathlib.Path(parquet_output_dir))
            sp.checkpoint("price_parquet_complete")
            _log.info("Price subprocess: price parquet complete")

        if do_json_prices:
            _log.info("Price subprocess: running JSON price build")
            from mtgjson5.build.price_builder import PolarsPriceBuilder

            PolarsPriceBuilder().build_prices()
            sp.checkpoint("price_json_complete")
            _log.info("Price subprocess: JSON price build complete")

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
    do_prices: bool,
    error_queue: Queue[str],
    log_file: str | None = None,
    profile: bool = False,
    profile_queue: Queue[dict[str, Any]] | None = None,
) -> None:
    """Entry point for the exports/prices subprocess.

    When both price work and remaining format exports are needed, prices
    are spawned in a nested subprocess so they run in parallel with the
    format exports (sqlite, csv, psql, sql).  This overlaps the ~285 s
    network-bound price build with the ~162 s format writes.

    Both code paths are fully disk-backed:

    - ``AssemblyContext.from_cache()`` reads parquet/JSON from the cache dir
    - ``PriceBuilderContext.from_cache()`` loads ID mappings from parquet
    """
    try:
        from mtgjson5.utils import init_logger

        init_logger(log_file)
        _log = logging.getLogger(__name__)

        from mtgjson5.profiler import SubprocessProfiler

        sp = SubprocessProfiler(label="exports", enabled=profile)
        sp.start()

        # Early exit: nothing to do
        if not formats and not do_prices:
            return

        # Resolve writer
        writer = None
        if formats:
            from mtgjson5.build.writer import UnifiedOutputWriter

            writer = UnifiedOutputWriter.from_cache()
            sp.checkpoint("cache_loaded")
            if writer is None:
                _log.warning("Subprocess: no assembly cache found, skipping exports")
                formats = None

        has_parquet = bool(formats and "parquet" in formats)
        remaining = [f for f in (formats or []) if f != "parquet"]
        needs_prices = has_parquet or do_prices

        # Parallel path: price subprocess || remaining format exports
        if needs_prices and remaining and writer is not None:
            _log.info("Exports: parallel mode (prices subprocess + format exports)")
            _run_parallel(writer, formats, remaining, has_parquet, do_prices, _log, sp)
        else:
            # Sequential path — no parallelism opportunity
            _log.info("Exports: sequential mode")
            if formats and writer is not None:
                writer.write_all(cast("list[FormatType]", formats))
                sp.checkpoint("formats_complete")
                _log.info("Subprocess: exports complete")
            if do_prices:
                _log.info("Subprocess: running price build")
                from mtgjson5.build.price_builder import PolarsPriceBuilder

                PolarsPriceBuilder().build_prices()
                sp.checkpoint("prices_complete")
                _log.info("Subprocess: price build complete")

        sp.checkpoint("finish")
        if profile_queue is not None:
            profile_queue.put(sp.to_dict())
    except Exception as exc:
        error_queue.put(f"{exc}\n{traceback.format_exc()}")


def _run_parallel(
    writer: UnifiedOutputWriter,
    formats: list[str] | None,
    remaining: list[str],
    has_parquet: bool,
    do_prices: bool,
    _log: logging.Logger,
    sp: Any = None,
) -> None:
    """Parallel export path: prices in subprocess, formats in-process.

    Flow:
        1. Parquet data writes (without prices)  — ~95 s
        2. Build normalized_tables, release card_data
        3. Spawn price subprocess                 — ~285 s (network-bound)
        4. Run remaining formats in-process       — ~162 s (uses normalized_tables)
        5. Join price subprocess
    """
    import multiprocessing

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

    # Phase 2a: Spawn price subprocess
    parquet_dir = str(writer.ctx.output_path / "parquet") if has_parquet else None

    from mtgjson5.utils import get_log_file

    profile_enabled = sp.enabled if sp else False
    mp_ctx = multiprocessing.get_context("spawn")
    price_error_queue: Queue[str] = mp_ctx.Queue()
    price_profile_queue: Queue[dict[str, Any]] | None = mp_ctx.Queue() if profile_enabled else None
    price_proc = mp_ctx.Process(
        target=_run_price_build,
        args=(parquet_dir, do_prices, price_error_queue, get_log_file(),
              profile_enabled, price_profile_queue),
    )
    price_proc.start()
    _log.info("Exports: price subprocess spawned")

    # Phase 2b: Run remaining formats in-process (only need normalized_tables)
    for fmt in remaining:
        writer.write(cast("FormatType", fmt))
    if sp:
        sp.checkpoint("formats_complete")
    _log.info("Exports: remaining formats complete")

    # Phase 3: Wait for price subprocess
    price_proc.join(timeout=1200)  # 20 min timeout
    if price_proc.is_alive():
        _log.error("Price subprocess timed out, terminating")
        price_proc.terminate()
        price_proc.join(timeout=10)
    elif price_proc.exitcode != 0:
        _log.error(f"Price subprocess exited with code {price_proc.exitcode}")

    if not price_error_queue.empty():
        err = price_error_queue.get_nowait()
        _log.error(f"Price subprocess error: {err}")

    # Collect nested price subprocess profile
    if price_profile_queue is not None and not price_profile_queue.empty():
        price_profile = price_profile_queue.get_nowait()
        if sp:
            sp.add_nested_profile(price_profile)

    _log.info("Exports: all complete (parallel)")
