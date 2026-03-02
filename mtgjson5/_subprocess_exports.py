"""Subprocess target for running exports + prices in isolation.

This module is intentionally free of top-level side effects so that
``multiprocessing.spawn`` can import it without re-executing the heavy
init code in ``__main__.py`` (logger setup, urllib3 warnings, etc.).
"""

import logging
import traceback


def run_exports(
    formats: list[str] | None,
    do_prices: bool,
    error_queue: "multiprocessing.Queue[str]",
) -> None:
    """Entry point for the exports/prices subprocess.

    Both code paths are fully disk-backed:

    - ``AssemblyContext.from_cache()`` reads parquet/JSON from the cache dir
    - ``PriceBuilderContext.from_cache()`` loads ID mappings from parquet
    """
    try:
        from mtgjson5.utils import init_logger

        init_logger()
        _log = logging.getLogger(__name__)

        if formats:
            _log.info(f"Subprocess: running exports {formats}")
            from mtgjson5.build.writer import UnifiedOutputWriter

            writer = UnifiedOutputWriter.from_cache()
            if writer is not None:
                writer.write_all(formats)
                _log.info("Subprocess: exports complete")
            else:
                _log.warning("Subprocess: no assembly cache found, skipping exports")

        if do_prices:
            _log.info("Subprocess: running price build")
            from mtgjson5.build.price_builder import PolarsPriceBuilder

            PolarsPriceBuilder().build_prices()
            _log.info("Subprocess: price build complete")
    except Exception as exc:
        error_queue.put(f"{exc}\n{traceback.format_exc()}")
