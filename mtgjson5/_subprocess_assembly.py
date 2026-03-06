"""Subprocess targets for memory-heavy JSON assembly tasks.

This module is intentionally free of top-level side effects so that
``multiprocessing.spawn`` can import it without re-executing the heavy
init code in ``__main__.py`` (logger setup, urllib3 warnings, etc.).

Each group of assembly tasks runs in an isolated subprocess. The subprocess
loads ``AssemblyContext.from_cache()`` (fully disk-backed), builds its
outputs, then exits — freeing all jemalloc allocations on process death.
"""

from __future__ import annotations

import logging
import traceback
from multiprocessing import Queue
from pathlib import Path
from typing import Any

# Per-group skip sets: fields that a group's tasks don't access.
# Avoids loading large DataFrames into subprocesses that never use them.
_GROUP_SKIP: dict[str, frozenset[str]] = {
    "A": frozenset(),  # AllPrintings needs everything
    "B": frozenset({"decks", "sealed", "token_products", "boosters"}),
    "C": frozenset(),  # SetFiles needs everything
    "D": frozenset({"sealed", "token_products", "boosters"}),
    "E": frozenset({"decks", "boosters"}),
    "F": frozenset({"token_products", "boosters"}),
}


def run_assembly_group(
    tasks: list[str],
    output_dir: str,
    pretty: bool,
    set_codes: list[str] | None,
    sets_only: bool,
    include_decks: bool,
    results_queue: Queue[dict[str, Any]],
    error_queue: Queue[str],
    log_file: str | None = None,
    profile: bool = False,
    group_label: str = "",
) -> None:
    """Execute a list of assembly tasks in an isolated subprocess.

    Args:
        tasks: Task names to execute (e.g. ["AllPrintings", "FormatPrintings"])
        output_dir: Output directory path (string for pickling)
        pretty: Pretty-print JSON output
        set_codes: Optional set code filter
        sets_only: Whether sets-only mode is active
        include_decks: Whether to include deck files
        results_queue: Queue to put results dict on completion
        error_queue: Queue to put error strings on failure
        log_file: Parent's log file path (all subprocesses share one log)
        profile: Whether to collect RSS checkpoints
        group_label: Group identifier for profiler label (e.g. "A")
    """
    try:
        from mtgjson5.utils import init_logger

        init_logger(log_file)
        _log = logging.getLogger(__name__)

        from mtgjson5.profiler import SubprocessProfiler

        sp = SubprocessProfiler(label=f"assembly_{group_label}", enabled=profile)
        sp.start()

        from mtgjson5.build.context import AssemblyContext
        from mtgjson5.build.formats.json import JsonOutputBuilder

        skip = _GROUP_SKIP.get(group_label, frozenset())
        ctx = AssemblyContext.from_cache(skip=skip)
        sp.checkpoint("cache_loaded")

        if ctx is None:
            error_queue.put("AssemblyContext cache not found")
            return

        ctx.pretty = pretty
        builder = JsonOutputBuilder(ctx)
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        results: dict[str, Any] = {}

        for task in tasks:
            _log.info(f"Subprocess: building {task}")
            _run_task(task, builder, ctx, out, set_codes, sets_only, include_decks, results)
            sp.checkpoint(f"task_{task}")
            _log.info(f"Subprocess: {task} complete")

        sp.checkpoint("finish")
        results["_profile"] = sp.to_dict()
        results_queue.put(results)
    except Exception as exc:
        error_queue.put(f"assembly group {tasks}: {exc}\n{traceback.format_exc()}")


def _run_task(
    task: str,
    builder: Any,
    ctx: Any,
    out: Path,
    set_codes: list[str] | None,
    sets_only: bool,
    include_decks: bool,
    results: dict[str, int],
) -> None:
    """Dispatch a single assembly task to the appropriate builder method."""
    import gc

    if task == "AllPrintings":
        count = builder.write_all_printings(
            out / "AllPrintings.json",
            set_codes=set_codes,
            streaming=True,
        )
        results["AllPrintings"] = count if isinstance(count, int) else len(count.data)

    elif task == "FormatPrintings":
        from mtgjson5.models.files import AllPrintingsFile

        ap_path = out / "AllPrintings.json"
        if not ap_path.exists():
            return
        all_printings = AllPrintingsFile.read(ap_path)
        printings_formats = ["legacy", "modern", "pioneer", "standard", "vintage"]
        for fmt in printings_formats:
            fmt_file = builder.write_format_file(all_printings, fmt, out / f"{fmt.title()}.json")
            results[fmt.title()] = len(fmt_file.data)
        del all_printings
        gc.collect()

    elif task == "AtomicCards":
        count = builder.write_atomic_cards(out / "AtomicCards.json", streaming=True)
        results["AtomicCards"] = count if isinstance(count, int) else len(count.data)

    elif task == "FormatAtomics":
        from mtgjson5.models.files import AtomicCardsFile

        ac_path = out / "AtomicCards.json"
        if not ac_path.exists():
            # Ensure AtomicCards exists (may need to build it)
            builder.write_atomic_cards(ac_path, streaming=True)
        atomic_cards_file = AtomicCardsFile.read(ac_path)
        atomic_formats = ["legacy", "modern", "pauper", "pioneer", "standard", "vintage"]
        for fmt in atomic_formats:
            atomic_file = builder.write_format_atomic(atomic_cards_file, fmt, out / f"{fmt.title()}Atomic.json")
            results[f"{fmt.title()}Atomic"] = len(atomic_file.data)
        del atomic_cards_file
        gc.collect()

    elif task == "SetFiles":
        from mtgjson5.models.files import IndividualSetFile
        from mtgjson5.polars_utils import get_windows_safe_set_code

        set_count = 0
        valid_codes = set_codes
        for code, set_data in ctx.sets.iter_sets(set_codes=valid_codes):
            single = IndividualSetFile.from_set_data(set_data, ctx.meta)
            safe_code = get_windows_safe_set_code(code)
            single.write(out / f"{safe_code}.json", pretty=ctx.pretty)
            set_count += 1
        results["sets"] = set_count

    elif task == "SetList":
        set_list = builder.write_set_list(out / "SetList.json")
        results["SetList"] = len(set_list.data)

    elif task == "Meta":
        builder.write_meta(out / "Meta.json")
        results["Meta"] = 1

    elif task == "CompiledList":
        compiled_list = builder.write_compiled_list(out / "CompiledList.json")
        results["CompiledList"] = len(compiled_list.data)

    elif task == "DeckFiles":
        if not include_decks:
            return
        deck_count = builder.write_decks(out / "decks", set_codes=set_codes)
        results["decks"] = deck_count

    elif task == "DeckList":
        if not include_decks:
            return
        from mtgjson5.models.files import DeckListFile

        deck_list = ctx.deck_list.build()
        deck_list_file = DeckListFile.with_meta(deck_list, ctx.meta)
        deck_list_file.write(out / "DeckList.json", pretty=ctx.pretty)
        results["DeckList"] = len(deck_list)

    elif task == "TcgplayerSkus":
        count = builder.write_tcgplayer_skus(out / "TcgplayerSkus.json")
        results["TcgplayerSkus"] = count if isinstance(count, int) else len(count.data)

    elif task == "AllIdentifiers":
        count = builder.write_all_identifiers(out / "AllIdentifiers.json")
        results["AllIdentifiers"] = count

    elif task == "Keywords":
        keywords = builder.write_keywords(out / "Keywords.json")
        results["Keywords"] = sum(len(v) for v in keywords.data.values())

    elif task == "CardTypes":
        card_types = builder.write_card_types(out / "CardTypes.json")
        results["CardTypes"] = len(card_types.data)

    elif task == "EnumValues":
        enum_values = builder.write_enum_values(out / "EnumValues.json")
        results["EnumValues"] = sum(
            len(v) if isinstance(v, list) else sum(len(vv) for vv in v.values()) for v in enum_values.data.values()
        )
