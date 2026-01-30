"""JSON output writers for MTGJSON.

Writers handle file I/O
"""

from __future__ import annotations

import pathlib
from typing import TYPE_CHECKING, Any

import orjson

from mtgjson5.v2.models.files import (
    AllPricesFile,
    AllPrintingsFile,
    AtomicCardsFile,
    DeckListFile,
    FormatAtomicFile,
    FormatPrintingsFile,
    IndividualSetFile,
    SetListFile,
)

if TYPE_CHECKING:
    from ..context import AssemblyContext

# Format definitions - single source of truth
_PRINTINGS_FORMATS = ["legacy", "modern", "pioneer", "standard", "vintage"]
_ATOMIC_FORMATS = ["legacy", "modern", "pauper", "pioneer", "standard", "vintage"]
_PRINTINGS_OUTPUTS = {fmt.title() for fmt in _PRINTINGS_FORMATS}
_ATOMIC_OUTPUTS = {f"{fmt.title()}Atomic" for fmt in _ATOMIC_FORMATS}


class JsonOutputBuilder:
    """Writes all JSON-based MTGJSON output files."""

    def __init__(self, ctx: AssemblyContext):
        self.ctx = ctx
        self._orjson_opts = orjson.OPT_SORT_KEYS | (
            orjson.OPT_INDENT_2 if ctx.pretty else 0
        )

    def write_all_printings(
        self,
        output_path: pathlib.Path,
        set_codes: list[str] | None = None,
        streaming: bool = True,
    ) -> AllPrintingsFile | int:
        """Build AllPrintings.json. Returns count if streaming, else full file."""
        if streaming:
            return self._write_all_printings_streaming(output_path, set_codes)

        data: dict[str, dict[str, Any]] = {}
        for code, set_data in self.ctx.sets.iter_sets(set_codes=set_codes):
            data[code] = set_data

        file = AllPrintingsFile.with_meta(data, self.ctx.meta)
        file.write(output_path)
        return file  # type: ignore[return-value]

    def _write_all_printings_streaming(
        self,
        output_path: pathlib.Path,
        set_codes: list[str] | None = None,
    ) -> int:
        """Stream AllPrintings.json to disk."""
        codes = set_codes or sorted(self.ctx.set_meta.keys())

        with output_path.open("wb") as f:
            f.write(b'{"meta":')
            f.write(orjson.dumps(self.ctx.meta, option=self._orjson_opts))
            f.write(b',"data":{')

            first = True
            count = 0

            for code, set_data in self.ctx.sets.iter_sets(set_codes=codes):
                if not first:
                    f.write(b",")
                first = False

                f.write(b'"')
                f.write(code.encode())
                f.write(b'":')
                f.write(orjson.dumps(set_data, option=self._orjson_opts))
                count += 1

            f.write(b"}}")

        return count

    def write_atomic_cards(self, output_path: pathlib.Path) -> AtomicCardsFile:
        """Build AtomicCards.json."""
        data = self.ctx.atomic_cards.build()

        file = AtomicCardsFile.with_meta(data, self.ctx.meta)
        file.write(output_path)
        return file  # type: ignore[return-value]

    def write_set_list(self, output_path: pathlib.Path) -> SetListFile:
        """Build SetList.json."""
        data = self.ctx.set_list.build()

        file = SetListFile.with_meta(data, self.ctx.meta)
        file.write(output_path)
        return file  # type: ignore[return-value]

    def write_format_file(
        self,
        all_printings: AllPrintingsFile,
        format_name: str,
        output_path: pathlib.Path,
    ) -> FormatPrintingsFile:
        """Build format-specific printings file."""
        from ..assemble import compute_format_legal_sets

        format_legal_sets = compute_format_legal_sets(self.ctx, format_name)
        file = FormatPrintingsFile.for_format(
            format_name, all_printings, format_legal_sets
        )
        file.write(output_path)
        return file

    def write_format_atomic(
        self,
        atomic_cards: AtomicCardsFile,
        format_name: str,
        output_path: pathlib.Path,
    ) -> FormatAtomicFile:
        """Build format-specific atomic file."""
        file = FormatAtomicFile.for_format(format_name, atomic_cards)
        file.write(output_path)
        return file

    def write_decks(
        self,
        output_dir: pathlib.Path | None = None,
        set_codes: list[str] | None = None,
    ) -> int:
        """Write individual deck JSON files with expanded cards.

        Args:
            output_dir: Output directory (defaults to ctx.output_path/decks)
            set_codes: Optional filter for specific set codes

        Returns:
            Number of deck files written
        """
        import hashlib

        import polars as pl

        if self.ctx.decks_df is None or len(self.ctx.decks_df) == 0:
            return 0

        if output_dir is None:
            output_dir = self.ctx.output_path / "decks"
        output_dir.mkdir(parents=True, exist_ok=True)

        decks_df = self.ctx.decks_df
        if set_codes:
            upper_codes = {s.upper() for s in set_codes}
            decks_df = decks_df.filter(
                pl.col("setCode").str.to_uppercase().is_in(upper_codes)
            )

        if len(decks_df) == 0:
            return 0

        assembler = self.ctx.deck_assembler()

        meta_dict = self.ctx.meta
        count = 0

        for deck_raw in decks_df.to_dicts():
            deck = assembler.build(deck_raw)

            safe_name = "".join(c for c in deck["name"] if c.isalnum())
            set_code = deck.get("code", deck_raw.get("setCode", "UNK"))
            filename = f"{safe_name}_{set_code}"

            output = {"meta": meta_dict, "data": deck}
            json_path = output_dir / f"{filename}.json"
            json_bytes = orjson.dumps(output, option=self._orjson_opts)
            json_path.write_bytes(json_bytes)

            sha_path = output_dir / f"{filename}.json.sha256"
            sha_path.write_text(hashlib.sha256(json_bytes).hexdigest())

            count += 1

        del assembler
        return count

    def write_prices(self, output_dir: pathlib.Path) -> dict[str, int]:
        """Build AllPrices.json and AllPricesToday.json using Polars builder.

        Args:
            output_dir: Output directory for price files

        Returns:
            Dict mapping file names to record counts
        """
        from mtgjson5.v2.build.price_builder import PolarsPriceBuilder
        from mtgjson5.utils import LOGGER

        builder = PolarsPriceBuilder()
        all_prices, today_prices = builder.build_prices()

        if not all_prices:
            LOGGER.warning("No price data generated")
            return {}

        results: dict[str, int] = {}

        # Write AllPrices.json
        all_file = AllPricesFile.with_meta(all_prices, self.ctx.meta)
        all_file.write(output_dir / "AllPrices.json")
        results["AllPrices"] = len(all_prices)

        # Write AllPricesToday.json
        today_file = AllPricesFile.with_meta(today_prices, self.ctx.meta)
        today_file.write(output_dir / "AllPricesToday.json")
        results["AllPricesToday"] = len(today_prices)

        LOGGER.info(
            f"Built AllPrices.json ({len(all_prices):,} cards) "
            f"and AllPricesToday.json ({len(today_prices):,} cards)"
        )

        return results

    def write_all(
        self,
        output_dir: pathlib.Path | None = None,
        set_codes: list[str] | None = None,
        streaming: bool = True,
        include_decks: bool = True,
        outputs: set[str] | None = None,
    ) -> dict[str, int]:
        """Build all MTGJSON JSON output files.

        Args:
            output_dir: Output directory (defaults to context output_path)
            set_codes: Optional filter for specific set codes
            streaming: Use streaming for large files like AllPrintings
            include_decks: Include individual deck files and DeckList.json
            outputs: Optional set of output types to build (e.g., {"AllPrintings"}).
                    If None or empty, builds all outputs.

        Returns:
            Dict mapping file names to record counts.
        """
        from mtgjson5.utils import LOGGER

        if output_dir is None:
            output_dir = self.ctx.output_path
        output_dir.mkdir(parents=True, exist_ok=True)

        # Filter set codes if provided
        valid_codes = None
        if set_codes:
            valid_codes = [code for code in set_codes if code in self.ctx.set_meta]

        results: dict[str, int] = {}

        # Helper to check if an output should be built
        def should_build(name: str) -> bool:
            return not outputs or name in outputs

        # Build AllPrintings
        all_printings = None
        if should_build("AllPrintings"):
            LOGGER.info("Building AllPrintings.json...")
            all_printings = self.write_all_printings(
                output_dir / "AllPrintings.json",
                set_codes=valid_codes,
                streaming=streaming,
            )
            results["AllPrintings"] = (
                all_printings
                if isinstance(all_printings, int)
                else len(all_printings.data)  # pylint: disable=no-member
            )

        # Build AtomicCards
        atomic_cards = None
        if should_build("AtomicCards"):
            LOGGER.info("Building AtomicCards.json...")
            atomic_cards = self.write_atomic_cards(output_dir / "AtomicCards.json")
            results["AtomicCards"] = len(atomic_cards.data)

        # Build SetList
        if should_build("SetList"):
            LOGGER.info("Building SetList.json...")
            set_list = self.write_set_list(output_dir / "SetList.json")
            results["SetList"] = len(set_list.data)

        # Build format-specific files (require AllPrintings)
        if outputs is None or (_PRINTINGS_OUTPUTS & outputs):
            LOGGER.info("Building format-specific files...")
            if all_printings is None or (streaming and isinstance(all_printings, int)):
                all_printings = AllPrintingsFile.read(output_dir / "AllPrintings.json")  # type: ignore[assignment]

            if isinstance(all_printings, AllPrintingsFile):
                for fmt in _PRINTINGS_FORMATS:
                    if should_build(fmt.title()):
                        fmt_file = self.write_format_file(
                            all_printings, fmt, output_dir / f"{fmt.title()}.json"
                        )
                        results[fmt.title()] = len(fmt_file.data)

        # Format atomic files (require AtomicCards)
        if outputs is None or (_ATOMIC_OUTPUTS & outputs):
            if atomic_cards is None:
                atomic_cards = self.write_atomic_cards(output_dir / "AtomicCards.json")
            for fmt in _ATOMIC_FORMATS:
                if should_build(f"{fmt.title()}Atomic"):
                    atomic_file = self.write_format_atomic(
                        atomic_cards, fmt, output_dir / f"{fmt.title()}Atomic.json"
                    )
                    results[f"{fmt.title()}Atomic"] = len(atomic_file.data)

        # Build individual set files
        if outputs is None or not outputs:
            LOGGER.info("Building individual set files...")
            set_count = 0
            for code, set_data in self.ctx.sets.iter_sets(set_codes=valid_codes):
                single = IndividualSetFile.from_set_data(set_data, self.ctx.meta)
                single.write(output_dir / f"{code}.json")
                set_count += 1
            results["sets"] = set_count

        # Build deck files
        if include_decks and (
            outputs is None or "Decks" in outputs or "DeckList" in outputs
        ):
            LOGGER.info("Building deck files...")
            deck_count = self.write_decks(output_dir / "decks", set_codes=valid_codes)
            results["decks"] = deck_count

            deck_list = self.ctx.deck_list.build()
            deck_list_file = DeckListFile.with_meta(deck_list, self.ctx.meta)
            deck_list_file.write(output_dir / "DeckList.json")
            results["DeckList"] = len(deck_list)

        # Use --price-build flag for dedicated price builds, or --outputs AllPrices
        if outputs and ("AllPrices" in outputs or "AllPricesToday" in outputs):
            LOGGER.info("Building price files...")
            price_results = self.write_prices(output_dir)
            results.update(price_results)

        return results

    def write(self) -> None:
        """Write all JSON outputs to the configured output path."""
        self.write_all(self.ctx.output_path)
