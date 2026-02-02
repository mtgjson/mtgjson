"""JSON output writers for MTGJSON.

Writers handle file I/O
"""

from __future__ import annotations

import pathlib
from typing import TYPE_CHECKING, Any

import orjson

from mtgjson5.v2.models.files import (
    AllIdentifiersFile,
    AllPrintingsFile,
    AtomicCardsFile,
    CardTypesFile,
    CompiledListFile,
    DeckListFile,
    FormatAtomicFile,
    FormatPrintingsFile,
    IndividualSetFile,
    KeywordsFile,
    MetaFile,
    SetListFile,
    TcgplayerSkusFile,
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

    def write_meta(self, output_path: pathlib.Path) -> MetaFile:
        """Build Meta.json."""
        file = MetaFile.with_meta(self.ctx.meta, self.ctx.meta)
        file.write(output_path, pretty=self.ctx.pretty)
        return file  # type: ignore[return-value]

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
        file.write(output_path, pretty=self.ctx.pretty)
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
        file.write(output_path, pretty=self.ctx.pretty)
        return file  # type: ignore[return-value]

    def write_set_list(self, output_path: pathlib.Path) -> SetListFile:
        """Build SetList.json."""
        data = self.ctx.set_list.build()

        file = SetListFile.with_meta(data, self.ctx.meta)
        file.write(output_path, pretty=self.ctx.pretty)
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
        file.write(output_path, pretty=self.ctx.pretty)
        return file

    def write_format_atomic(
        self,
        atomic_cards: AtomicCardsFile,
        format_name: str,
        output_path: pathlib.Path,
    ) -> FormatAtomicFile:
        """Build format-specific atomic file."""
        file = FormatAtomicFile.for_format(format_name, atomic_cards)
        file.write(output_path, pretty=self.ctx.pretty)
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
            Dict mapping file names to record counts.
        """
        from mtgjson5.v2.build.price_builder import PolarsPriceBuilder
        from mtgjson5.utils import LOGGER

        builder = PolarsPriceBuilder()
        all_prices_path, today_prices_path = builder.build_prices()

        if all_prices_path is None:
            LOGGER.warning("No price data generated")
            return {}

        results: dict[str, int] = {}

        if all_prices_path.exists():
            all_size_mb = all_prices_path.stat().st_size / 1024 / 1024
            results["AllPrices"] = int(all_size_mb * 1000)

        if today_prices_path and today_prices_path.exists():
            today_size_mb = today_prices_path.stat().st_size / 1024 / 1024
            results["AllPricesToday"] = int(today_size_mb * 1000)

        LOGGER.info(
            f"Built AllPrices.json ({all_size_mb:.1f} MB) "
            f"and AllPricesToday.json ({today_size_mb:.1f} MB)"
        )

        return results

    def write_tcgplayer_skus(self, output_path: pathlib.Path) -> TcgplayerSkusFile:
        """Build TcgplayerSkus.json.

        Args:
            output_path: Output file path

        Returns:
            TcgplayerSkusFile with UUID to SKU mappings
        """
        data = self.ctx.tcgplayer_skus.build()
        file = TcgplayerSkusFile.with_meta(data, self.ctx.meta)
        file.write(output_path)
        return file  # type: ignore[return-value]

    def write_keywords(self, output_path: pathlib.Path) -> KeywordsFile:
        """Build Keywords.json.

        Args:
            output_path: Output file path

        Returns:
            KeywordsFile with abilityWords, keywordAbilities, keywordActions
        """
        from ..assemble import KeywordsAssembler

        assembler = KeywordsAssembler(self.ctx)
        data = assembler.build()
        file = KeywordsFile.with_meta(data, self.ctx.meta)
        file.write(output_path)
        return file  # type: ignore[return-value]

    def write_card_types(self, output_path: pathlib.Path) -> CardTypesFile:
        """Build CardTypes.json.

        Args:
            output_path: Output file path

        Returns:
            CardTypesFile with type -> {subTypes, superTypes} mapping
        """
        from ..assemble import CardTypesAssembler

        assembler = CardTypesAssembler(self.ctx)
        data = assembler.build()
        file = CardTypesFile.with_meta(data, self.ctx.meta)
        file.write(output_path)
        return file  # type: ignore[return-value]

    def write_all_identifiers(self, output_path: pathlib.Path) -> int:
        """Build AllIdentifiers.json using streaming to minimize memory usage.

        Streams UUID:card/token pairs directly to file without holding the full
        dict in memory. This reduces memory usage significantly for 100k+ entries.

        Args:
            output_path: Output file path

        Returns:
            Number of entries written
        """
        import gc
        import orjson

        from mtgjson5.utils import LOGGER

        count = 0
        meta_dict = self.ctx.meta

        with open(output_path, "wb") as f:
            f.write(b'{"meta": ')
            f.write(orjson.dumps(meta_dict))
            f.write(b', "data": {')

            first = True
            for uuid, entry in self.ctx.all_identifiers.iter_entries():
                if not first:
                    f.write(b",")
                first = False

                f.write(b"\n")
                f.write(orjson.dumps(uuid))
                f.write(b": ")
                f.write(orjson.dumps(entry))
                count += 1

                if count % 5000 == 0:
                    gc.collect()
                    LOGGER.debug(f"AllIdentifiers: streamed {count} entries")

            f.write(b"\n}}")

        LOGGER.info(f"Streamed AllIdentifiers with {count} entries")
        return count

    def write_compiled_list(self, output_path: pathlib.Path) -> CompiledListFile:
        """Build CompiledList.json.

        Args:
            output_path: Output file path

        Returns:
            CompiledListFile with sorted list of compiled file names
        """
        from ..assemble import CompiledListAssembler

        assembler = CompiledListAssembler()
        data = assembler.build()
        file = CompiledListFile.with_meta(data, self.ctx.meta)
        file.write(output_path)
        return file  # type: ignore[return-value]

    def write_all(
        self,
        output_dir: pathlib.Path | None = None,
        set_codes: list[str] | None = None,
        streaming: bool = True,
        include_decks: bool = True,
        outputs: set[str] | None = None,
        sets_only: bool = False,
    ) -> dict[str, int]:
        """Build all MTGJSON JSON output files.

        Args:
            output_dir: Output directory (defaults to context output_path)
            set_codes: Optional filter for specific set codes
            streaming: Use streaming for large files like AllPrintings
            include_decks: Include individual deck files and DeckList.json
            outputs: Optional set of output types to build (e.g., {"AllPrintings"}).
                    If None or empty, builds all outputs.
            sets_only: When True, only build individual set files. Compiled
                      outputs are skipped unless explicitly listed in ``outputs``.

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
            if sets_only:
                # In sets-only mode, only build outputs explicitly requested
                return outputs is not None and name in outputs
            return not outputs or name in outputs

        # Build Meta
        if should_build("Meta"):
            LOGGER.info("Building Meta.json...")
            self.write_meta(output_dir / "Meta.json")
            results["Meta"] = 1

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
        if not sets_only or (outputs and (_PRINTINGS_OUTPUTS & outputs)):
            if outputs is None or (_PRINTINGS_OUTPUTS & outputs):
                LOGGER.info("Building format-specific files...")
                if all_printings is None or (
                    streaming and isinstance(all_printings, int)
                ):
                    all_printings = AllPrintingsFile.read(output_dir / "AllPrintings.json")  # type: ignore[assignment]

                if isinstance(all_printings, AllPrintingsFile):
                    for fmt in _PRINTINGS_FORMATS:
                        if should_build(fmt.title()):
                            fmt_file = self.write_format_file(
                                all_printings, fmt, output_dir / f"{fmt.title()}.json"
                            )
                            results[fmt.title()] = len(fmt_file.data)

        # Format atomic files (require AtomicCards)
        if not sets_only or (outputs and (_ATOMIC_OUTPUTS & outputs)):
            if outputs is None or (_ATOMIC_OUTPUTS & outputs):
                if atomic_cards is None:
                    atomic_cards = self.write_atomic_cards(
                        output_dir / "AtomicCards.json"
                    )
                for fmt in _ATOMIC_FORMATS:
                    if should_build(f"{fmt.title()}Atomic"):
                        atomic_file = self.write_format_atomic(
                            atomic_cards, fmt, output_dir / f"{fmt.title()}Atomic.json"
                        )
                        results[f"{fmt.title()}Atomic"] = len(atomic_file.data)

        # Build individual set files
        if outputs is None or not outputs or sets_only:
            from mtgjson5.v2.utils import get_windows_safe_set_code

            LOGGER.info("Building individual set files...")
            set_count = 0
            for code, set_data in self.ctx.sets.iter_sets(set_codes=valid_codes):
                single = IndividualSetFile.from_set_data(set_data, self.ctx.meta)
                safe_code = get_windows_safe_set_code(code)
                single.write(output_dir / f"{safe_code}.json", pretty=self.ctx.pretty)
                set_count += 1
            results["sets"] = set_count

        # Build deck files
        if (
            include_decks
            and (outputs is None or "Decks" in outputs or "DeckList" in outputs)
            and not (sets_only and not outputs)
        ):
            LOGGER.info("Building deck files...")
            deck_count = self.write_decks(output_dir / "decks", set_codes=valid_codes)
            results["decks"] = deck_count

            deck_list = self.ctx.deck_list.build()
            deck_list_file = DeckListFile.with_meta(deck_list, self.ctx.meta)
            deck_list_file.write(output_dir / "DeckList.json", pretty=self.ctx.pretty)
            results["DeckList"] = len(deck_list)

        # Use --price-build flag for dedicated price builds, or --outputs AllPrices
        if outputs and ("AllPrices" in outputs or "AllPricesToday" in outputs):
            LOGGER.info("Building price files...")
            price_results = self.write_prices(output_dir)
            results.update(price_results)

        # Build TcgplayerSkus.json
        if should_build("TcgplayerSkus"):
            LOGGER.info("Building TcgplayerSkus.json...")
            tcgplayer_skus = self.write_tcgplayer_skus(
                output_dir / "TcgplayerSkus.json"
            )
            results["TcgplayerSkus"] = len(tcgplayer_skus.data)

        # Build Keywords.json
        if should_build("Keywords"):
            LOGGER.info("Building Keywords.json...")
            keywords = self.write_keywords(output_dir / "Keywords.json")
            results["Keywords"] = sum(len(v) for v in keywords.data.values())

        # Build CardTypes.json
        if should_build("CardTypes"):
            LOGGER.info("Building CardTypes.json...")
            card_types = self.write_card_types(output_dir / "CardTypes.json")
            results["CardTypes"] = len(card_types.data)

        # Build AllIdentifiers.json
        if should_build("AllIdentifiers"):
            LOGGER.info("Building AllIdentifiers.json...")
            all_identifiers_count = self.write_all_identifiers(
                output_dir / "AllIdentifiers.json"
            )
            results["AllIdentifiers"] = all_identifiers_count

        # Build CompiledList.json
        if should_build("CompiledList"):
            LOGGER.info("Building CompiledList.json...")
            compiled_list = self.write_compiled_list(output_dir / "CompiledList.json")
            results["CompiledList"] = len(compiled_list.data)

        return results

    def write(self) -> None:
        """Write all JSON outputs to the configured output path."""
        self.write_all(self.ctx.output_path)
