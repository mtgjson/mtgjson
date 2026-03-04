"""Parquet output builder for MTGJSON."""

from __future__ import annotations

import pathlib
from typing import TYPE_CHECKING, Any, Literal

import polars as pl

from mtgjson5.models.containers import MtgjsonMeta
from mtgjson5.utils import LOGGER

if TYPE_CHECKING:
    from ..context import AssemblyContext

_COMPRESSION: Literal["zstd"] = "zstd"
_COMPRESSION_LEVEL = 9


def _write(df: pl.DataFrame, path: pathlib.Path) -> None:
    """Write a DataFrame to parquet with standard compression."""
    df.write_parquet(path, compression=_COMPRESSION, compression_level=_COMPRESSION_LEVEL)
    LOGGER.info(f"  {path.name}: {df.height:,} rows")


class ParquetBuilder:
    """Builds Parquet file exports."""

    def __init__(self, ctx: AssemblyContext):
        self.ctx = ctx

    # ------------------------------------------------------------------
    # Small / enum-like files
    # ------------------------------------------------------------------

    def _write_keywords(self, output_dir: pathlib.Path) -> None:
        """Write Keywords.parquet (category, keyword)."""
        from ..assemble import KeywordsAssembler

        data = KeywordsAssembler(self.ctx).build()
        rows: list[dict[str, str]] = []
        for category, words in data.items():
            for word in words:
                rows.append({"category": category, "keyword": word})
        if rows:
            _write(pl.DataFrame(rows), output_dir / "Keywords.parquet")

    def _write_card_types(self, output_dir: pathlib.Path) -> None:
        """Write CardTypes.parquet (type, kind, value) — one row per sub/super type."""
        from ..assemble import CardTypesAssembler

        data = CardTypesAssembler(self.ctx).build()
        rows: list[dict[str, str]] = []
        for card_type, info in data.items():
            for kind in ("subTypes", "superTypes"):
                for val in info.get(kind, []):
                    rows.append({"type": card_type, "kind": kind, "value": val})
        if rows:
            _write(pl.DataFrame(rows), output_dir / "CardTypes.parquet")

    def _write_enum_values(self, output_dir: pathlib.Path) -> None:
        """Write EnumValues.parquet (category, field, value)."""
        from ..assemble import EnumValuesAssembler

        data = EnumValuesAssembler(self.ctx).build()
        rows: list[dict[str, str]] = []
        for category, fields in data.items():
            if isinstance(fields, dict):
                for field, values in fields.items():
                    for val in values:
                        rows.append({"category": category, "field": field, "value": val})
        if rows:
            _write(pl.DataFrame(rows), output_dir / "EnumValues.parquet")

    def _write_deck_list(self, output_dir: pathlib.Path) -> None:
        """Write DeckList.parquet."""
        data = self.ctx.deck_list.build()
        if data:
            _write(pl.DataFrame(data), output_dir / "DeckList.parquet")

    def _write_all_decks(self, output_dir: pathlib.Path) -> None:
        """Write AllDecks.parquet — one row per card-in-deck.

        Explodes each board's List[Struct{uuid, count, isFoil, isEtched}]
        into flat rows with a ``board`` column. Join with cards.parquet on
        ``uuid`` to get full card data.
        """
        decks_df = self.ctx.decks_df
        if decks_df is None or decks_df.is_empty():
            return

        # Normalise column name
        if "setCode" in decks_df.columns and "code" not in decks_df.columns:
            decks_df = decks_df.rename({"setCode": "code"})

        meta_cols = [c for c in ("code", "name", "type", "releaseDate") if c in decks_df.columns]
        board_cols = [
            "mainBoard",
            "sideBoard",
            "commander",
            "displayCommander",
            "tokens",
            "planes",
            "schemes",
        ]
        available = [c for c in board_cols if c in decks_df.columns]

        dfs: list[pl.DataFrame] = []
        for board in available:
            board_df = (
                decks_df.select([*meta_cols, board])
                .filter(pl.col(board).list.len() > 0)
                .explode(board)
                .unnest(board)
                .with_columns(pl.lit(board).alias("board"))
            )
            dfs.append(board_df)

        if dfs:
            result = pl.concat(dfs, how="diagonal")
            _write(result, output_dir / "AllDecks.parquet")

    def _write_tcgplayer_skus(self, output_dir: pathlib.Path) -> None:
        """Write TcgplayerSkus.parquet (uuid + flattened SKU fields)."""
        data = self.ctx.tcgplayer_skus.build()
        rows: list[dict[str, Any]] = []
        for uuid, skus in data.items():
            for sku in skus:
                row = {"uuid": uuid}
                row.update(sku)
                rows.append(row)
        if rows:
            _write(pl.DataFrame(rows), output_dir / "TcgplayerSkus.parquet")

    def _write_prices(self, output_dir: pathlib.Path) -> None:
        """Write AllPrices.parquet and AllPricesToday.parquet."""
        from mtgjson5.build.price_builder import PolarsPriceBuilder

        builder = PolarsPriceBuilder()
        all_prices_df, today_df = builder.build_prices_parquet()

        if len(all_prices_df) > 0:
            _write(all_prices_df, output_dir / "AllPrices.parquet")
        if len(today_df) > 0:
            _write(today_df, output_dir / "AllPricesToday.parquet")

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def write(self, output_dir: pathlib.Path | None = None) -> pathlib.Path | None:
        """Write Parquet files to output directory."""
        if output_dir is None:
            output_dir = self.ctx.output_path / "parquet"

        output_dir.mkdir(parents=True, exist_ok=True)

        tables = self.ctx.normalized_tables
        if not tables:
            return None

        # Write normalized relational tables
        for name, df in tables.items():
            if df is not None and len(df) > 0:
                _write(df, output_dir / f"{name}.parquet")

        # Write AllPrintings (full cards with nested structures)
        cards_df = self.ctx.all_cards_df
        if cards_df is not None:
            _write(cards_df, output_dir / "AllPrintings.parquet")

        # Write meta
        meta = MtgjsonMeta()
        meta_df = pl.DataFrame({"date": [meta.date], "version": [meta.version]})
        _write(meta_df, output_dir / "meta.parquet")

        # Write booster tables
        for name, df in self.ctx.normalized_boosters.items():
            if df is not None and len(df) > 0:
                _write(df, output_dir / f"{name}.parquet")

        # Write additional files
        self._write_keywords(output_dir)
        self._write_card_types(output_dir)
        self._write_enum_values(output_dir)
        self._write_deck_list(output_dir)
        self._write_all_decks(output_dir)
        self._write_tcgplayer_skus(output_dir)
        self._write_prices(output_dir)

        LOGGER.info(f"Wrote Parquet files to {output_dir}")
        return output_dir
