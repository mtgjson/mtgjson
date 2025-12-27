"""
MTGJSON assembly utilities.

High-level builders for assembling MTGJSON output files from data sources.
"""

from __future__ import annotations

import pathlib
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from .cards import CardDeck, CardSet, CardToken
from .files import (
    AllPrintingsFile,
    AtomicCardsFile,
    FormatAtomicFile,
    FormatFilter,
    FormatPrintingsFile,
    LegacyAtomicFile,
    LegacyFile,
    ModernAtomicFile,
    ModernFile,
    PauperAtomicFile,
    PioneerAtomicFile,
    PioneerFile,
    SetListFile,
    StandardAtomicFile,
    StandardFile,
    VintageAtomicFile,
    VintageFile,
)
from .serialize import clean_nested
from .sets import SealedProduct


if TYPE_CHECKING:
    import polars as pl
    from polars.dataframe import DataFrame

try:
    import orjson
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False
    pl = None  # type: ignore
    orjson = None  # type: ignore


# =============================================================================
# Assembly Context
# =============================================================================

@dataclass
class AssemblyContext:
    """Shared context for assembly operations."""
    parquet_dir: pathlib.Path
    tokens_dir: pathlib.Path
    set_meta: dict[str, dict[str, Any]]
    meta: dict[str, str]
    
    # Optional pre-loaded data
    decks_df: DataFrame | None = None
    sealed_df: DataFrame | None = None
    booster_configs: dict[str, dict[str, Any]] = field(default_factory=dict)


# =============================================================================
# Set Assembler
# =============================================================================

class SetAssembler:
    """Assembles Set objects from parquet data."""

    def __init__(self, ctx: AssemblyContext):
        self.ctx = ctx

    def get_cards(self, set_code: str) -> list[dict[str, Any]]:
        """Load and serialize cards for a set."""
        if not POLARS_AVAILABLE:
            raise ImportError("Polars required")
        
        path = self.ctx.parquet_dir / f"setCode={set_code}"
        if not path.exists():
            return []

        df = pl.read_parquet(path / "*.parquet")
        models = CardSet.from_dataframe(df)
        return [m.to_polars_dict(exclude_none=True) for m in models]

    def get_tokens(self, set_code: str) -> list[dict[str, Any]]:
        """Load and serialize tokens for a set."""
        if not POLARS_AVAILABLE:
            raise ImportError("Polars required")
        
        meta = self.ctx.set_meta.get(set_code, {})
        token_code = meta.get("tokenSetCode", f"T{set_code}")
        path = self.ctx.tokens_dir / f"setCode={token_code}"

        if not path.exists():
            return []

        df = pl.read_parquet(path / "*.parquet")
        models = CardToken.from_dataframe(df)
        return [m.to_polars_dict(exclude_none=True) for m in models]

    def assemble(
        self,
        set_code: str,
        include_decks: bool = True,
        include_sealed: bool = True,
        include_booster: bool = True,
    ) -> dict[str, Any]:
        """Assemble a complete Set dict."""
        meta = self.ctx.set_meta.get(set_code, {})
        cards = self.get_cards(set_code)
        tokens = self.get_tokens(set_code)

        # Clean translations (remove None values)
        translations_raw = meta.get("translations", {})
        translations = {k: v for k, v in translations_raw.items() if v is not None} if translations_raw else {}

        # Base set structure
        set_data: dict[str, Any] = {
            "baseSetSize": meta.get("baseSetSize") or len([c for c in cards if not c.get("isReprint")]) or len(cards),
            "cards": cards,
            "code": set_code,
            "isFoilOnly": meta.get("isFoilOnly", False),
            "isOnlineOnly": meta.get("isOnlineOnly", False),
            "keyruneCode": meta.get("keyruneCode", set_code),
            "name": meta.get("name", set_code),
            "releaseDate": meta.get("releaseDate", ""),
            "tokens": tokens,
            "totalSetSize": meta.get("totalSetSize") or len(cards),
            "translations": translations,
            "type": meta.get("type", ""),
        }

        # Optional fields (isNonFoilOnly excluded - not in source schema)
        for fld in ["block", "parentCode", "mtgoCode", "tokenSetCode",
                    "tcgplayerGroupId", "cardsphereSetId", "mcmId", "mcmName",
                    "isPaperOnly", "isForeignOnly"]:
            if meta.get(fld) is not None:
                set_data[fld] = meta[fld]

        # Languages from foreign data
        languages = {"English"}
        for card in cards:
            for fd in card.get("foreignData", []):
                if fd.get("language"):
                    languages.add(fd["language"])
        set_data["languages"] = sorted(languages)

        # Booster config
        if include_booster and set_code in self.ctx.booster_configs:
            set_data["booster"] = self.ctx.booster_configs[set_code]

        # Decks (minimal format: code, name, type, and card lists with {count, uuid, isFoil?})
        if include_decks and self.ctx.decks_df is not None:
            set_decks = self.ctx.decks_df.filter(pl.col("setCode") == set_code)
            if len(set_decks) > 0:
                minimal_decks = []
                for deck in set_decks.to_dicts():
                    minimal_deck = {
                        "code": deck.get("code", set_code),
                        "name": deck.get("name", ""),
                        "type": deck.get("type", ""),
                    }
                    if deck.get("releaseDate"):
                        minimal_deck["releaseDate"] = deck["releaseDate"]
                    if deck.get("sealedProductUuids"):
                        minimal_deck["sealedProductUuids"] = deck["sealedProductUuids"]
                    # Keep only minimal card fields {count, uuid, isFoil}
                    for board in ["mainBoard", "sideBoard", "commander"]:
                        cards = deck.get(board)
                        if cards:
                            minimal_deck[board] = [
                                {k: v for k, v in c.items() if k in ("count", "uuid", "isFoil") and v not in (None, False)}
                                for c in cards if isinstance(c, dict)
                            ]
                        else:
                            minimal_deck[board] = []
                    minimal_decks.append(minimal_deck)
                set_data["decks"] = minimal_decks

        # Sealed products
        if include_sealed and self.ctx.sealed_df is not None:
            set_sealed = self.ctx.sealed_df.filter(pl.col("setCode") == set_code)
            if len(set_sealed) > 0:
                models = SealedProduct.from_dataframe(set_sealed.drop("setCode"))
                set_data["sealedProduct"] = [m.to_polars_dict(exclude_none=True) for m in models]

        return set_data

    def iter_sets(
        self,
        set_codes: list[str] | None = None,
        **kwargs: Any,
    ) -> Iterator[tuple[str, dict[str, Any]]]:
        """Iterate over assembled sets."""
        codes = set_codes or sorted(self.ctx.set_meta.keys())
        for code in codes:
            if (self.ctx.parquet_dir / f"setCode={code}").exists():
                yield code, self.assemble(code, **kwargs)


# =============================================================================
# Deck Assembler
# =============================================================================

class DeckAssembler:
    """Assembles Deck objects with expanded card data."""

    def __init__(self, cards_df: DataFrame):
        self.cards_df = cards_df
        self._uuid_index: dict[str, dict[str, Any]] | None = None

    @property
    def uuid_index(self) -> dict[str, dict[str, Any]]:
        """Lazy-build UUID -> card dict index."""
        if self._uuid_index is None:
            models = CardDeck.from_dataframe(self.cards_df)
            self._uuid_index = {
                m.uuid: m.to_polars_dict(exclude_none=True)
                for m in models
            }
        return self._uuid_index

    def expand_card_list(self, refs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Expand card references to full card objects."""
        result = []
        for ref in refs:
            uuid = ref.get("uuid")
            if not uuid:
                continue

            card = self.uuid_index.get(uuid)
            if card is None:
                continue

            expanded = dict(card)
            expanded["count"] = ref.get("count", 1)
            if ref.get("isFoil"):
                expanded["isFoil"] = True
            if ref.get("isEtched"):
                expanded["isEtched"] = True

            result.append(expanded)

        return result

    def assemble(self, deck_data: dict[str, Any]) -> dict[str, Any]:
        """Assemble a complete Deck with expanded cards."""
        result = {
            "code": deck_data.get("code", deck_data.get("setCode", "")),
            "name": deck_data.get("name", ""),
            "type": deck_data.get("type", ""),
            "releaseDate": deck_data.get("releaseDate"),
        }

        if deck_data.get("sealedProductUuids"):
            result["sealedProductUuids"] = deck_data["sealedProductUuids"]

        for board in ["mainBoard", "sideBoard", "commander", "tokens"]:
            refs = deck_data.get(board, [])
            if refs:
                result[board] = self.expand_card_list(refs)
            elif board in ["mainBoard", "sideBoard"]:
                result[board] = []

        return result

    @classmethod
    def from_parquet(
        cls,
        parquet_dir: pathlib.Path,
        set_codes: list[str] | None = None,
    ) -> DeckAssembler:
        """Create assembler with cards loaded from parquet."""
        if not POLARS_AVAILABLE:
            raise ImportError("Polars required")
        
        if set_codes:
            dfs = []
            for code in set_codes:
                path = parquet_dir / f"setCode={code}"
                if path.exists():
                    dfs.append(pl.read_parquet(path / "*.parquet"))
            cards_df = pl.concat(dfs) if dfs else pl.DataFrame()
        else:
            cards_df = pl.read_parquet(parquet_dir / "**/*.parquet")

        return cls(cards_df)


# =============================================================================
# Atomic Cards Assembler
# =============================================================================

class AtomicCardsAssembler:
    """Assembles AtomicCards grouped by name."""

    def __init__(self, ctx: AssemblyContext):
        self.ctx = ctx

    def iter_atomic(self) -> Iterator[tuple[str, list[dict[str, Any]]]]:
        """Iterate over atomic cards grouped by name."""
        if not POLARS_AVAILABLE:
            raise ImportError("Polars required")
        
        from .cards import CardAtomic

        atomic_schema = CardAtomic.polars_schema()
        atomic_fields = set(atomic_schema.keys())

        lf = pl.scan_parquet(self.ctx.parquet_dir / "**/*.parquet")
        available = set(lf.collect_schema().names())
        select_cols = list(atomic_fields & available)

        if "name" not in select_cols:
            select_cols.append("name")

        df = (
            lf
            .select(select_cols)
            .unique(subset=["name", "colorIdentity", "manaCost", "type"])
            .sort("name")
            .collect()
        )

        current_name: str | None = None
        current_cards: list[dict[str, Any]] = []

        for row in df.to_dicts():
            name = row.get("name", "")

            if name != current_name:
                if current_name is not None and current_cards:
                    yield current_name, current_cards
                current_name = name
                current_cards = []

            try:
                model = CardAtomic.model_validate(row)
                current_cards.append(model.to_polars_dict(exclude_none=True))
            except Exception:
                pass

        if current_name is not None and current_cards:
            yield current_name, current_cards

    def assemble(self) -> dict[str, list[dict[str, Any]]]:
        """Assemble complete AtomicCards data dict."""
        return dict(self.iter_atomic())


# =============================================================================
# SetList Assembler
# =============================================================================

class SetListAssembler:
    """Assembles SetList (set summaries without cards)."""

    def __init__(self, ctx: AssemblyContext):
        self.ctx = ctx

    def assemble(self, set_code: str) -> dict[str, Any]:
        """Assemble a single SetList entry."""
        if not POLARS_AVAILABLE:
            raise ImportError("Polars required")
        
        meta = self.ctx.set_meta.get(set_code, {})

        cards_path = self.ctx.parquet_dir / f"setCode={set_code}"
        card_count = 0
        if cards_path.exists():
            card_count = pl.scan_parquet(cards_path / "*.parquet").select(pl.len()).collect().item()

        # Clean translations (remove None values)
        translations_raw = meta.get("translations", {})
        translations = {k: v for k, v in translations_raw.items() if v is not None} if translations_raw else {}

        return {
            "baseSetSize": meta.get("baseSetSize") or card_count,
            "code": set_code,
            "isFoilOnly": meta.get("isFoilOnly", False),
            "isOnlineOnly": meta.get("isOnlineOnly", False),
            "keyruneCode": meta.get("keyruneCode", set_code),
            "name": meta.get("name", set_code),
            "releaseDate": meta.get("releaseDate", ""),
            "totalSetSize": meta.get("totalSetSize") or card_count,
            "translations": translations,
            "type": meta.get("type", ""),
            **{k: meta[k] for k in [
                "block", "parentCode", "mtgoCode", "tokenSetCode",
                "tcgplayerGroupId", "cardsphereSetId", "mcmId", "mcmName",
            ] if meta.get(k) is not None},
        }

    def assemble_all(self) -> list[dict[str, Any]]:
        """Assemble all SetList entries."""
        return [
            self.assemble(code)
            for code in sorted(self.ctx.set_meta.keys())
            if (self.ctx.parquet_dir / f"setCode={code}").exists()
        ]


# =============================================================================
# High-Level File Builder
# =============================================================================

class MtgjsonFileBuilder:
    """Builds all non-sql based MTGJSON output files."""

    def __init__(self, ctx: AssemblyContext):
        self.ctx = ctx
        self.set_assembler = SetAssembler(ctx)

    def build_all_printings(
        self,
        output_path: pathlib.Path,
        set_codes: list[str] | None = None,
        streaming: bool = True,
    ) -> AllPrintingsFile | int:
        """Build AllPrintings.json. Returns count if streaming, else full file."""
        if streaming:
            # Stream directly to disk, return count
            return self._build_all_printings_streaming(output_path, set_codes)

        data: dict[str, dict[str, Any]] = {}
        for code, set_data in self.set_assembler.iter_sets(set_codes=set_codes):
            data[code] = set_data

        file = AllPrintingsFile.with_meta(data, self.ctx.meta)
        file.write(output_path)
        return file

    def _build_all_printings_streaming(
        self,
        output_path: pathlib.Path,
        set_codes: list[str] | None = None,
    ) -> int:
        """Stream AllPrintings.json to disk."""
        if orjson is None:
            raise ImportError("orjson required")
        
        codes = set_codes or sorted(self.ctx.set_meta.keys())

        with output_path.open("wb") as f:
            f.write(b'{"meta":')
            f.write(orjson.dumps(self.ctx.meta))
            f.write(b',"data":{')

            first = True
            count = 0

            for code, set_data in self.set_assembler.iter_sets(set_codes=codes):
                if not first:
                    f.write(b",")
                first = False

                f.write(b'"')
                f.write(code.encode())
                f.write(b'":')
                # Clean None values before serializing
                cleaned = clean_nested(set_data, omit_empty=True)
                f.write(orjson.dumps(cleaned, option=orjson.OPT_SORT_KEYS))
                count += 1

            f.write(b"}}")

        return count

    def build_atomic_cards(self, output_path: pathlib.Path) -> AtomicCardsFile:
        """Build AtomicCards.json."""
        assembler = AtomicCardsAssembler(self.ctx)
        data = assembler.assemble()

        file = AtomicCardsFile.with_meta(data, self.ctx.meta)
        file.write(output_path)
        return file

    def build_set_list(self, output_path: pathlib.Path) -> SetListFile:
        """Build SetList.json."""
        assembler = SetListAssembler(self.ctx)
        data = assembler.assemble_all()

        file = SetListFile.with_meta(data, self.ctx.meta)
        file.write(output_path)
        return file

    def build_format_file(
        self,
        all_printings: AllPrintingsFile,
        format_name: str,
        output_path: pathlib.Path,
    ) -> FormatPrintingsFile:
        """Build format-specific printings file."""
        filter_fn = getattr(FormatFilter, format_name)

        file_class: type[FormatPrintingsFile] = {
            "legacy": LegacyFile,
            "modern": ModernFile,
            "pioneer": PioneerFile,
            "standard": StandardFile,
            "vintage": VintageFile,
        }[format_name]

        file = file_class.from_all_printings(all_printings, filter_fn)
        file.write(output_path)
        return file

    def build_format_atomic(
        self,
        atomic_cards: AtomicCardsFile,
        format_name: str,
        output_path: pathlib.Path,
    ) -> FormatAtomicFile:
        """Build format-specific atomic file."""
        filter_fn = getattr(FormatFilter, format_name)

        file_class: type[FormatAtomicFile] = {
            "legacy": LegacyAtomicFile,
            "modern": ModernAtomicFile,
            "pauper": PauperAtomicFile,
            "pioneer": PioneerAtomicFile,
            "standard": StandardAtomicFile,
            "vintage": VintageAtomicFile,
        }[format_name]

        file = file_class.from_atomic_cards(atomic_cards, filter_fn)
        file.write(output_path)
        return file

    def build_all_files(
        self,
        output_dir: pathlib.Path,
        set_codes: list[str] | None = None,
    ) -> dict[str, int]:
        """Build all MTGJSON output files."""
        output_dir.mkdir(parents=True, exist_ok=True)
        results: dict[str, int] = {}

        # Core files
        all_printings = self.build_all_printings(
            output_dir / "AllPrintings.json",
            set_codes=set_codes,
            streaming=False,
        )
        results["AllPrintings"] = len(all_printings.data)

        atomic_cards = self.build_atomic_cards(output_dir / "AtomicCards.json")
        results["AtomicCards"] = len(atomic_cards.data)

        set_list = self.build_set_list(output_dir / "SetList.json")
        results["SetList"] = len(set_list.data)

        # Format printings files
        for fmt in ["legacy", "modern", "pioneer", "standard", "vintage"]:
            fmt_file = self.build_format_file(
                all_printings, fmt, output_dir / f"{fmt.title()}.json"
            )
            results[fmt.title()] = len(fmt_file.data)

        # Format atomic files
        for fmt in ["legacy", "modern", "pauper", "pioneer", "standard", "vintage"]:
            atomic_file = self.build_format_atomic(
                atomic_cards, fmt, output_dir / f"{fmt.title()}Atomic.json"
            )
            results[f"{fmt.title()}Atomic"] = len(atomic_file.data)

        # Individual set files
        set_count = 0
        for code, set_data in self.set_assembler.iter_sets(set_codes=set_codes):
            from .base import MtgjsonFileBase
            single = MtgjsonFileBase.with_meta(set_data, self.ctx.meta)
            single.write(output_dir / f"{code}.json")
            set_count += 1
        results["sets"] = set_count

        return results
