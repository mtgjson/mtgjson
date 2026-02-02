"""Assembly context for output generation."""

from __future__ import annotations

import contextlib
import json
import pathlib
from dataclasses import dataclass, field
from functools import cached_property
from typing import TYPE_CHECKING, Any

import orjson
import polars as pl

from mtgjson5 import constants
from mtgjson5.classes import MtgjsonMetaObject
from mtgjson5.mtgjson_config import MtgjsonConfig
from mtgjson5.utils import LOGGER


if TYPE_CHECKING:
    from mtgjson5.v2.data import PipelineContext

    from .assemble import (
        AtomicCardsAssembler,
        DeckAssembler,
        DeckListAssembler,
        SetAssembler,
        SetListAssembler,
        TcgplayerSkusAssembler,
    )

# Cache file names for fast-path assembly
CACHE_SET_META = "_assembly_set_meta.json"
CACHE_BOOSTER_CONFIGS = "_assembly_boosters.json"
CACHE_DECKS = "_assembly_decks.parquet"
CACHE_SEALED = "_assembly_sealed.parquet"
CACHE_TOKEN_PRODUCTS = "_assembly_token_products.json"


@dataclass
class AssemblyContext:
    """Shared context for all output format builders."""

    parquet_dir: pathlib.Path
    tokens_dir: pathlib.Path
    set_meta: dict[str, dict[str, Any]]
    meta: dict[str, str]
    decks_df: pl.DataFrame | None = None
    sealed_df: pl.DataFrame | None = None
    booster_configs: dict[str, dict[str, Any]] = field(default_factory=dict)
    token_products: dict[str, list] = field(default_factory=dict)
    output_path: pathlib.Path = field(
        default_factory=lambda: MtgjsonConfig().output_path
    )
    pretty: bool = False

    @classmethod
    def from_pipeline(cls, ctx: PipelineContext) -> AssemblyContext:
        """Build AssemblyContext from PipelineContext."""
        from mtgjson5.v2.pipeline import build_sealed_products_lf, build_set_metadata_df

        parquet_dir = constants.CACHE_PATH / "_parquet"
        tokens_dir = constants.CACHE_PATH / "_parquet_tokens"

        # Build set metadata
        LOGGER.info("Loading set metadata...")
        set_meta_df = build_set_metadata_df(ctx)
        if isinstance(set_meta_df, pl.LazyFrame):
            set_meta_df = set_meta_df.collect()
        set_meta = {row["code"]: row for row in set_meta_df.to_dicts()}

        if ctx._cache is not None:
            translations_by_name = ctx._cache.set_translations
            tcg_overrides = ctx._cache.tcgplayer_set_id_overrides
            keyrune_overrides = ctx._cache.keyrune_code_overrides
            base_set_sizes = ctx._cache.base_set_sizes
            for code, meta in set_meta.items():
                set_name = meta.get("name", "")
                if set_name:
                    raw_translations = translations_by_name.get(set_name, {})
                    meta["translations"] = raw_translations if raw_translations else {}
                else:
                    meta["translations"] = {}
                if code in tcg_overrides:
                    meta["tcgplayerGroupId"] = tcg_overrides[code]
                raw_keyrune = meta.get("keyruneCode", "")
                if raw_keyrune in keyrune_overrides:
                    meta["keyruneCode"] = keyrune_overrides[raw_keyrune]
                if code in base_set_sizes:
                    meta["baseSetSize"] = base_set_sizes[code]

        for code, meta in set_meta.items():
            card_path = parquet_dir / f"setCode={code}"
            if card_path.exists():
                card_df = pl.read_parquet(card_path / "*.parquet")
                if "isRebalanced" in card_df.columns:
                    total = card_df.filter(
                        ~pl.col("isRebalanced").fill_null(False)
                    ).height
                else:
                    total = card_df.height
                meta["totalSetSize"] = total

        for code, meta in set_meta.items():
            set_type = meta.get("type", "")
            parent_code = meta.get("parentCode")
            # S/F prefixes identify special token sets:
            # S = Substitute/special token sets (e.g., SBRO, SMKM)
            # F = Japanese promo token sets (e.g., F18, F20)
            if (
                parent_code
                and (code.startswith("S") or code.startswith("F"))
                and set_type in ("token", "memorabilia")
            ):
                meta["tokenSetCode"] = code
            else:
                t_code_path = tokens_dir / f"setCode=T{code}"
                code_path = tokens_dir / f"setCode={code}"
                if t_code_path.exists():
                    meta["tokenSetCode"] = f"T{code}"
                elif code_path.exists():
                    meta["tokenSetCode"] = code
                else:
                    meta["tokenSetCode"] = None

        LOGGER.info("Loading deck data...")
        decks_df = ctx.decks_lf
        if decks_df is not None and isinstance(decks_df, pl.LazyFrame):
            decks_df = decks_df.collect()  # type: ignore[assignment]

        LOGGER.info("Loading sealed products...")
        sealed_df = build_sealed_products_lf(ctx)
        if isinstance(sealed_df, pl.LazyFrame):
            sealed_df = sealed_df.collect()  # type: ignore[assignment]

        booster_configs: dict[str, dict] = {}
        for code, meta in set_meta.items():
            booster_raw = meta.get("booster")
            if booster_raw and isinstance(booster_raw, str):
                with contextlib.suppress(orjson.JSONDecodeError):
                    booster_configs[code] = orjson.loads(booster_raw)
            elif isinstance(booster_raw, dict):
                booster_configs[code] = booster_raw

        # Load token products lookup (uuid -> list of product dicts)
        LOGGER.info("Loading token products...")
        token_products: dict[str, list] = {}
        tp_lf = ctx.token_products_lf
        if tp_lf is not None:
            tp_df = tp_lf.collect() if isinstance(tp_lf, pl.LazyFrame) else tp_lf
            if not tp_df.is_empty():
                uuids = tp_df["uuid"].to_list()
                raw_products = tp_df["tokenProducts"].to_list()
                token_products = {
                    uuid: json.loads(raw)
                    for uuid, raw in zip(uuids, raw_products)
                    if uuid and raw
                }

        meta_obj = MtgjsonMetaObject()
        meta_dict = {"date": meta_obj.date, "version": meta_obj.version}

        return cls(
            parquet_dir=parquet_dir,
            tokens_dir=tokens_dir,
            set_meta=set_meta,
            meta=meta_dict,
            decks_df=decks_df,  # type: ignore[arg-type]
            sealed_df=sealed_df,  # type: ignore[arg-type]
            booster_configs=booster_configs,
            token_products=token_products,
        )

    @classmethod
    def from_cache(
        cls, cache_dir: pathlib.Path | None = None
    ) -> AssemblyContext | None:
        """Load AssemblyContext from cached files (fast path).

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

        # Load set metadata
        set_meta: dict[str, dict[str, Any]] = orjson.loads(set_meta_path.read_bytes())
        LOGGER.info("Loading cached set metadata...")
        LOGGER.debug(f"Loaded cached set metadata for {len(set_meta)} sets.")

        # Load booster configs
        booster_configs: dict[str, dict[str, Any]] = {}
        if boosters_path.exists():
            booster_configs = orjson.loads(boosters_path.read_bytes())
        LOGGER.info("Loaded assembly cache.")
        LOGGER.debug(
            f"Loaded cached booster configurations for {len(booster_configs)} sets."
        )

        # Load decks DataFrame
        decks_df: pl.DataFrame | None = None
        if decks_path.exists():
            decks_df = pl.read_parquet(decks_path)
        LOGGER.info("Loading cached deck data...")
        LOGGER.debug(
            f"Loaded cached decks DataFrame with {len(decks_df) if decks_df is not None else 0} rows."
        )

        # Load sealed products DataFrame
        sealed_df: pl.DataFrame | None = None
        if sealed_path.exists():
            sealed_df = pl.read_parquet(sealed_path)
        LOGGER.info("Loading cached sealed products...")
        LOGGER.debug(
            f"Loaded cached sealed products DataFrame with {len(sealed_df) if sealed_df is not None else 0} rows."
        )

        # Load token products
        token_products_path = cache_dir / CACHE_TOKEN_PRODUCTS
        token_products: dict[str, list] = {}
        if token_products_path.exists():
            token_products = orjson.loads(token_products_path.read_bytes())
        LOGGER.info("Loading cached token products...")
        LOGGER.debug(f"Loaded cached token products for {len(token_products)} tokens.")

        # Create meta dict
        meta_obj = MtgjsonMetaObject()
        meta_dict = {"date": meta_obj.date, "version": meta_obj.version}
        LOGGER.info("Loading meta information...")
        LOGGER.debug(f"Loaded meta: {meta_dict}")

        return cls(
            parquet_dir=parquet_dir,
            tokens_dir=tokens_dir,
            set_meta=set_meta,
            meta=meta_dict,
            decks_df=decks_df,
            sealed_df=sealed_df,
            booster_configs=booster_configs,
            token_products=token_products,
        )

    def save_cache(self, cache_dir: pathlib.Path | None = None) -> None:
        """Save context to cache for fast rebuilds."""
        if cache_dir is None:
            cache_dir = constants.CACHE_PATH
        cache_dir.mkdir(parents=True, exist_ok=True)
        LOGGER.info("Saving assembly cache...")

        # Save set metadata (convert any non-serializable values)
        set_meta_path = cache_dir / CACHE_SET_META
        cleaned_meta: dict[str, Any] = {}
        for code, meta in self.set_meta.items():
            cleaned_meta[code] = {
                k: v
                for k, v in meta.items()
                if v is not None and not isinstance(v, pl.DataFrame | pl.LazyFrame)
            }
        set_meta_path.write_bytes(orjson.dumps(cleaned_meta))
        LOGGER.info("Saved set metadata to assembly cache.")
        LOGGER.debug(
            f"Saved cached set metadata for {len(cleaned_meta)} sets to {set_meta_path}"
        )

        # Save booster configs
        boosters_path = cache_dir / CACHE_BOOSTER_CONFIGS
        boosters_path.write_bytes(orjson.dumps(self.booster_configs))
        LOGGER.info("Saved booster configurations to assembly cache.")
        LOGGER.debug(
            f"Saved cached booster configurations for {len(self.booster_configs)} sets to {boosters_path}"
        )

        # Save decks DataFrame
        if self.decks_df is not None and len(self.decks_df) > 0:
            decks_path = cache_dir / CACHE_DECKS
            self.decks_df.write_parquet(decks_path)
            LOGGER.info("Saved decks DataFrame to assembly cache.")
            LOGGER.debug(
                f"Saved cached decks DataFrame with {len(self.decks_df)} rows to {decks_path}"
            )

        # Save sealed products DataFrame
        if self.sealed_df is not None and len(self.sealed_df) > 0:
            sealed_path = cache_dir / CACHE_SEALED
            self.sealed_df.write_parquet(sealed_path)
            LOGGER.info("Saved sealed products DataFrame to assembly cache.")
            LOGGER.debug(
                f"Saved cached sealed products DataFrame with {len(self.sealed_df)} rows to {sealed_path}"
            )

        # Save token products
        if self.token_products:
            token_products_path = cache_dir / CACHE_TOKEN_PRODUCTS
            token_products_path.write_bytes(orjson.dumps(self.token_products))
            LOGGER.info("Saved token products to assembly cache.")
            LOGGER.debug(
                f"Saved cached token products for {len(self.token_products)} tokens to {token_products_path}"
            )

        LOGGER.info("Assembly cache saved successfully.")

    # =========================================================================
    # Assembler Properties
    # =========================================================================

    @cached_property
    def sets(self) -> SetAssembler:
        """Assembler for complete Set objects."""
        from .assemble import SetAssembler

        return SetAssembler(self)

    @cached_property
    def atomic_cards(self) -> AtomicCardsAssembler:
        """Assembler for AtomicCards grouped by name."""
        from .assemble import AtomicCardsAssembler

        return AtomicCardsAssembler(self)

    @cached_property
    def set_list(self) -> SetListAssembler:
        """Assembler for SetList summaries."""
        from .assemble import SetListAssembler

        return SetListAssembler(self)

    @cached_property
    def deck_list(self) -> DeckListAssembler:
        """Assembler for DeckList summaries."""
        from .assemble import DeckListAssembler

        return DeckListAssembler(self)

    def deck_assembler(self) -> DeckAssembler:
        """Create a DeckAssembler for expanding deck card/token references."""
        from .assemble import DeckAssembler

        return DeckAssembler(self)

    @cached_property
    def tcgplayer_skus(self) -> TcgplayerSkusAssembler:
        """Assembler for TcgplayerSkus.json."""
        from .assemble import TcgplayerSkusAssembler

        return TcgplayerSkusAssembler(self)
