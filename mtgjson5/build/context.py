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
from mtgjson5.models.containers import MtgjsonMeta
from mtgjson5.mtgjson_config import MtgjsonConfig
from mtgjson5.utils import LOGGER

if TYPE_CHECKING:
    from mtgjson5.data import PipelineContext

    from .assemble import (
        AllIdentifiersAssembler,
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


def _enrich_set_metadata(
    ctx: PipelineContext,
    set_meta: dict[str, dict[str, Any]],
    parquet_dir: pathlib.Path,
    tokens_dir: pathlib.Path,
) -> None:
    """Enrich set metadata with translations, overrides, sizes, and token set codes."""
    from mtgjson5.polars_utils import get_windows_safe_set_code

    if ctx._cache is not None:
        translations_by_name = ctx._cache.set_translations
        tcg_overrides = ctx._cache.tcgplayer_set_id_overrides
        keyrune_overrides = ctx._cache.keyrune_code_overrides
        base_set_sizes = ctx._cache.base_set_sizes
        # Use a consistent schema for translations so Polars infers
        # Struct fields even when the first sets have no translations.
        empty_translations = {
            "Chinese Simplified": None,
            "Chinese Traditional": None,
            "French": None,
            "German": None,
            "Italian": None,
            "Japanese": None,
            "Korean": None,
            "Portuguese (Brazil)": None,
            "Russian": None,
            "Spanish": None,
        }
        for code, meta in set_meta.items():
            set_name = meta.get("name", "")
            if set_name:
                raw_translations = translations_by_name.get(set_name, {})
                meta["translations"] = raw_translations if raw_translations else empty_translations
            else:
                meta["translations"] = empty_translations
            if code in tcg_overrides:
                meta["tcgplayerGroupId"] = tcg_overrides[code]
            raw_keyrune = meta.get("keyruneCode", "")
            if raw_keyrune in keyrune_overrides:
                meta["keyruneCode"] = keyrune_overrides[raw_keyrune]
            if code in base_set_sizes:
                meta["baseSetSize"] = base_set_sizes[code]

    if parquet_dir.exists() and any(parquet_dir.iterdir()):
        try:
            sizes_df = (
                pl.scan_parquet(
                    parquet_dir / "**/*.parquet",
                    cast_options=pl.ScanCastOptions(missing_struct_fields="insert"),
                )
                .select(["setCode", "number", "isRebalanced"])
                .filter(~pl.col("isRebalanced").fill_null(False))
                .group_by("setCode")
                .agg(pl.col("number").n_unique().alias("totalSetSize"))
                .collect()
            )
            size_map = {r["setCode"]: r["totalSetSize"] for r in sizes_df.iter_rows(named=True)}
            for code, meta in set_meta.items():
                if code.upper() in size_map:
                    meta["totalSetSize"] = size_map[code.upper()]
        except Exception as e:
            LOGGER.warning(f"Batch set size computation failed, falling back to per-set: {e}")
            for code, meta in set_meta.items():
                safe_code = get_windows_safe_set_code(code)
                card_path = parquet_dir / f"setCode={safe_code}"
                if card_path.exists():
                    card_df = pl.read_parquet(card_path / "*.parquet", columns=["number", "isRebalanced"])
                    if "isRebalanced" in card_df.columns:
                        card_df = card_df.filter(~pl.col("isRebalanced").fill_null(False))
                    if "number" in card_df.columns:
                        meta["totalSetSize"] = card_df["number"].n_unique()
                    else:
                        meta["totalSetSize"] = card_df.height

    for code, meta in set_meta.items():
        set_type = meta.get("type", "")
        parent_code = meta.get("parentCode")
        # S/F prefixes identify special token sets:
        # S = Substitute/special token sets (e.g., SBRO, SMKM)
        # F = Japanese promo token sets (e.g., F18, F20)
        if parent_code and (code.startswith(("S", "F"))) and set_type in ("token", "memorabilia"):
            meta["tokenSetCode"] = code
        else:
            safe_t_code = get_windows_safe_set_code(f"T{code}")
            safe_code = get_windows_safe_set_code(code)
            t_code_path = tokens_dir / f"setCode={safe_t_code}"
            code_path = tokens_dir / f"setCode={safe_code}"
            if t_code_path.exists():
                meta["tokenSetCode"] = f"T{code}"
            elif code_path.exists():
                meta["tokenSetCode"] = code
            else:
                meta["tokenSetCode"] = None


def _enrich_sets_with_sealed(
    records: list[dict[str, Any]],
    sealed_df: pl.DataFrame | None,
) -> None:
    """Attach sealedProduct lists to set records from sealed_df."""
    if sealed_df is None or sealed_df.is_empty():
        return

    from mtgjson5.models.sealed import SealedProduct

    # Group sealed products by setCode
    sealed_by_set: dict[str, list[dict[str, Any]]] = {}
    for code, group_df in sealed_df.group_by("setCode"):
        set_code = code[0] if isinstance(code, tuple) else code
        models = SealedProduct.from_dataframe(group_df)
        products = []
        for m in models:
            d = m.to_polars_dict(exclude_none=False)
            d.pop("setCode", None)
            d.pop("language", None)
            # Pre-serialize contents to JSON string to avoid Polars struct inference issues
            if "contents" in d and d["contents"] is not None:
                d["contents"] = orjson.dumps(d["contents"]).decode("utf-8")
            products.append(d)
        sealed_by_set[set_code] = products

    for rec in records:
        rec["sealedProduct"] = sealed_by_set.get(rec.get("code", ""), [])


def _enrich_sets_with_decks(
    records: list[dict[str, Any]],
    decks_df: pl.DataFrame | None,
) -> None:
    """Attach minimal deck lists to set records from decks_df."""
    if decks_df is None or decks_df.is_empty():
        return

    # Group decks by setCode and build minimal deck dicts
    decks_by_set: dict[str, list[dict[str, Any]]] = {}
    for row in decks_df.to_dicts():
        set_code = row.get("setCode", "")
        minimal: dict[str, Any] = {
            "code": row.get("code", set_code),
            "name": row.get("name", ""),
            "type": row.get("type", ""),
            "releaseDate": row.get("releaseDate"),
            "source": row.get("source"),
            "sealedProductUuids": orjson.dumps(row.get("sealedProductUuids") or []).decode("utf-8"),
            "sourceSetCodes": orjson.dumps(row.get("sourceSetCodes") or []).decode("utf-8"),
        }
        # Boards that include isEtched - pre-serialized to JSON strings
        # to avoid Polars struct schema inference issues
        for board in ["mainBoard", "sideBoard", "commander", "displayCommander"]:
            cards_list = row.get(board)
            if cards_list and isinstance(cards_list, list):
                filtered = [
                    {
                        k: v
                        for k, v in c.items()
                        if k in ("count", "uuid", "isFoil", "isEtched") and v not in (None, False)
                    }
                    for c in cards_list
                    if isinstance(c, dict)
                ]
                minimal[board] = orjson.dumps(filtered).decode("utf-8")
            else:
                minimal[board] = "[]"
        # Boards that exclude isEtched
        for board in ["tokens", "planes", "schemes"]:
            cards_list = row.get(board)
            if cards_list and isinstance(cards_list, list):
                filtered = [
                    {k: v for k, v in c.items() if k in ("count", "uuid", "isFoil") and v not in (None, False)}
                    for c in cards_list
                    if isinstance(c, dict)
                ]
                minimal[board] = orjson.dumps(filtered).decode("utf-8")
            else:
                minimal[board] = "[]"
        decks_by_set.setdefault(set_code, []).append(minimal)

    for rec in records:
        rec["decks"] = decks_by_set.get(rec.get("code", ""), [])


def _build_languages_by_set(cards_df: pl.DataFrame | None) -> dict[str, list[str]]:
    """Compute sorted language lists per setCode from card foreignData.

    Returns dict mapping setCode to sorted list of languages (always includes English).
    """
    if cards_df is None or cards_df.is_empty():
        return {}
    if "foreignData" not in cards_df.columns or "setCode" not in cards_df.columns:
        return {}

    # Bulk language extraction: explode foreignData, extract language, group by setCode
    try:
        lang_df = (
            cards_df.lazy()
            .select("setCode", "foreignData")
            .filter(pl.col("foreignData").list.len() > 0)
            .explode("foreignData")
            .with_columns(pl.col("foreignData").struct.field("language").alias("language"))
            .filter(pl.col("language").is_not_null() & (pl.col("language") != ""))
            .select("setCode", "language")
            .unique()
            .group_by("setCode")
            .agg(pl.col("language"))
            .collect()
        )
    except Exception as e:
        LOGGER.warning(f"Language extraction failed, sets will default to English: {e}")
        return {}

    result: dict[str, list[str]] = {}
    for row in lang_df.iter_rows(named=True):
        langs = set(row["language"]) | {"English"}
        result[row["setCode"]] = sorted(langs)

    # Also ensure sets that have cards but no foreignData get English
    all_set_codes = cards_df.select("setCode").unique().to_series().to_list()
    for code in all_set_codes:
        if code not in result:
            result[code] = ["English"]

    return result


def _load_scryfall_catalogs(
    ctx: PipelineContext,
) -> tuple[dict[str, list[str]], dict[str, list[str]], list[str], list[str]]:
    """Load Scryfall keyword and card type catalog data from cache."""
    keyword_data: dict[str, list[str]] = {}
    card_type_data: dict[str, list[str]] = {}
    super_types: list[str] = []
    planar_types: list[str] = []
    if ctx._cache is not None:
        keyword_data = {
            "abilityWords": sorted(ctx._cache.ability_words),
            "keywordAbilities": sorted(ctx._cache.keyword_abilities),
            "keywordActions": sorted(ctx._cache.keyword_actions),
        }
        card_type_data = ctx._cache.card_type_subtypes
        super_types = ctx._cache.super_types
        planar_types = ctx._cache.planar_types
    return keyword_data, card_type_data, super_types, planar_types


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
    output_path: pathlib.Path = field(default_factory=lambda: MtgjsonConfig().output_path)
    pretty: bool = False
    # Scryfall catalog data (loaded in GlobalCache, passed through for assemblers)
    keyword_data: dict[str, list[str]] = field(default_factory=dict)
    card_type_data: dict[str, list[str]] = field(default_factory=dict)
    super_types: list[str] = field(default_factory=list)
    planar_types: list[str] = field(default_factory=list)

    def validate_assembly_data(self, skip: frozenset[str] = frozenset()) -> None:
        """Validate that expected assembly data is present.

        Logs warnings for any missing data that will result in empty output
        fields (sealedProduct, booster, decks, sourceProducts).

        Args:
            skip: Field names that were intentionally skipped during loading.
        """
        issues: list[str] = []

        if "sealed" not in skip and (self.sealed_df is None or len(self.sealed_df) == 0):
            issues.append("sealed_df is empty — sealedProduct will be missing from all sets")

        if "boosters" not in skip and not self.booster_configs:
            issues.append("booster_configs is empty — booster will be missing from all sets")

        if "decks" not in skip and (self.decks_df is None or len(self.decks_df) == 0):
            issues.append("decks_df is empty — decks will be missing from all sets")

        if not self.set_meta:
            issues.append("set_meta is empty — no sets will be assembled")

        if not self.parquet_dir.exists():
            issues.append(f"parquet_dir does not exist: {self.parquet_dir}")

        for issue in issues:
            LOGGER.warning(f"Assembly validation: {issue}")

        if issues:
            LOGGER.error(
                f"Assembly validation found {len(issues)} issue(s). "
                "Output may be incomplete. Check GitHub data provider logs above."
            )

    @cached_property
    def all_cards_df(self) -> pl.DataFrame | None:
        """Load all cards from parquet cache (shared across format builders)."""
        if not self.parquet_dir.exists():
            LOGGER.error("No parquet cache found. Run build_cards() first.")
            return None
        return pl.read_parquet(self.parquet_dir / "**/*.parquet")

    @cached_property
    def all_tokens_df(self) -> pl.DataFrame | None:
        """Load all tokens from parquet cache (shared across format builders)."""
        if not self.tokens_dir.exists():
            return None
        return pl.read_parquet(self.tokens_dir / "**/*.parquet")

    @cached_property
    def sets_df(self) -> pl.DataFrame | None:
        """Sets metadata as DataFrame (shared across format builders).

        Filters out traditional token sets (type='token' AND code starts with 'T')
        to match CDN reference.

        Note: accesses self.all_cards_df for language computation. When accessed
        via normalized_tables, all_cards_df is already cached. If accessed
        independently, this will trigger loading all_cards_df as a side effect.
        """
        if not self.set_meta:
            return None
        schema_overrides = {
            "isOnlineOnly": pl.Boolean,
            "isFoilOnly": pl.Boolean,
            "isNonFoilOnly": pl.Boolean,
            "isForeignOnly": pl.Boolean,
            "isPaperOnly": pl.Boolean,
            "isPartialPreview": pl.Boolean,
        }
        # Ensure sparse boolean keys exist in every dict — Polars drops
        # schema_overrides columns when the key is absent from most rows.
        records = list(self.set_meta.values())
        for rec in records:
            for key in schema_overrides:
                rec.setdefault(key, None)
        # Enrich with sealed products, decks, and languages
        _enrich_sets_with_sealed(records, self.sealed_df)
        _enrich_sets_with_decks(records, self.decks_df)
        languages_map = _build_languages_by_set(self.all_cards_df)
        for rec in records:
            rec["languages"] = languages_map.get(rec.get("code", ""), ["English"])
        df = pl.DataFrame(records, schema_overrides=schema_overrides)
        if "type" in df.columns:
            is_traditional_token = (pl.col("type") == "token") & pl.col("code").str.starts_with("T")
            df = df.filter(~is_traditional_token)
        return df

    @cached_property
    def normalized_tables(self) -> dict[str, pl.DataFrame]:
        """Normalized relational tables shared across sqlite/csv/parquet/mysql builders."""
        from .assemble import TableAssembler

        cards_df = self.all_cards_df
        if cards_df is None:
            return {}
        return TableAssembler.build_all(cards_df, self.all_tokens_df, self.sets_df)

    @cached_property
    def normalized_boosters(self) -> dict[str, pl.DataFrame]:
        """Booster tables shared across format builders."""
        from .assemble import TableAssembler

        if self.booster_configs:
            return TableAssembler.build_boosters(self.booster_configs)
        return {}

    @classmethod
    def from_pipeline(cls, ctx: PipelineContext) -> AssemblyContext:
        """Build AssemblyContext from PipelineContext."""
        from mtgjson5.pipeline import build_sealed_products_lf, build_set_metadata_df

        parquet_dir = constants.CACHE_PATH / "_parquet"
        tokens_dir = constants.CACHE_PATH / "_parquet_tokens"

        # Build set metadata
        LOGGER.info("Loading set metadata...")
        set_meta_df = build_set_metadata_df(ctx)
        if isinstance(set_meta_df, pl.LazyFrame):
            set_meta_df = set_meta_df.collect()
        set_meta = {row["code"]: row for row in set_meta_df.to_dicts()}

        _enrich_set_metadata(ctx, set_meta, parquet_dir, tokens_dir)

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
                    uuid: json.loads(raw) for uuid, raw in zip(uuids, raw_products, strict=False) if uuid and raw
                }

        meta_obj = MtgjsonMeta()
        meta_dict = {"date": meta_obj.date, "version": meta_obj.version}

        keyword_data, card_type_data, super_types, planar_types = _load_scryfall_catalogs(ctx)

        instance = cls(
            parquet_dir=parquet_dir,
            tokens_dir=tokens_dir,
            set_meta=set_meta,
            meta=meta_dict,
            decks_df=decks_df,  # type: ignore[arg-type]
            sealed_df=sealed_df,  # type: ignore[arg-type]
            booster_configs=booster_configs,
            token_products=token_products,
            keyword_data=keyword_data,
            card_type_data=card_type_data,
            super_types=super_types,
            planar_types=planar_types,
        )
        instance.validate_assembly_data()
        return instance

    @classmethod
    def from_cache(
        cls,
        cache_dir: pathlib.Path | None = None,
        skip: frozenset[str] = frozenset(),
    ) -> AssemblyContext | None:
        """Load AssemblyContext from cached files (fast path).

        Args:
            cache_dir: Override cache directory (default: CACHE_PATH).
            skip: Optional set of field names to skip loading.  Valid names:
                ``"decks"``, ``"sealed"``, ``"token_products"``, ``"boosters"``.
                Skipped fields are left at their empty defaults, saving memory
                in subprocesses that don't need them.

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

        # Load set metadata (always required)
        set_meta: dict[str, dict[str, Any]] = orjson.loads(set_meta_path.read_bytes())
        LOGGER.info("Loading cached set metadata...")

        # Load booster configs
        booster_configs: dict[str, dict[str, Any]] = {}
        if "boosters" not in skip and boosters_path.exists():
            booster_configs = orjson.loads(boosters_path.read_bytes())
        LOGGER.info("Loaded assembly cache.")

        # Load decks DataFrame
        decks_df: pl.DataFrame | None = None
        if "decks" not in skip and decks_path.exists():
            decks_df = pl.read_parquet(decks_path)
            LOGGER.info("Loading cached deck data...")

        # Load sealed products DataFrame
        sealed_df: pl.DataFrame | None = None
        if "sealed" not in skip and sealed_path.exists():
            sealed_df = pl.read_parquet(sealed_path)
            LOGGER.info("Loading cached sealed products...")

        # Load token products
        token_products_path = cache_dir / CACHE_TOKEN_PRODUCTS
        token_products: dict[str, list] = {}
        if "token_products" not in skip and token_products_path.exists():
            token_products = orjson.loads(token_products_path.read_bytes())
            LOGGER.info("Loading cached token products...")

        keyword_data: dict[str, list[str]] = {}
        card_type_data: dict[str, list[str]] = {}
        super_types: list[str] = []
        planar_types: list[str] = []
        keywords_cache = cache_dir / "scryfall_keywords.json"
        types_cache = cache_dir / "scryfall_card_types.json"
        if keywords_cache.exists():
            data = orjson.loads(keywords_cache.read_bytes())
            keyword_data = {
                "abilityWords": sorted(data.get("ability_words", [])),
                "keywordAbilities": sorted(data.get("keyword_abilities", [])),
                "keywordActions": sorted(data.get("keyword_actions", [])),
            }
        if types_cache.exists():
            data = orjson.loads(types_cache.read_bytes())
            card_type_data = data.get("subtypes", {})
            super_types = data.get("super_types", [])
            planar_types = data.get("planar_types", [])

        # Create meta dict
        meta_obj = MtgjsonMeta()
        meta_dict = {"date": meta_obj.date, "version": meta_obj.version}
        LOGGER.info("Loading meta information...")

        instance = cls(
            parquet_dir=parquet_dir,
            tokens_dir=tokens_dir,
            set_meta=set_meta,
            meta=meta_dict,
            decks_df=decks_df,
            sealed_df=sealed_df,
            booster_configs=booster_configs,
            token_products=token_products,
            keyword_data=keyword_data,
            card_type_data=card_type_data,
            super_types=super_types,
            planar_types=planar_types,
        )
        instance.validate_assembly_data(skip=skip)
        return instance

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
                k: v for k, v in meta.items() if v is not None and not isinstance(v, pl.DataFrame | pl.LazyFrame)
            }
        set_meta_path.write_bytes(orjson.dumps(cleaned_meta))
        LOGGER.info("Saved set metadata to assembly cache.")
        LOGGER.debug(f"Saved cached set metadata for {len(cleaned_meta)} sets to {set_meta_path}")

        # Save booster configs
        boosters_path = cache_dir / CACHE_BOOSTER_CONFIGS
        boosters_path.write_bytes(orjson.dumps(self.booster_configs))
        LOGGER.info("Saved booster configurations to assembly cache.")
        LOGGER.debug(f"Saved cached booster configurations for {len(self.booster_configs)} sets to {boosters_path}")

        # Save decks DataFrame
        if self.decks_df is not None and len(self.decks_df) > 0:
            decks_path = cache_dir / CACHE_DECKS
            self.decks_df.write_parquet(decks_path)
            LOGGER.info("Saved decks DataFrame to assembly cache.")
            LOGGER.debug(f"Saved cached decks DataFrame with {len(self.decks_df)} rows to {decks_path}")

        # Save sealed products DataFrame
        if self.sealed_df is not None and len(self.sealed_df) > 0:
            sealed_path = cache_dir / CACHE_SEALED
            self.sealed_df.write_parquet(sealed_path)
            LOGGER.info("Saved sealed products DataFrame to assembly cache.")
            LOGGER.debug(f"Saved cached sealed products DataFrame with {len(self.sealed_df)} rows to {sealed_path}")

        # Save token products
        if self.token_products:
            token_products_path = cache_dir / CACHE_TOKEN_PRODUCTS
            token_products_path.write_bytes(orjson.dumps(self.token_products))
            LOGGER.info("Saved token products to assembly cache.")
            LOGGER.debug(f"Saved cached token products for {len(self.token_products)} tokens to {token_products_path}")

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

    @cached_property
    def all_identifiers(self) -> AllIdentifiersAssembler:
        """Assembler for AllIdentifiers.json."""
        from .assemble import AllIdentifiersAssembler

        return AllIdentifiersAssembler(self)

    def release_card_data(self) -> None:
        """Free card/token DataFrames and assembler caches.

        Evicts heavy ``@cached_property`` values so the GC can reclaim them.
        Does NOT evict ``normalized_tables`` or ``normalized_boosters`` which
        may still be needed by export format builders.
        """
        import gc

        for attr in (
            "all_cards_df",
            "all_tokens_df",
            "sets_df",
            "sets",
            "atomic_cards",
            "set_list",
            "tcgplayer_skus",
            "all_identifiers",
        ):
            self.__dict__.pop(attr, None)
        gc.collect()
        LOGGER.info("Released card data caches from AssemblyContext")
