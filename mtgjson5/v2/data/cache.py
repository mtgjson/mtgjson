"""
Global cache for MTGJSON provider data and pre-computed aggregations.

This module is our main Data Layer and is responsible for:
- Downloading and caching provider data
- Storing all raw DataFrames/LazyFrames
- Storing all dict lookups

"""

import asyncio
import json
import pathlib
import re
import time
from argparse import Namespace
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from typing import cast, overload

import polars as pl

from mtgjson5 import constants
from mtgjson5.utils import LOGGER
from mtgjson5.v2.providers import CardHoarderPriceProvider as CardHoarderProvider
from mtgjson5.v2.providers import (
    CardMarketProvider,
    CKProvider,
    EdhrecSaltProvider,
    ManapoolPriceProvider,
    ScryfallProvider,
    SealedDataProvider,
    TCGProvider,
)
from mtgjson5.v2.providers.gatherer import GathererProvider
from mtgjson5.v2.providers.mtgwiki import SecretLairProvider
from mtgjson5.v2.providers.scryfall.orientation import OrientationDetector
from mtgjson5.v2.providers.whats_in_standard import WhatsInStandardProvider
from mtgjson5.v2.providers.wizards import WizardsProvider
from mtgjson5.v2.utils import DynamicCategoricals, discover_categoricals


def load_resource_json(filename: str) -> dict | list:
    """Load a JSON resource file and return raw data."""
    file_path = constants.RESOURCE_PATH / filename
    if not file_path.exists():
        LOGGER.warning(f"Resource file not found: {file_path}")
        return {}
    with file_path.open("rb") as f:
        return cast("dict | list", json.loads(f.read()))


def _cache_fresh(path: pathlib.Path, max_age_hours: float = 290.0) -> bool:
    """Check if a cache file exists and is fresh."""
    if not path.exists():
        return False
    age_hours = (time.time() - path.stat().st_mtime) / 3600
    return age_hours < max_age_hours


def _snake_to_camel(name: str) -> str:
    """Convert snake_case to camelCase."""
    if "_" not in name:
        return name
    parts = name.split("_")
    return parts[0] + "".join(p.capitalize() for p in parts[1:])


@overload
def _normalize_columns(df: None) -> None: ...


@overload
def _normalize_columns(df: pl.DataFrame) -> pl.DataFrame: ...


@overload
def _normalize_columns(df: pl.LazyFrame) -> pl.LazyFrame: ...


def _normalize_columns(
    df: pl.DataFrame | pl.LazyFrame | None,
) -> pl.DataFrame | pl.LazyFrame | None:
    """Normalize all snake_case columns to camelCase."""
    if df is None:
        return None
    cols = df.collect_schema().names() if isinstance(df, pl.LazyFrame) else df.columns
    renames = {c: _snake_to_camel(c) for c in cols if "_" in c}
    return df.rename(renames) if renames else df


class GlobalCache:
    """
    Global shared access singleton for source data.

    All DataFrame attributes use the `_lf` suffix as they will be LazyFrames
    by time they are accessed.
    """

    _instance: "GlobalCache | None" = None

    def __new__(cls, args: Namespace | None = None) -> "GlobalCache":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, _args: Namespace | None = None) -> None:
        if getattr(self, "_initialized", False):
            return

        constants.CACHE_PATH.mkdir(parents=True, exist_ok=True)
        self.cache_path = constants.CACHE_PATH

        # Core Bulk Data LFs
        self.cards_lf: pl.LazyFrame | None = None
        self.rulings_lf: pl.LazyFrame | None = None
        self.sets_lf: pl.LazyFrame | None = None

        # Pre-computed Aggregation LFs
        self.uuid_cache_lf: pl.LazyFrame | None = None

        # Provider Data lFs
        self.card_kingdom_lf: pl.LazyFrame | None = None
        self.card_kingdom_raw_lf: pl.LazyFrame | None = None
        self.mcm_lookup_lf: pl.LazyFrame | None = None
        self.salt_lf: pl.LazyFrame | None = None
        self.spellbook_lf: pl.LazyFrame | None = None
        self.sld_subsets_lf: pl.LazyFrame | None = None
        self.orientation_lf: pl.LazyFrame | None = None
        self.gatherer_lf: pl.LazyFrame | None = None
        self.multiverse_bridge_lf: pl.LazyFrame | None = None

        # GitHub/Sealed LFs
        self.sealed_cards_lf: pl.LazyFrame | None = None
        self.sealed_products_lf: pl.LazyFrame | None = None
        self.sealed_contents_lf: pl.LazyFrame | None = None
        self.decks_lf: pl.LazyFrame | None = None
        self.boosters_lf: pl.LazyFrame | None = None
        self.token_products_lf: pl.LazyFrame | None = None

        # Marketplace Data LFs
        self.tcg_skus_lf: pl.LazyFrame | None = None
        self.tcg_sku_map_lf: pl.LazyFrame | None = None
        self.tcg_to_uuid_lf: pl.LazyFrame | None = None
        self.tcg_etched_to_uuid_lf: pl.LazyFrame | None = None
        self.mtgo_to_uuid_lf: pl.LazyFrame | None = None
        self.scryfall_to_uuid_lf: pl.LazyFrame | None = None
        self.cardmarket_to_uuid_lf: pl.LazyFrame | None = None

        self.languages_lf: pl.LazyFrame | None = None

        # Final Output
        self.final_cards_lf: pl.LazyFrame | None = None

        # Raw Resource Dicts
        self.duel_deck_sides: dict = {}
        self.meld_data: dict | list = {}
        self.meld_triplets: dict[str, list[str]] = {}
        self.meld_overrides: dict = {}
        self.world_championship_signatures: dict = {}
        self.manual_overrides: dict = {}
        self.foreigndata_exceptions: dict = {}
        self.gatherer_map: dict = {}
        self.set_code_watermarks: dict = {}
        self.standard_legal_sets: set[str] = set()
        self.unlimited_cards: set[str] = set()
        self.set_translations: dict[str, dict[str, str | None]] = {}
        self.tcgplayer_set_id_overrides: dict[str, int] = {}
        self.keyrune_code_overrides: dict[str, str] = {}
        self.base_set_sizes: dict[str, int] = {}
        self.card_enrichment: dict[str, dict[str, dict]] = {}

        # Scryfall Catalog Data (for Keywords.json and CardTypes.json)
        self.keyword_abilities: list[str] = []
        self.keyword_actions: list[str] = []
        self.ability_words: list[str] = []
        self.card_type_subtypes: dict[str, list[str]] = {}
        self.super_types: list[str] = []
        self.planar_types: list[str] = []

        # Categoricals - discovered from scryfall data inspection
        self._categoricals: DynamicCategoricals | None = None

        # Provider Instances
        self._scryfall: ScryfallProvider | None = None
        self._cardkingdom: CKProvider | None = None
        self._cardmarket: CardMarketProvider | None = None
        self._gatherer: GathererProvider | None = None
        self._github: SealedDataProvider | None = None
        self._edhrec: EdhrecSaltProvider | None = None
        self._standard: WhatsInStandardProvider | None = None
        self._manapool: ManapoolPriceProvider | None = None
        self._cardhoarder: CardHoarderProvider | None = None
        self._tcgplayer: TCGProvider | None = None
        self._secretlair: SecretLairProvider | None = None
        self._orientations: OrientationDetector | None = None
        self._wizards: WizardsProvider | None = None

        # State
        self._initialized = True
        self._loaded = False
        self._set_filter: list[str] | None = None
        self._scryfall_id_filter: set[str] | None = None
        self._output_types: set[str] = set()
        self._export_formats: set[str] | None = None
        self._tcg_skus_future: Future[pl.LazyFrame] | None = None

    def release(self, *attrs: str) -> None:
        """Release specific cached data to free memory.

        Args:
            *attrs: Attribute names to clear (e.g., 'cards_lf', 'rulings_lf')
        """
        for attr in attrs:
            if hasattr(self, attr):
                setattr(self, attr, None)

    def clear(self) -> None:
        """Clear all cached data to free memory."""
        self.release(
            "cards_lf",
            "rulings_lf",
            "sets_lf",
            "uuid_cache_lf",
            "card_kingdom_lf",
            "card_kingdom_raw_lf",
            "mcm_lookup_lf",
            "salt_lf",
            "spellbook_lf",
            "sld_subsets_lf",
            "orientation_lf",
            "gatherer_lf",
            "multiverse_bridge_lf",
            "sealed_cards_lf",
            "sealed_products_lf",
            "sealed_contents_lf",
            "decks_lf",
            "boosters_lf",
            "token_products_lf",
            "tcg_skus_lf",
            "tcg_sku_map_lf",
            "tcg_to_uuid_lf",
            "tcg_etched_to_uuid_lf",
            "mtgo_to_uuid_lf",
            "scryfall_to_uuid_lf",
            "cardmarket_to_uuid_lf",
            "languages_lf",
            "final_cards_lf",
        )
        self._loaded = False

    def load_all(
        self,
        set_codes: list[str] | None = None,
        output_types: set[str] | None = None,
        export_formats: set[str] | None = None,
        skip_mcm: bool = False,
    ) -> "GlobalCache":
        """
        Load all data sources and pre-compute aggregations.

        Args:
            set_codes: Optional list of set codes to filter
            output_types: Optional set of output types (e.g., {"decks"}).
                When "decks" is the only output, computes _scryfall_id_filter
                to limit card processing to only deck cards.
            export_formats: Optional set of export formats (e.g., {"parquet", "csv"}).
                When specified, can be used to skip loading data not needed
                for the requested formats.
            skip_mcm: Skip CardMarket data fetching (speeds up builds).
        """
        self._output_types = output_types or set()
        self._export_formats = export_formats
        self._set_filter = [s.upper() for s in set_codes] if set_codes else None

        self._download_bulk_data()
        self._load_bulk_data()
        self._load_resources()
        self._load_sets_metadata()
        self._load_missing_set_cards()

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(self._load_orientations): "orientations",
                executor.submit(self._load_card_kingdom): "card_kingdom",
                executor.submit(self._load_edhrec_salt): "edhrec",
                executor.submit(self._load_spellbook): "spellbook",
                executor.submit(self._load_gatherer): "gatherer",
                executor.submit(self._load_whats_in_standard): "standard",
                executor.submit(self._load_github_data): "github",
                executor.submit(self._load_secretlair_subsets): "secretlair",
                executor.submit(self._load_scryfall_catalogs): "scryfall_catalogs",
            }
            if not skip_mcm:
                futures[executor.submit(self._load_mcm_lookup)] = "mcm"
            else:
                LOGGER.info("Skipping MCM data (--skip-mcm flag)")

            self._start_tcg_skus_fetch(executor)

            for future in as_completed(futures):
                name = futures[future]
                try:
                    future.result()
                    LOGGER.info(f"Loaded {name}")
                except Exception as e:
                    LOGGER.error(f"Failed to load {name}: {e}")

            if self.cards_lf is None:
                raise RuntimeError("Bulk data not loaded")

            self.unlimited_cards = self.scryfall.cards_without_limits

            # Build scryfall_id filter for deck-only builds
            if self._output_types == {"decks"} and self.decks_lf is not None:
                self._build_deck_scryfall_filter()

            self._normalize_all_columns()
            self._apply_categoricals()
            self._dump_and_reload_as_lazy()

            self._loaded = True
            return self

    def _start_tcg_skus_fetch(self, executor: ThreadPoolExecutor) -> None:
        """Start TCGPlayer SKU fetch in background.

        The fetch runs in a thread pool executor. The result can be awaited
        later using _await_tcg_skus() when TcgplayerSkus.json is needed.
        """
        cache_path = self.cache_path / "tcg_skus.parquet"

        if _cache_fresh(cache_path):
            self.tcg_skus_lf = pl.read_parquet(cache_path).lazy()
            LOGGER.info("Using cached TCG SKUs data")
            return

        LOGGER.info("Starting TCGPlayer SKU fetch in background...")
        self._tcg_skus_future = executor.submit(self.tcgplayer.fetch_all_products_sync)

    def _await_tcg_skus(self) -> None:
        """Block until TCG SKUs are ready (called when actually needed).

        This method is called by TcgplayerSkusAssembler when it needs the data.
        If the data was cached, this returns immediately. If the background
        fetch is still running, this blocks until completion.
        """
        if self.tcg_skus_lf is not None:
            return  # Already loaded from cache

        if self._tcg_skus_future is not None:
            try:
                LOGGER.info("Waiting for TCGPlayer SKU fetch to complete...")
                self.tcg_skus_lf = self._tcg_skus_future.result()
                LOGGER.info("TCGPlayer SKU fetch complete")
            except Exception as e:
                LOGGER.warning(f"Failed to fetch TCGPlayer SKUs: {e}")
                from mtgjson5.v2.providers.tcgplayer.models import PRODUCT_SCHEMA

                self.tcg_skus_lf = pl.DataFrame(schema=cast("dict", PRODUCT_SCHEMA)).lazy()
            finally:
                self._tcg_skus_future = None

    def _dump_and_reload_as_lazy(self) -> None:
        """
        Dump all DataFrames to parquet and reload as LazyFrames to free memory
        and optimize query planning.
        """
        import gc

        lazy_cache_path = self.cache_path / "lazy"
        lazy_cache_path.mkdir(parents=True, exist_ok=True)

        dataframes_to_dump = {
            "cards_lf": "cards.parquet",
            "rulings_lf": "rulings.parquet",
            "sets_lf": "sets.parquet",
            "card_kingdom_lf": "card_kingdom.parquet",
            "card_kingdom_raw_lf": "card_kingdom_raw.parquet",
            "mcm_lookup_lf": "mcm_lookup.parquet",
            "salt_lf": "salt.parquet",
            "spellbook_lf": "spellbook.parquet",
            "sld_subsets_lf": "sld_subsets.parquet",
            "orientation_lf": "orientations.parquet",
            "gatherer_lf": "gatherer.parquet",
            "multiverse_bridge_lf": "multiverse_bridge.parquet",
            "uuid_cache_lf": "uuid_cache.parquet",
            "sealed_cards_lf": "sealed_cards.parquet",
            "sealed_products_lf": "sealed_products.parquet",
            "sealed_contents_lf": "sealed_contents.parquet",
            "decks_lf": "decks.parquet",
            "boosters_lf": "boosters.parquet",
            "token_products_lf": "token_products.parquet",
            "tcg_skus_lf": "tcg_skus.parquet",
            "tcg_sku_map_lf": "tcg_sku_map.parquet",
            "tcg_to_uuid_lf": "tcg_to_uuid.parquet",
            "tcg_etched_to_uuid_lf": "tcg_etched_to_uuid.parquet",
            "mtgo_to_uuid_lf": "mtgo_to_uuid.parquet",
            "scryfall_to_uuid_lf": "scryfall_to_uuid.parquet",
            "cardmarket_to_uuid_lf": "cardmarket_to_uuid.parquet",
            "languages_lf": "languages.parquet",
        }

        for attr, filename in dataframes_to_dump.items():
            df_or_lf = getattr(self, attr, None)
            if df_or_lf is None:
                continue

            parquet_path = lazy_cache_path / filename

            try:
                if isinstance(df_or_lf, pl.LazyFrame):
                    df_or_lf.sink_parquet(parquet_path)
                    del df_or_lf
                elif isinstance(df_or_lf, pl.DataFrame):
                    if len(df_or_lf) == 0:
                        continue
                    df_or_lf.write_parquet(parquet_path)
                else:
                    continue

                setattr(self, attr, None)
                new_lf = pl.scan_parquet(parquet_path)
                setattr(self, attr, new_lf)
            except Exception as e:
                LOGGER.error(f"Failed to dump/reload {attr}: {e}")

            gc.collect()

    def _apply_categoricals(self) -> None:
        """Apply dynamic categoricals to relevant DataFrames."""
        if self._categoricals is None and self.cards_lf is not None:
            self._categoricals = discover_categoricals(self.cards_lf, self.sets_lf)

    def _build_deck_scryfall_filter(self) -> None:
        """
        Build scryfall_id filter from deck UUIDs for deck-only builds.

        Extracts all UUIDs from deck card lists (mainBoard, sideBoard, commander, tokens),
        then uses uuid_cache to reverse-lookup the corresponding scryfall_ids.
        """
        if self.decks_lf is None:
            return

        # Collect deck metadata
        decks = self.decks_lf.collect() if isinstance(self.decks_lf, pl.LazyFrame) else self.decks_lf

        # Filter by set codes if specified
        if self._set_filter:
            decks = decks.filter(pl.col("setCode").is_in(self._set_filter))

        if len(decks) == 0:
            return

        # Extract all UUIDs from card list columns
        deck_uuids: set[str] = set()
        for col in ["mainBoard", "sideBoard", "commander", "tokens"]:
            if col not in decks.columns:
                continue
            for card_list in decks[col].to_list():
                if card_list:
                    for card in card_list:
                        if card and card.get("uuid"):
                            deck_uuids.add(card["uuid"])

        if not deck_uuids:
            LOGGER.warning("No UUIDs found in deck data")
            return

        LOGGER.info(f"Found {len(deck_uuids):,} unique UUIDs in deck data")

        # Use uuid_cache to get scryfall_ids for these UUIDs
        uuid_cache = self.uuid_cache_lf
        if uuid_cache is not None:
            if isinstance(uuid_cache, pl.LazyFrame):
                uuid_cache = uuid_cache.collect()  # type: ignore[assignment]
            if len(uuid_cache) > 0:  # type: ignore[arg-type]
                scryfall_ids = (
                    uuid_cache.filter(pl.col("cachedUuid").is_in(deck_uuids))  # type: ignore[attr-defined]
                    .select("scryfallId")
                    .unique()
                    .to_series()
                    .to_list()
                )
                self._scryfall_id_filter = set(scryfall_ids)
                LOGGER.info(
                    f"Built scryfall_id filter: {len(self._scryfall_id_filter):,} IDs "
                    f"for {len(deck_uuids):,} deck UUIDs"
                )
                return
        LOGGER.warning("uuid_cache not available, cannot build scryfall_id filter")

    def _download_bulk_data(self, force_refresh: bool = False) -> None:
        """Download Scryfall bulk data if missing or stale."""
        cards_path = self.cache_path / "all_cards.ndjson"
        default_cards_path = self.cache_path / "default_cards.ndjson"
        rulings_path = self.cache_path / "rulings.ndjson"

        needs_download = force_refresh
        if not cards_path.exists() or cards_path.stat().st_size == 0:
            needs_download = True
        if not default_cards_path.exists() or default_cards_path.stat().st_size == 0:
            needs_download = True
        if not rulings_path.exists() or rulings_path.stat().st_size == 0:
            needs_download = True

        # Check age (72h max)
        if not needs_download and cards_path.exists():
            age_hours = (time.time() - cards_path.stat().st_mtime) / 3600
            if age_hours > 72:
                LOGGER.info(f"Bulk data is {age_hours:.1f}h old, refreshing...")
                needs_download = True

        if needs_download:
            LOGGER.info("Downloading bulk scryfall data...")
            self.bulkdata.download_bulk_files_sync(
                self.cache_path,
                ["all_cards", "default_cards", "rulings"],
                force_refresh,
            )
        else:
            LOGGER.info("Using cached bulk data")

    def _load_bulk_data(self) -> None:
        """Load bulk NDJSON files into LazyFrames."""
        LOGGER.info("Loading LazyFrames...")

        cards_path = self.cache_path / "all_cards.ndjson"
        rulings_path = self.cache_path / "rulings.ndjson"

        # Columns that may be numeric in some records but should be strings
        string_cast_columns = [
            "power",
            "toughness",
            "loyalty",
            "defense",
            "hand_modifier",
            "life_modifier",
        ]

        # Scan without schema overrides first
        self.cards_lf = pl.scan_ndjson(
            cards_path,
            infer_schema_length=100000,
        )

        schema = self.cards_lf.collect_schema()

        # Build cast expressions
        cast_exprs = []
        for col_name in string_cast_columns:
            if col_name in schema:
                cast_exprs.append(pl.col(col_name).cast(pl.Utf8))

        if cast_exprs:
            self.cards_lf = self.cards_lf.with_columns(cast_exprs)

        # Ensure optional columns exist
        optional_columns = {
            "defense": pl.Utf8,
            "flavor_name": pl.Utf8,
        }
        missing_cols = []
        for col_name, dtype in optional_columns.items():
            if col_name not in schema:
                missing_cols.append(pl.lit(None).cast(dtype).alias(col_name))

        if missing_cols:
            self.cards_lf = self.cards_lf.with_columns(missing_cols)

        self.rulings_lf = pl.scan_ndjson(rulings_path, infer_schema_length=1000)

        # Build languages from default_cards
        self._load_languages()

    def _load_languages(self) -> None:
        """
        Build default card languages mapping from default_cards bulk file.
        Sets up the base for context.py to build full foreign data.
        """
        default_cards_path = self.cache_path / "default_cards.ndjson"
        if not default_cards_path.exists():
            LOGGER.warning("default_cards.ndjson not found, skipping language mapping")
            return

        LOGGER.info("Building languages mapping...")

        default_lf = pl.scan_ndjson(default_cards_path, infer_schema_length=1000)

        from mtgjson5.v2.consts import LANGUAGE_MAP

        self.languages_lf = default_lf.select(
            [
                pl.col("id").alias("scryfallId"),
                pl.col("lang").replace_strict(LANGUAGE_MAP, default=pl.col("lang")).alias("language"),
            ]
        ).unique(["scryfallId", "language"])

        LOGGER.info("Built languages mapping")

    def load_id_mappings(self) -> None:
        """
        Load ID -> UUID mappings from cache if they exist.

        These mappings are created by the card pipeline in sink_cards() and are
        used by PriceBuilderContext for price builds that run separately from
        a full card build.
        """
        tcg_path = self.cache_path / "tcg_to_uuid.parquet"
        if tcg_path.exists():
            self.tcg_to_uuid_lf = pl.scan_parquet(tcg_path)
            LOGGER.info("Loaded tcg_to_uuid mapping from cache")

        tcg_etched_path = self.cache_path / "tcg_etched_to_uuid.parquet"
        if tcg_etched_path.exists():
            self.tcg_etched_to_uuid_lf = pl.scan_parquet(tcg_etched_path)
            LOGGER.info("Loaded tcg_etched_to_uuid mapping from cache")

        mtgo_path = self.cache_path / "mtgo_to_uuid.parquet"
        if mtgo_path.exists():
            self.mtgo_to_uuid_lf = pl.scan_parquet(mtgo_path)
            LOGGER.info("Loaded mtgo_to_uuid mapping from cache")

        scryfall_path = self.cache_path / "scryfall_to_uuid.parquet"
        if scryfall_path.exists():
            self.scryfall_to_uuid_lf = pl.scan_parquet(scryfall_path)
            LOGGER.info("Loaded scryfall_to_uuid mapping from cache")

    def _load_resources(self) -> None:
        """Load local JSON resource files."""
        LOGGER.info("Loading resource files...")

        self.duel_deck_sides = cast("dict", load_resource_json("duel_deck_sides.json"))
        self.meld_data = load_resource_json("meld_triplets.json")
        self.meld_overrides = cast("dict", load_resource_json("meld_overrides.json"))
        self.world_championship_signatures = cast("dict", load_resource_json("world_championship_signatures.json"))
        self.manual_overrides = cast("dict", load_resource_json("manual_overrides.json"))
        self.foreigndata_exceptions = cast("dict", load_resource_json("foreigndata_exceptions.json"))
        self.set_code_watermarks = cast("dict", load_resource_json("set_code_watermarks.json"))
        uuid_raw = cast("dict", load_resource_json("legacy_mtgjson_v5_uuid_mapping.json"))
        if uuid_raw:
            rows = [
                {"scryfallId": sid, "side": side, "cachedUuid": uuid}
                for sid, sides in uuid_raw.items()
                for side, uuid in sides.items()
            ]
            self.uuid_cache_lf = pl.DataFrame(rows).lazy()

        meld_triplets_expanded: dict[str, list[str]] = {}
        if isinstance(self.meld_data, list):
            for triplet in self.meld_data:
                if len(triplet) == 3:
                    for name in triplet:
                        meld_triplets_expanded[name] = triplet

        self.meld_triplets = meld_triplets_expanded

        raw_translations = cast("dict", load_resource_json("mkm_set_name_translations.json"))
        for set_name, langs in raw_translations.items():
            self.set_translations[set_name] = {
                "Chinese Simplified": langs.get("zhs"),
                "Chinese Traditional": langs.get("zht"),
                "French": langs.get("fr"),
                "German": langs.get("de"),
                "Italian": langs.get("it"),
                "Japanese": langs.get("ja"),
                "Korean": langs.get("ko"),
                "Portuguese (Brazil)": None,
                "Russian": langs.get("ru"),
                "Spanish": langs.get("es"),
            }

        self.tcgplayer_set_id_overrides = cast("dict", load_resource_json("tcgplayer_set_id_overrides.json"))

        self.keyrune_code_overrides = cast("dict", load_resource_json("keyrune_code_overrides.json"))

        self.base_set_sizes = cast("dict", load_resource_json("base_set_sizes.json"))
        self.card_enrichment = cast("dict", load_resource_json("card_enrichment.json"))

        multiverse_bridge_raw = cast("dict", load_resource_json("multiverse_bridge_backup.json"))
        if multiverse_bridge_raw and "cards" in multiverse_bridge_raw:
            cards_data = multiverse_bridge_raw["cards"]
            rows = [
                {
                    "cachedUuid": uuid,
                    "cardsphereId": data.get("cardsphereId"),
                    "cardsphereFoilId": data.get("cardsphereFoilId"),
                    "deckboxId": data.get("deckboxId"),
                }
                for uuid, data in cards_data.items()
            ]
            if rows:
                self.multiverse_bridge_lf = pl.DataFrame(rows).lazy()
                LOGGER.info(f"Loaded multiverse bridge: {len(rows):,} cards")

        LOGGER.info("Loaded resource files")

    def _load_sets_metadata(self) -> None:
        """Load set metadata from Scryfall."""
        cache_path = self.cache_path / "sets.parquet"

        if _cache_fresh(cache_path):
            self.sets_lf = pl.read_parquet(cache_path).lazy()
            return

        sets_response = self.scryfall.download(self.scryfall.ALL_SETS_URL)
        if sets_response.get("object") == "error":
            return

        sets_data = sets_response.get("data", [])
        for s in sets_data:
            s["code"] = s["code"].upper()

        sets_df = pl.DataFrame(sets_data)

        # Link parent sets to their token children
        token_sets = (
            sets_df.filter(pl.col("set_type") == "token")
            .filter(pl.col("parent_set_code").is_not_null())
            .select(
                [
                    pl.col("parent_set_code"),
                    pl.col("code").alias("tokenSetCode"),
                ]
            )
        )

        sets_df = sets_df.join(
            token_sets,
            left_on="code",
            right_on="parent_set_code",
            how="left",
        )

        sets_df.write_parquet(cache_path)
        self.sets_lf = sets_df.lazy()

    def _load_missing_set_cards(self) -> None:
        """Fetch cards for sets in API but not in bulk data."""
        if self.sets_lf is None or self.cards_lf is None:
            return

        sets_collected = self.sets_lf.collect() if isinstance(self.sets_lf, pl.LazyFrame) else self.sets_lf

        cards_collected = self.cards_lf.collect() if isinstance(self.cards_lf, pl.LazyFrame) else self.cards_lf
        bulk_sets = set(cards_collected.select("set").unique()["set"].str.to_uppercase().to_list())

        missing_sets = sets_collected.filter((pl.col("card_count") > 0) & (~pl.col("code").is_in(bulk_sets)))

        if missing_sets.height == 0:
            LOGGER.info("No missing set cards to fetch")
            return

        LOGGER.info(f"Fetching cards for {missing_sets.height} sets not in bulk data...")

        all_new_cards = []
        for row in missing_sets.iter_rows(named=True):
            set_code = row["code"].lower()
            card_count = row.get("card_count", 0)
            set_name = row.get("name", set_code)

            LOGGER.info(f"Fetching {set_code} ({set_name}): {card_count} cards...")

            try:
                search_url = f"https://api.scryfall.com/cards/search?q=set:{set_code}&unique=prints"
                cards = self.scryfall.download_all_pages_api(search_url)

                if cards:
                    all_new_cards.extend(cards)
                    LOGGER.info(f"Fetched {len(cards)} cards for {set_code}")

            except Exception as e:
                LOGGER.warning(f"Failed to fetch {set_code}: {e}")

        if all_new_cards:
            new_cards_df = pl.DataFrame(all_new_cards)
            LOGGER.info(f"Adding {len(all_new_cards)} cards from {missing_sets.height} preview sets")

            if isinstance(self.cards_lf, pl.LazyFrame):
                cards_df = self.cards_lf.collect()
            else:
                cards_df = self.cards_lf

            existing_cols = set(cards_df.columns)
            new_cols = set(new_cards_df.columns)

            for col in existing_cols - new_cols:
                new_cards_df = new_cards_df.with_columns(pl.lit(None).alias(col))

            new_cards_df = new_cards_df.select([c for c in cards_df.columns if c in new_cards_df.columns])

            # Cast new_cards_df columns to match cards_df schema to avoid type errors
            cards_schema = cards_df.schema
            cast_exprs = []
            for col in new_cards_df.columns:
                if col in cards_schema:
                    target_dtype = cards_schema[col]
                    if new_cards_df.schema[col] != target_dtype:
                        cast_exprs.append(pl.col(col).cast(target_dtype))
                    else:
                        cast_exprs.append(pl.col(col))
                else:
                    cast_exprs.append(pl.col(col))
            if cast_exprs:
                new_cards_df = new_cards_df.select(cast_exprs)

            self.cards_lf = pl.concat([cards_df, new_cards_df], how="diagonal").lazy()

    def _load_card_kingdom(self) -> None:
        """Load Card Kingdom data with caching."""
        pivoted_cache = self.cache_path / "ck_pivoted.parquet"
        raw_cache = self.cache_path / "ck_raw.parquet"

        if _cache_fresh(pivoted_cache) and _cache_fresh(raw_cache):
            self.card_kingdom_lf = pl.read_parquet(pivoted_cache).lazy()
            self.card_kingdom_raw_lf = pl.read_parquet(raw_cache).lazy()
            return

        # Initialize empty DataFrames
        self.card_kingdom_lf = (
            pl.DataFrame(
                {
                    "id": [],
                    "cardKingdomId": [],
                    "cardKingdomFoilId": [],
                    "cardKingdomUrl": [],
                    "cardKingdomFoilUrl": [],
                }
            )
            .cast(
                {
                    "id": pl.String,
                    "cardKingdomId": pl.String,
                    "cardKingdomFoilId": pl.String,
                    "cardKingdomUrl": pl.String,
                    "cardKingdomFoilUrl": pl.String,
                }
            )
            .lazy()
        )
        self.card_kingdom_raw_lf = pl.DataFrame().lazy()

        try:
            self.card_kingdom.fetch_sync()

            ck_data = self.card_kingdom.get_join_data()
            if ck_data is not None and len(ck_data) > 0:
                ck_data.write_parquet(pivoted_cache)
                self.card_kingdom_lf = ck_data.lazy()

            # pylint: disable=protected-access
            if self.card_kingdom._raw_df is not None and len(self.card_kingdom._raw_df) > 0:
                self.card_kingdom._raw_df.write_parquet(raw_cache)
                self.card_kingdom_raw_lf = self.card_kingdom._raw_df.lazy()

        except Exception as e:
            LOGGER.warning(f"Failed to fetch Card Kingdom data: {e}")

    def _load_edhrec_salt(self) -> None:
        """Load EDHREC saltiness data."""
        cache_path = self.cache_path / "edhrec_salt.parquet"

        if _cache_fresh(cache_path):
            self.salt_lf = pl.read_parquet(cache_path).lazy()
            return

        salt_df = self.edhrec.get_data_frame()
        if salt_df is not None and len(salt_df) > 0:
            salt_df.write_parquet(cache_path)
            self.salt_lf = salt_df.lazy()

    def _load_spellbook(self) -> None:
        """Load alchemy spellbook data from Scryfall."""
        cache_path = self.cache_path / "spellbook.parquet"

        if _cache_fresh(cache_path):
            self.spellbook_lf = pl.read_parquet(cache_path).lazy()
            return

        LOGGER.info("Fetching spellbook data from Scryfall...")
        spellbook_data = asyncio.run(ScryfallProvider().fetch_all_spellbooks())
        if spellbook_data:
            records = [{"name": parent, "spellbook": cards} for parent, cards in spellbook_data.items()]
            spellbook_df = pl.DataFrame(records)
            spellbook_df.write_parquet(cache_path)
            self.spellbook_lf = spellbook_df.lazy()
            LOGGER.info(f"Loaded {len(records)} spellbook entries")

    def _load_gatherer(self) -> None:
        """Load Gatherer original text data."""
        cache_path = self.cache_path / "gatherer_map.json"

        if _cache_fresh(cache_path):
            with cache_path.open("rb") as f:
                self.gatherer_map = json.loads(f.read())
        else:
            self.gatherer_map = getattr(self.gatherer, "_multiverse_id_to_data", {})
            with cache_path.open("w", encoding="utf-8") as f:
                json.dump(self.gatherer_map, f)

        if self.gatherer_map:
            rows = []
            for mv_id, entries in self.gatherer_map.items():
                if entries:
                    entry = entries[0]
                    rows.append(
                        {
                            "multiverse_id": str(mv_id),
                            "originalText": entry.get("original_text"),
                            "originalType": entry.get("original_types"),
                        }
                    )
            if rows:
                self.gatherer_lf = pl.DataFrame(rows).lazy()

    def _load_whats_in_standard(self) -> None:
        """Load current Standard-legal sets."""
        cache_path = self.cache_path / "standard_sets.json"

        if _cache_fresh(cache_path):
            with cache_path.open("rb") as f:
                self.standard_legal_sets = set(json.loads(f.read()))
            return

        self.standard_legal_sets = set(self.whats_in_standard.set_codes or [])
        with cache_path.open("w", encoding="utf-8") as f:
            json.dump(list(self.standard_legal_sets), f)

    def _load_github_data(self) -> None:
        """Load GitHub sealed/deck/booster/token-products data."""
        card_to_products_cache = self.cache_path / "github_card_to_products.parquet"
        sealed_products_cache = self.cache_path / "github_sealed_products.parquet"
        sealed_contents_cache = self.cache_path / "github_sealed_contents.parquet"
        decks_cache = self.cache_path / "github_decks.parquet"
        booster_cache = self.cache_path / "github_booster.parquet"
        token_products_cache = self.cache_path / "github_token_products.parquet"

        all_cached = all(
            _cache_fresh(p)
            for p in [
                card_to_products_cache,
                sealed_products_cache,
                sealed_contents_cache,
                decks_cache,
                booster_cache,
                token_products_cache,
            ]
        )

        if all_cached:
            self.sealed_cards_lf = pl.read_parquet(card_to_products_cache).lazy()
            self.sealed_products_lf = pl.read_parquet(sealed_products_cache).lazy()
            self.sealed_contents_lf = pl.read_parquet(sealed_contents_cache).lazy()
            self.decks_lf = pl.read_parquet(decks_cache).lazy()
            self.boosters_lf = pl.read_parquet(booster_cache).lazy()
            self.token_products_lf = pl.read_parquet(token_products_cache).lazy()
            return

        def on_github_complete(provider: SealedDataProvider) -> None:
            """Called when GitHub data finishes loading."""
            LOGGER.info("Sealed loaded - transferring to GlobalCache...")

            if provider.card_to_products_df is not None:
                provider.card_to_products_df.collect().write_parquet(card_to_products_cache)
                self.sealed_cards_lf = provider.card_to_products_df

            if provider.sealed_products_df is not None:
                provider.sealed_products_df.collect().write_parquet(sealed_products_cache)
                self.sealed_products_lf = provider.sealed_products_df

            if provider.sealed_contents_df is not None:
                provider.sealed_contents_df.collect().write_parquet(sealed_contents_cache)
                self.sealed_contents_lf = provider.sealed_contents_df

            if provider.decks_df is not None:
                provider.decks_df.collect().write_parquet(decks_cache)
                self.decks_lf = provider.decks_df

            if provider.boosters_df is not None:
                provider.boosters_df.collect().write_parquet(booster_cache)
                self.boosters_lf = provider.boosters_df

            if provider.token_products_df is not None:
                provider.token_products_df.collect().write_parquet(token_products_cache)
                self.token_products_lf = provider.token_products_df

        self.github.load_async_background(on_complete=on_github_complete)

    def _load_orientations(self) -> None:
        """Load orientation data for Art Series cards from Scryfall."""
        cache_path = self.cache_path / "orientations.parquet"
        if _cache_fresh(cache_path):
            self.orientation_lf = pl.read_parquet(cache_path).lazy()
            return

        detector = OrientationDetector()
        sets_lf_raw = self.sets_lf
        if sets_lf_raw is None:
            LOGGER.warning("Sets not loaded, skipping orientations")
            return
        sets_df: pl.DataFrame = sets_lf_raw.collect() if isinstance(sets_lf_raw, pl.LazyFrame) else sets_lf_raw
        art_series_sets = sets_df.filter(pl.col("name").str.contains("Art Series"))["code"].to_list()

        rows = []
        for set_code in art_series_sets:
            orientation_map = detector.get_uuid_to_orientation_map(set_code)
            for scryfall_id, orientation in (orientation_map or {}).items():
                rows.append({"scryfallId": scryfall_id, "orientation": orientation})

        orientation_df = pl.DataFrame(rows) if rows else pl.DataFrame()
        if len(orientation_df) > 0:
            orientation_df.write_parquet(cache_path)
        self.orientation_lf = orientation_df.lazy()

    def _load_secretlair_subsets(self) -> None:
        """Load Secret Lair subset mappings."""
        cache_path = self.cache_path / "sld_subsets.parquet"

        if _cache_fresh(cache_path):
            self.sld_subsets_lf = pl.read_parquet(cache_path).lazy()
            return

        relation_map = self.secretlair.download()
        if relation_map:
            rows = [{"number": num, "subsets": [name]} for num, name in relation_map.items()]
            sld_df = pl.DataFrame(rows)
            sld_df.write_parquet(cache_path)
            self.sld_subsets_lf = sld_df.lazy()

    def _load_scryfall_catalogs(self) -> None:
        """Load keyword and card type catalogs from Scryfall API and Magic rules.

        These are used by Keywords.json and CardTypes.json assemblers.
        Cached to JSON files to avoid repeated API calls.
        """
        from mtgjson5.utils import parse_magic_rules_subset

        keywords_cache = self.cache_path / "scryfall_keywords.json"
        types_cache = self.cache_path / "scryfall_card_types.json"

        if _cache_fresh(keywords_cache):
            with keywords_cache.open("rb") as f:
                data = json.loads(f.read())
                self.ability_words = data.get("ability_words", [])
                self.keyword_abilities = data.get("keyword_abilities", [])
                self.keyword_actions = data.get("keyword_actions", [])
        else:
            provider = self.scryfall
            self.ability_words = provider.get_catalog_entry("ability-words")
            self.keyword_abilities = provider.get_catalog_entry("keyword-abilities")
            self.keyword_actions = provider.get_catalog_entry("keyword-actions")
            with keywords_cache.open("w", encoding="utf-8") as f:
                json.dump(
                    {
                        "ability_words": self.ability_words,
                        "keyword_abilities": self.keyword_abilities,
                        "keyword_actions": self.keyword_actions,
                    },
                    f,
                )

        if _cache_fresh(types_cache):
            with types_cache.open("rb") as f:
                data = json.loads(f.read())
                self.card_type_subtypes = data.get("subtypes", {})
                self.super_types = data.get("super_types", [])
                self.planar_types = data.get("planar_types", [])
        else:
            provider = self.scryfall
            self.card_type_subtypes = {
                "artifact": provider.get_catalog_entry("artifact-types"),
                "battle": provider.get_catalog_entry("battle-types"),
                "creature": provider.get_catalog_entry("creature-types"),
                "enchantment": provider.get_catalog_entry("enchantment-types"),
                "land": provider.get_catalog_entry("land-types"),
                "planeswalker": provider.get_catalog_entry("planeswalker-types"),
                "spell": provider.get_catalog_entry("spell-types"),
            }

            magic_rules = parse_magic_rules_subset(self.wizards.get_magic_rules())
            super_regex = re.compile(r".*The supertypes are (.*)\.")
            planar_regex = re.compile(r".*The planar types are (.*)\.")
            self.super_types = self._regex_str_to_list(super_regex.search(magic_rules))
            self.planar_types = self._regex_str_to_list(planar_regex.search(magic_rules))

            with types_cache.open("w", encoding="utf-8") as f:
                json.dump(
                    {
                        "subtypes": self.card_type_subtypes,
                        "super_types": self.super_types,
                        "planar_types": self.planar_types,
                    },
                    f,
                )

        LOGGER.info(
            f"Loaded Scryfall catalogs: {len(self.keyword_abilities)} keyword abilities, "
            f"{len(self.card_type_subtypes)} card types"
        )

    @staticmethod
    def _regex_str_to_list(regex_match: re.Match | None) -> list[str]:
        """Convert regex match to list of types."""
        import string

        if not regex_match:
            return []
        card_types = regex_match.group(1).split(". ")[0]
        card_types_split: list[str] = card_types.split(", ")
        if len(card_types_split) == 1:
            card_types_split = card_types.split(" and ")
        else:
            card_types_split[-1] = card_types_split[-1].split(" ", 1)[1]
        for index, value in enumerate(card_types_split):
            card_types_split[index] = string.capwords(value.split(" (")[0])
        return card_types_split

    def _download_mcm_from_s3(self) -> bool:
        """
        Download mkm_cards.parquet from S3 if not present locally.

        Returns:
            True if file exists (downloaded or already present), False otherwise
        """
        from mtgjson5.mtgjson_config import MtgjsonConfig
        from mtgjson5.mtgjson_s3_handler import MtgjsonS3Handler

        raw_cache = self.cache_path / "mkm_cards.parquet"

        if raw_cache.exists() and _cache_fresh(raw_cache):
            return True

        if not MtgjsonConfig().has_section("Prices"):
            LOGGER.debug("No S3 config, skipping MCM download")
            return raw_cache.exists()

        bucket_name = MtgjsonConfig().get("Prices", "bucket_name")
        s3_path = "mkm_cards.parquet"

        LOGGER.info("Downloading mkm_cards.parquet from S3...")
        if MtgjsonS3Handler().download_file(bucket_name, s3_path, str(raw_cache)):
            LOGGER.info(f"Downloaded mkm_cards.parquet ({raw_cache.stat().st_size / 1024 / 1024:.1f} MB)")
            return True

        LOGGER.warning("Failed to download mkm_cards.parquet from S3")
        return raw_cache.exists()

    def _load_mcm_lookup(self) -> None:
        """
        Load raw MCM data from CardMarket provider.
        """
        from mtgjson5.v2.providers.cardmarket.provider import load_cardmarket_data

        cache_path = self.cache_path / "mcm_lookup.parquet"

        if _cache_fresh(cache_path):
            self.mcm_lookup_lf = pl.read_parquet(cache_path).lazy()
            return

        # Try to download from S3 if not present
        self._download_mcm_from_s3()

        raw_cache = self.cache_path / "mkm_cards.parquet"
        raw_df = load_cardmarket_data(raw_cache)

        if raw_df is None or raw_df.is_empty():
            self.mcm_lookup_lf = pl.DataFrame(
                schema={
                    "mcmId": pl.String,
                    "mcmMetaId": pl.String,
                    "expansionName": pl.String,
                    "name": pl.String,
                    "number": pl.String,
                }
            ).lazy()
            return

        mcm_df = raw_df.select(
            [
                pl.col("mcmId").cast(pl.String),
                pl.col("mcmMetaId").cast(pl.String),
                pl.col("expansionName"),
                pl.col("name"),
                pl.col("number").cast(pl.String),
            ]
        )

        mcm_df.write_parquet(cache_path)
        self.mcm_lookup_lf = mcm_df.lazy()
        LOGGER.info(f"Loaded MCM data: {len(mcm_df):,} cards")

    def _normalize_all_columns(self) -> None:
        """Normalize ALL DataFrame columns to camelCase."""
        self.cards_lf = _normalize_columns(self.cards_lf)
        self.rulings_lf = _normalize_columns(self.rulings_lf)
        self.sets_lf = _normalize_columns(self.sets_lf)
        self.rulings_lf = _normalize_columns(self.rulings_lf)
        self.uuid_cache_lf = _normalize_columns(self.uuid_cache_lf)
        self.card_kingdom_lf = _normalize_columns(self.card_kingdom_lf)
        self.mcm_lookup_lf = _normalize_columns(self.mcm_lookup_lf)
        self.salt_lf = _normalize_columns(self.salt_lf)
        self.spellbook_lf = _normalize_columns(self.spellbook_lf)
        self.sld_subsets_lf = _normalize_columns(self.sld_subsets_lf)
        self.orientation_lf = _normalize_columns(self.orientation_lf)
        self.gatherer_lf = _normalize_columns(self.gatherer_lf)
        self.multiverse_bridge_lf = _normalize_columns(self.multiverse_bridge_lf)
        if self._github is not None:
            self._github.card_to_products_df = _normalize_columns(self._github.card_to_products_df)
            self._github.sealed_products_df = _normalize_columns(self._github.sealed_products_df)
            self._github.sealed_contents_df = _normalize_columns(self._github.sealed_contents_df)
            self._github.decks_df = _normalize_columns(self._github.decks_df)
            self._github.boosters_df = _normalize_columns(self._github.boosters_df)

    @property
    def scryfall(self) -> ScryfallProvider:
        """Get or create the Scryfall provider instance."""
        if self._scryfall is None:
            self._scryfall = ScryfallProvider()
        return self._scryfall

    @property
    def card_kingdom(self) -> CKProvider:
        """Get or create the Card Kingdom provider instance."""
        if self._cardkingdom is None:
            self._cardkingdom = CKProvider()
        return self._cardkingdom

    @property
    def cardmarket(self) -> CardMarketProvider:
        """Get or create the CardMarket provider instance."""
        if self._cardmarket is None:
            self._cardmarket = CardMarketProvider()
        return self._cardmarket

    @property
    def gatherer(self) -> GathererProvider:
        """Get or create the Gatherer provider instance."""
        if self._gatherer is None:
            self._gatherer = GathererProvider()
        return self._gatherer

    @property
    def github(self) -> SealedDataProvider:
        """Get or create the GitHub data provider instance."""
        if self._github is None:
            self._github = SealedDataProvider()
        return self._github

    @property
    def whats_in_standard(self) -> WhatsInStandardProvider:
        """Get or create the WhatsInStandard provider instance."""
        if self._standard is None:
            self._standard = WhatsInStandardProvider()
        return self._standard

    @property
    def edhrec(self) -> EdhrecSaltProvider:
        """Get or create the EDHREC saltiness provider instance."""
        if self._edhrec is None:
            self._edhrec = EdhrecSaltProvider()
        return self._edhrec

    @property
    def manapool(self) -> ManapoolPriceProvider:
        """Get or create the Manapool prices provider instance."""
        if self._manapool is None:
            self._manapool = ManapoolPriceProvider()
        return self._manapool

    @property
    def cardhoarder(self) -> CardHoarderProvider:
        """Get or create the Cardhoarder provider instance."""
        if self._cardhoarder is None:
            self._cardhoarder = CardHoarderProvider()
        return self._cardhoarder

    @property
    def tcgplayer(self) -> TCGProvider:
        """Get or create the TCGPlayer provider instance."""
        if self._tcgplayer is None:
            self._tcgplayer = TCGProvider()
        return self._tcgplayer

    @property
    def secretlair(self) -> SecretLairProvider:
        """Get or create the Secret Lair provider instance."""
        if self._secretlair is None:
            self._secretlair = SecretLairProvider()
        return self._secretlair

    @property
    def wizards(self) -> WizardsProvider:
        """Get or create the Wizards provider instance."""
        if self._wizards is None:
            self._wizards = WizardsProvider()
        return self._wizards

    @property
    def bulkdata(self) -> ScryfallProvider:
        """Get or create the Scryfall bulk data provider instance."""
        if self._scryfall is None:
            self._scryfall = ScryfallProvider()
        return self._scryfall

    @property
    def categoricals(self) -> DynamicCategoricals | None:
        """Get the DynamicCategoricals instance (set during load_all)."""
        return self._categoricals

    def get_tcg_to_uuid_map(self) -> dict[str, set[str]]:
        """Get mapping from TCGPlayer product ID to MTGJSON UUID(s)."""
        if self.tcg_to_uuid_lf is None:
            return {}
        df = self.tcg_to_uuid_lf.collect()
        if df.is_empty():
            return {}
        result: dict[str, set[str]] = {}
        for row in df.iter_rows(named=True):
            tcg_id = str(row.get("tcgplayerProductId", ""))
            uuid = row.get("uuid")
            if tcg_id and uuid:
                if tcg_id not in result:
                    result[tcg_id] = set()
                result[tcg_id].add(uuid)
        return result

    def get_tcg_etched_to_uuid_map(self) -> dict[str, set[str]]:
        """Get mapping from TCGPlayer etched product ID to MTGJSON UUID(s)."""
        if self.tcg_etched_to_uuid_lf is None:
            return {}
        df = self.tcg_etched_to_uuid_lf.collect()
        if df.is_empty():
            return {}
        result: dict[str, set[str]] = {}
        for row in df.iter_rows(named=True):
            tcg_id = str(row.get("tcgplayerEtchedProductId", ""))
            uuid = row.get("uuid")
            if tcg_id and uuid:
                if tcg_id not in result:
                    result[tcg_id] = set()
                result[tcg_id].add(uuid)
        return result

    def get_mtgo_to_uuid_map(self) -> dict[str, set[str]]:
        """Get mapping from MTGO ID to MTGJSON UUID(s)."""
        if self.mtgo_to_uuid_lf is None:
            return {}
        df = self.mtgo_to_uuid_lf.collect()
        if df.is_empty():
            return {}
        result: dict[str, set[str]] = {}
        for row in df.iter_rows(named=True):
            mtgo_id = str(row.get("mtgoId", ""))
            uuid = row.get("uuid")
            if mtgo_id and uuid:
                if mtgo_id not in result:
                    result[mtgo_id] = set()
                result[mtgo_id].add(uuid)
        return result

    def get_cardmarket_to_uuid_map(self) -> dict[str, str]:
        """Get mapping from CardMarket (MCM) ID to MTGJSON UUID."""
        if self.cardmarket_to_uuid_lf is None:
            return {}
        df = self.cardmarket_to_uuid_lf.collect()
        if df.is_empty():
            return {}
        return dict(
            zip(
                df["mcmId"].to_list(),
                df["uuid"].to_list(),
                strict=False,
            )
        )

    def get_cardmarket_to_finishes_map(self) -> dict[str, set[str]]:
        """Get mapping from CardMarket (MCM) ID to finishes.

        Returns:
            Dict mapping mcmId -> set of finish strings (e.g. {"foil", "nonfoil"})
        """
        # Use cards_lf which has cardmarketId and finishes columns
        if self.cards_lf is None:
            return {}
        try:
            df = (
                self.cards_lf.filter(pl.col("cardmarketId").is_not_null())
                .select(
                    [
                        pl.col("cardmarketId").cast(pl.String).alias("mcmId"),
                        pl.col("finishes"),
                    ]
                )
                .unique(subset=["mcmId"])
                .collect()
            )
        except Exception as e:
            LOGGER.warning(f"Failed to build cardmarket finishes map: {e}")
            return {}

        if df.is_empty():
            return {}

        result: dict[str, set[str]] = {}
        for row in df.iter_rows(named=True):
            mcm_id = row.get("mcmId")
            finishes = row.get("finishes")
            if mcm_id and finishes:
                result[mcm_id] = set(finishes) if isinstance(finishes, list) else set()
        return result

    def get_scryfall_to_uuid_map(self) -> dict[str, set[str]]:
        """Get mapping from Scryfall ID to MTGJSON UUID(s)."""
        if self.uuid_cache_lf is None:
            return {}
        df = self.uuid_cache_lf.collect()
        if df.is_empty():
            return {}

        # uuid_cache has scryfallId and cachedUuid columns
        result: dict[str, set[str]] = {}
        for row in df.iter_rows(named=True):
            scryfall_id = row.get("scryfallId")
            uuid = row.get("cachedUuid")
            if scryfall_id and uuid:
                if scryfall_id not in result:
                    result[scryfall_id] = set()
                result[scryfall_id].add(uuid)
        return result

    @classmethod
    def get_instance(cls) -> "GlobalCache":
        """Get the singleton GlobalCache instance."""
        if cls._instance is None:
            cls()
        return cls._instance  # type: ignore[return-value]


GLOBAL_CACHE = GlobalCache.get_instance()
