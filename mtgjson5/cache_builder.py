"""
Global cache for MTGJSON provider data and pre-computed aggregations.
"""

import json
import pathlib
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
from typing import Optional

import polars as pl

from mtgjson5 import constants
from mtgjson5.categoricals import DynamicCategoricals, discover_categoricals
from mtgjson5.providers import (
    CardHoarderProvider,
    CardKingdomProviderV2,
    CardMarketProvider,
    EdhrecSaltProvider,
    GathererProvider,
    GitHubDataProvider,
    ManapoolPricesProvider,
    MtgWikiProviderSecretLair,
    MultiverseBridgeProvider,
    ScryfallProvider,
    ScryfallProviderOrientationDetector,
    TCGPlayerProvider,
    WhatsInStandardProvider,
    ZachsScryfallClassIsTooCoolForElmo,
)
from mtgjson5.utils import LOGGER


def load_resource_json(filename: str) -> dict | list:
    """Load a JSON resource file and return raw data."""
    file_path = constants.RESOURCE_PATH / filename
    if not file_path.exists():
        LOGGER.warning(f"Resource file not found: {file_path}")
        return {}
    with file_path.open("rb") as f:
        return json.loads(f.read())


def _cache_fresh(path: pathlib.Path, max_age_hours: float = 24.0) -> bool:
    """Check if a cache file exists and is fresh."""
    if not path.exists():
        return False
    age_hours = (time.time() - path.stat().st_mtime) / 3600
    return age_hours < max_age_hours


class GlobalCache:
    """Global shared access cache for provider data."""

    _instance: Optional["GlobalCache"] = None

    def __new__(cls) -> "GlobalCache":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        if getattr(self, "_initialized", False):
            return

        self.CACHE_DIR: pathlib.Path = constants.CACHE_PATH
        self.CACHE_DIR.mkdir(parents=True, exist_ok=True)

        # Dynamic categoricals
        self.categoricals: DynamicCategoricals = None

        # Bulk data
        self.cards_df: pl.LazyFrame | None = None
        self.raw_rulings_df: pl.LazyFrame | None = None

        # Pre-computed aggregations
        self.sets_df: pl.DataFrame | None = None
        self.printings_df: pl.DataFrame | None = None
        self.rulings_df: pl.DataFrame | None = None
        self.foreign_data_df: pl.DataFrame | None = None
        self.uuid_cache_df: pl.DataFrame | None = None

        # Provider DataFrames
        self.card_kingdom_df: pl.DataFrame | None = None
        self.mcm_lookup_df: pl.DataFrame | None = None
        self.salt_df: pl.DataFrame | None = None
        self.spellbook_df: pl.DataFrame | None = None
        self.meld_lookup_df: pl.DataFrame | None = None
        self.sld_subsets_df: pl.DataFrame | None = None
        self.orientation_df: pl.DataFrame | None = None

        # Raw resource data
        self.duel_deck_sides: dict = {}
        self.meld_triplets: dict = {}
        self.world_championship_signatures: dict = {}
        self.manual_overrides: dict = {}
        self.gatherer_map: dict = {}
        self.multiverse_bridge_cards: dict = {}
        self.multiverse_bridge_sets: dict = {}
        self.standard_legal_sets: set[str] = set()

        # Provider instances (lazy)
        self._scryfall: ScryfallProvider | None = None
        self._cardkingdom: CardKingdomProviderV2 | None = None
        self._cardmarket: CardMarketProvider | None = None
        self._gatherer: GathererProvider | None = None
        self._multiverse: MultiverseBridgeProvider | None = None
        self._github: GitHubDataProvider | None = None
        self._edhrec: EdhrecSaltProvider | None = None
        self._standard: WhatsInStandardProvider | None = None
        self._manapool: ManapoolPricesProvider | None = None
        self._cardhoarder: CardHoarderProvider | None = None
        self._tcgplayer: TCGPlayerProvider | None = None
        self._secretlair: MtgWikiProviderSecretLair | None = None
        self._orientations: ScryfallProviderOrientationDetector | None = None
        self._too_cool: ZachsScryfallClassIsTooCoolForElmo | None = None

        self._initialized = True
        self._loaded = False

    # =========================================================================
    # Main Entry Point
    # =========================================================================

    def load_all(self, force_refresh: bool = False) -> "GlobalCache":
        """
        Load all data sources and pre-compute aggregations.

        Call this once at startup before building any sets.
        """
        if self._loaded and not force_refresh:
            LOGGER.info("Cache already loaded, skipping")
            return self

        # Download bulk data if needed
        self._download_bulk_data(force_refresh)

        # Load bulk data into LazyFrames
        self._load_bulk_data()

        # Load resource files
        self._load_resources()

        # Load provider data (parallel)
        self._load_providers_parallel()

        # Compute aggregations
        self._compute_aggregations()

        # categoricals
        self.categoricals = discover_categoricals(
            cards_lf=self.cards_df.lazy(),
            sets_lf=self.sets_df.lazy(),
            logger=LOGGER,
        )
        self._loaded = True

        return self

    # =========================================================================
    # Step 1: Download Bulk Data
    # =========================================================================

    def _download_bulk_data(self, force_refresh: bool = False) -> None:
        """Download Scryfall bulk data if missing or stale."""
        cards_path = self.CACHE_DIR / "all_cards.ndjson"
        rulings_path = self.CACHE_DIR / "rulings.ndjson"

        needs_download = force_refresh
        if not cards_path.exists() or cards_path.stat().st_size == 0:
            needs_download = True
        if not rulings_path.exists() or rulings_path.stat().st_size == 0:
            needs_download = True

        # Check age (24h max)
        if not needs_download and cards_path.exists():
            age_hours = (time.time() - cards_path.stat().st_mtime) / 3600
            if age_hours > 24:
                LOGGER.info(f"Bulk data is {age_hours:.1f}h old, refreshing...")
                needs_download = True

        if needs_download:
            LOGGER.info("[1/5] Downloading bulk data...")
            self.too_cool.download_bulk_files_sync(
                self.CACHE_DIR, ["all_cards", "rulings"], force_refresh
            )
        else:
            LOGGER.info("[1/5] Using cached bulk data")

    # =========================================================================
    # Step 2: Load Bulk Data
    # =========================================================================

    def _load_bulk_data(self) -> None:
        """Load bulk NDJSON files into LazyFrames."""
        LOGGER.info("[2/5] Loading bulk data into LazyFrames...")

        cards_path = self.CACHE_DIR / "all_cards.ndjson"
        rulings_path = self.CACHE_DIR / "rulings.ndjson"

        # Columns that may be numeric in some records but should be strings
        # (e.g., power/toughness can be "*", "1+*", etc.)
        string_cast_columns = [
            "power",
            "toughness",
            "loyalty",
            "defense",
            "hand_modifier",
            "life_modifier",
        ]

        # Scan without schema overrides first (avoid errors on missing columns)
        self.cards_df = pl.scan_ndjson(
            cards_path,
            infer_schema_length=10000,
        )

        # Get the actual schema to know which columns exist
        schema = self.cards_df.collect_schema()

        # Build cast expressions only for columns that actually exist
        cast_exprs = []
        for col_name in string_cast_columns:
            if col_name in schema:
                cast_exprs.append(pl.col(col_name).cast(pl.Utf8))

        if cast_exprs:
            self.cards_df = self.cards_df.with_columns(cast_exprs)
            LOGGER.info(
                f"  Cast {len(cast_exprs)} columns to Utf8: {[c for c in string_cast_columns if c in schema]}"
            )

        # Add missing optional columns with null defaults
        # These columns may not exist in older Scryfall data or may be nested in card_faces
        optional_columns = {
            "defense": pl.Utf8,  # Battle cards only - may be nested in card_faces
        }
        missing_cols = []
        for col_name, dtype in optional_columns.items():
            if col_name not in schema:
                missing_cols.append(pl.lit(None).cast(dtype).alias(col_name))

        if missing_cols:
            self.cards_df = self.cards_df.with_columns(missing_cols)
            LOGGER.info(
                f"  Added {len(missing_cols)} missing optional columns: {[c for c in optional_columns.keys() if c not in schema]}"
            )

        self.raw_rulings_df = pl.scan_ndjson(rulings_path, infer_schema_length=1000)
        LOGGER.info("  Loaded bulk data as LazyFrames")

    # =========================================================================
    # Step 3: Load Resources
    # =========================================================================

    def _load_resources(self) -> None:
        """Load local JSON resource files."""
        LOGGER.info("[3/5] Loading resource files...")

        self.duel_deck_sides = load_resource_json("duel_deck_sides.json")
        self.meld_data = load_resource_json("meld_triplets.json")
        self.world_championship_signatures = load_resource_json(
            "world_championship_signatures.json"
        )
        self.manual_overrides = load_resource_json("manual_overrides.json")

        # UUID cache -> DataFrame
        uuid_raw = load_resource_json("legacy_mtgjson_v5_uuid_mapping.json")
        if uuid_raw:
            rows = [
                {"scryfall_id": sid, "side": side, "cached_uuid": uuid}
                for sid, sides in uuid_raw.items()
                for side, uuid in sides.items()
            ]
            self.uuid_cache_df = pl.DataFrame(rows)
            LOGGER.info(f"  Loaded {len(rows):,} cached UUIDs")

        # Meld triplets -> DataFrame for joining
        self.meld_triplets: dict[str, list[str]] = {}
        for triplet in self.meld_data:
            if len(triplet) == 3:
                for name in triplet:
                    self.meld_triplets[name] = triplet

    # =========================================================================
    # Step 4: Load Providers (Parallel)
    # =========================================================================

    def _load_providers_parallel(self) -> None:
        """Load external provider data in parallel."""
        LOGGER.info("[4/5] Loading provider data (parallel)...")

        # Load sets first (fast, ~0.3s) - needed by MCM loader

        LOGGER.info("  Loaded sets")

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(self._load_orientations): "orientations",
                executor.submit(self._load_card_kingdom): "card_kingdom",
                executor.submit(self._load_edhrec_salt): "edhrec",
                executor.submit(self._load_multiverse_bridge): "multiverse_bridge",
                executor.submit(self._load_gatherer): "gatherer",
                executor.submit(self._load_whats_in_standard): "standard",
                executor.submit(self._load_github_data): "github",
                executor.submit(self._load_spellbook): "spellbook",
                executor.submit(self._load_secretlair_subsets): "secretlair",
                executor.submit(self._load_mcm_lookup): "mcm",
            }

            for future in as_completed(futures):
                name = futures[future]
                try:
                    future.result()
                    LOGGER.info(f"  Loaded {name}")
                except Exception as e:
                    LOGGER.error(f"  Failed to load {name}: {e}")

    def _load_sets_metadata(self) -> None:
        """Load set metadata from Scryfall."""
        cache_path = self.CACHE_DIR / "sets.parquet"

        if _cache_fresh(cache_path):
            self.sets_df = pl.read_parquet(cache_path)
            return

        sets_response = self.scryfall.download(self.scryfall.ALL_SETS_URL)
        if sets_response.get("object") == "error":
            LOGGER.error(f"Failed to load sets: {sets_response}")
            self.sets_df = pl.DataFrame()
            return

        sets_data = sets_response.get("data", [])
        for s in sets_data:
            s["code"] = s["code"].upper()

        self.sets_df = pl.DataFrame(sets_data)
        self.sets_df.write_parquet(cache_path)

    def _load_card_kingdom(self) -> None:
        """Load Card Kingdom data with caching."""
        cache_path = self.CACHE_DIR / "ck_prices.parquet"

        if _cache_fresh(cache_path):
            self.card_kingdom.load(cache_path)
            self.card_kingdom_df = self.card_kingdom.get_join_data()
            return

        self.card_kingdom.download()
        self.card_kingdom.save(cache_path)
        self.card_kingdom_df = self.card_kingdom.get_join_data()

    def _load_edhrec_salt(self) -> None:
        """Load EDHREC saltiness data."""
        cache_path = self.CACHE_DIR / "edhrec_salt.parquet"

        if _cache_fresh(cache_path):
            self.salt_df = pl.read_parquet(cache_path)
            return

        self.salt_df = self.edhrec.get_data_frame()
        if self.salt_df is not None and not self.salt_df.is_empty():
            self.salt_df.write_parquet(cache_path)

    def _load_multiverse_bridge(self) -> None:
        """Load MultiverseBridge Rosetta Stone data."""
        cards_cache = self.CACHE_DIR / "multiverse_bridge_cards.json"
        sets_cache = self.CACHE_DIR / "multiverse_bridge_sets.json"

        if _cache_fresh(cards_cache) and _cache_fresh(sets_cache):
            with cards_cache.open("rb") as f:
                self.multiverse_bridge_cards = json.loads(f.read())
            with sets_cache.open("rb") as f:
                self.multiverse_bridge_sets = json.loads(f.read())
            return

        self.multiverse_bridge_cards = getattr(
            self.multiverse, "rosetta_stone_cards", {}
        )
        self.multiverse_bridge_sets = getattr(self.multiverse, "rosetta_stone_sets", {})

        with cards_cache.open("w", encoding="utf-8") as f:
            json.dump(self.multiverse_bridge_cards, f)
        with sets_cache.open("w", encoding="utf-8") as f:
            json.dump(self.multiverse_bridge_sets, f)

    def _load_gatherer(self) -> None:
        """Load Gatherer original text data."""
        cache_path = self.CACHE_DIR / "gatherer_map.json"

        if _cache_fresh(cache_path):
            with cache_path.open("rb") as f:
                self.gatherer_map = json.loads(f.read())
            return

        self.gatherer_map = getattr(self.gatherer, "_multiverse_id_to_data", {})
        with cache_path.open("w", encoding="utf-8") as f:
            json.dump(self.gatherer_map, f)

    def _load_whats_in_standard(self) -> None:
        """Load current Standard-legal sets."""
        cache_path = self.CACHE_DIR / "standard_sets.json"

        if _cache_fresh(cache_path):
            with cache_path.open("rb") as f:
                self.standard_legal_sets = set(json.loads(f.read()))
            return

        self.standard_legal_sets = set(self.whats_in_standard.set_codes or [])
        with cache_path.open("w", encoding="utf-8") as f:
            json.dump(list(self.standard_legal_sets), f)

    def _load_github_data(self) -> None:
        """Load GitHub sealed/deck/booster data."""
        card_to_products_cache = self.CACHE_DIR / "github_card_to_products.parquet"
        sealed_products_cache = self.CACHE_DIR / "github_sealed_products.parquet"
        sealed_contents_cache = self.CACHE_DIR / "github_sealed_contents.parquet"
        decks_cache = self.CACHE_DIR / "github_decks.parquet"
        booster_cache = self.CACHE_DIR / "github_booster.parquet"

        all_cached = all(
            _cache_fresh(p)
            for p in [
                card_to_products_cache,
                sealed_products_cache,
                sealed_contents_cache,
                decks_cache,
                booster_cache,
            ]
        )

        if all_cached:
            self.github._card_to_products_df = pl.read_parquet(card_to_products_cache)
            self.github._sealed_products_df = pl.read_parquet(sealed_products_cache)
            self.github._sealed_contents_df = pl.read_parquet(sealed_contents_cache)
            self.github._decks_df = pl.read_parquet(decks_cache)
            self.github._booster_df = pl.read_parquet(booster_cache)
            return

        self.github.preload_all()

        if self.github._card_to_products_df is not None:
            self.github._card_to_products_df.write_parquet(card_to_products_cache)
        if self.github._sealed_products_df is not None:
            self.github._sealed_products_df.write_parquet(sealed_products_cache)
        if self.github._sealed_contents_df is not None:
            self.github._sealed_contents_df.write_parquet(sealed_contents_cache)
        if self.github._decks_df is not None:
            self.github._decks_df.write_parquet(decks_cache)
        if self.github._booster_df is not None:
            self.github._booster_df.write_parquet(booster_cache)

    def _load_orientations(self) -> None:
        cache_path = self.CACHE_DIR / "orientations.parquet"
        if _cache_fresh(cache_path):
            self.orientation_df = pl.read_parquet(cache_path)
            return
        detector = self.ScryfallProviderOrientationDetector()
        art_series_sets = self.sets_df.filter(
            pl.col("name").str.contains("Art Series")
        )["code"].to_list()

        rows = []
        for set_code in art_series_sets:
            orientation_map = detector.get_uuid_to_orientation_map(set_code)
            for scryfall_id, orientation in (orientation_map or {}).items():
                rows.append({"scryfall_id": scryfall_id, "orientation": orientation})

        self.orientation_df = pl.DataFrame(rows) if rows else pl.DataFrame()
        if not self.orientation_df.is_empty():
            self.orientation_df.write_parquet(cache_path)

    def _load_spellbook(self) -> None:
        """Load Alchemy spellbook data."""
        cache_path = self.CACHE_DIR / "spellbook.parquet"

        if _cache_fresh(cache_path):
            self.spellbook_df = pl.read_parquet(cache_path)
            return

        self.spellbook_df = self.scryfall.build_spellbook_df()
        if self.spellbook_df is not None and not self.spellbook_df.is_empty():
            self.spellbook_df.write_parquet(cache_path)

    def _load_secretlair_subsets(self) -> None:
        """Load Secret Lair subset mappings."""
        cache_path = self.CACHE_DIR / "sld_subsets.parquet"

        if _cache_fresh(cache_path):
            self.sld_subsets_df = pl.read_parquet(cache_path)
            return

        relation_map = self.secretlair.download()
        if relation_map:
            rows = [
                {"number": num, "subsets": [name]} for num, name in relation_map.items()
            ]
            self.sld_subsets_df = pl.DataFrame(rows)
            self.sld_subsets_df.write_parquet(cache_path)

    def _load_mcm_lookup(self) -> None:
        """
        Build MCM lookup table from CardMarket provider.

        Must be called after sets_df is loaded since it iterates through all sets.
        This is a slow operation (~2-5 min) as it fetches data for each set.
        """
        cache_path = self.CACHE_DIR / "mcm_lookup.parquet"

        if _cache_fresh(cache_path):
            self.mcm_lookup_df = pl.read_parquet(cache_path)
            return

        if self.sets_df is None or self.sets_df.is_empty():
            LOGGER.warning("Sets not loaded, skipping MCM lookup table")
            return

        LOGGER.info("Building MCM lookup table (this may take a few minutes)...")
        try:
            self.mcm_lookup_df = self.cardmarket.build_mkm_df(self.sets_df)
            if self.mcm_lookup_df is not None and not self.mcm_lookup_df.is_empty():
                self.mcm_lookup_df.write_parquet(cache_path)
        except Exception as e:
            LOGGER.error(f"Failed to build MCM lookup: {e}")
            self.mcm_lookup_df = pl.DataFrame()

    # =========================================================================
    # Step 5: Compute Aggregations
    # =========================================================================

    def _compute_aggregations(self) -> None:
        """Pre-compute joined/aggregated DataFrames."""
        LOGGER.info("[5/5] Computing aggregations...")

        if self.cards_df is None:
            raise RuntimeError("Bulk data not loaded")

        # Printings: oracle_id -> List[set_code]
        self.printings_df = (
            self.cards_df.select(["oracle_id", "set"])
            .group_by("oracle_id")
            .agg(pl.col("set").str.to_uppercase().unique().sort().alias("printings"))
            .collect()
        )
        LOGGER.info(f"Computed printings for {len(self.printings_df):,} oracle IDs")

        # Rulings: oracle_id -> List[{date, text}]
        if self.raw_rulings_df is not None:
            self.rulings_df = (
                self.raw_rulings_df.sort(["published_at", "comment"])
                .group_by("oracle_id")
                .agg(
                    pl.struct(
                        [
                            pl.col("published_at").alias("date"),
                            pl.col("comment").alias("text"),
                        ]
                    ).alias("rulings")
                )
                .collect()
            )
            LOGGER.info(f"Computed rulings for {len(self.rulings_df):,} oracle IDs")

        # Foreign data: (set, collector_number) -> List[ForeignData]
        self.foreign_data_df = (
            self.cards_df.filter(pl.col("lang") != "en")
            .select(
                [
                    pl.col("set").str.to_uppercase().alias("set_code"),
                    "collector_number",
                    "lang",
                    "id",
                    "name",
                    "printed_name",
                    "printed_text",
                    "printed_type_line",
                    "flavor_text",
                    "multiverse_ids",
                ]
            )
            .with_columns(
                [
                    pl.col("lang")
                    .replace_strict(constants.LANGUAGE_MAP, default=pl.col("lang"))
                    .alias("language"),
                    pl.col("multiverse_ids").list.first().alias("multiverse_id"),
                ]
            )
            .group_by(["set_code", "collector_number"])
            .agg(
                pl.struct(
                    [
                        "language",
                        pl.col("id").alias("scryfall_id"),
                        "multiverse_id",
                        pl.coalesce("printed_name", "name").alias("name"),
                        pl.col("printed_text").alias("text"),
                        pl.col("printed_type_line").alias("type"),
                        "flavor_text",
                    ]
                ).alias("foreign_data")
            )
            .collect()
        )
        LOGGER.info(f"Computed foreign data for {len(self.foreign_data_df):,} cards")

    def save_categoricals_snapshot(self, output_path: pathlib.Path) -> None:
        """Save current categoricals to JSON for comparison."""
        if self.categoricals is None:
            return
        with output_path.open("w", encoding="utf-8") as f:
            json.dump(asdict(self.categoricals), f, indent=2, sort_keys=True)

    def load_previous_categoricals(
        self, path: pathlib.Path
    ) -> DynamicCategoricals | None:
        """Load previous categoricals for diff comparison."""
        if not path.exists():
            return None
        with path.open(encoding="utf-8") as f:
            data = json.load(f)
        return DynamicCategoricals(**data)

    # =========================================================================
    # Provider Properties (Lazy Init)
    # =========================================================================

    @property
    def scryfall(self) -> ScryfallProvider:
        if self._scryfall is None:
            self._scryfall = ScryfallProvider()
        return self._scryfall

    @property
    def card_kingdom(self) -> CardKingdomProviderV2:
        if self._cardkingdom is None:
            self._cardkingdom = CardKingdomProviderV2()
        return self._cardkingdom

    @property
    def cardmarket(self) -> CardMarketProvider:
        if self._cardmarket is None:
            self._cardmarket = CardMarketProvider()
        return self._cardmarket

    @property
    def multiverse(self) -> MultiverseBridgeProvider:
        if self._multiverse is None:
            self._multiverse = MultiverseBridgeProvider()
        return self._multiverse

    @property
    def gatherer(self) -> GathererProvider:
        if self._gatherer is None:
            self._gatherer = GathererProvider()
        return self._gatherer

    @property
    def github(self) -> GitHubDataProvider:
        if self._github is None:
            self._github = GitHubDataProvider()
        return self._github

    @property
    def whats_in_standard(self) -> WhatsInStandardProvider:
        if self._standard is None:
            self._standard = WhatsInStandardProvider()
        return self._standard

    @property
    def edhrec(self) -> EdhrecSaltProvider:
        if self._edhrec is None:
            self._edhrec = EdhrecSaltProvider()
        return self._edhrec

    @property
    def manapool(self) -> ManapoolPricesProvider:
        if self._manapool is None:
            self._manapool = ManapoolPricesProvider()
        return self._manapool

    @property
    def cardhoarder(self) -> CardHoarderProvider:
        if self._cardhoarder is None:
            self._cardhoarder = CardHoarderProvider()
        return self._cardhoarder

    @property
    def tcgplayer(self) -> TCGPlayerProvider:
        if self._tcgplayer is None:
            self._tcgplayer = TCGPlayerProvider()
        return self._tcgplayer

    @property
    def secretlair(self) -> MtgWikiProviderSecretLair:
        if self._secretlair is None:
            self._secretlair = MtgWikiProviderSecretLair()
        return self._secretlair

    @property
    def too_cool(self) -> ZachsScryfallClassIsTooCoolForElmo:
        if self._toocool is None:
            self._toocool = ZachsScryfallClassIsTooCoolForElmo()
        return self._toocool

    # =========================================================================
    # Singleton Access
    # =========================================================================

    @classmethod
    def get_instance(cls) -> "GlobalCache":
        if cls._instance is None:
            cls()
        return cls._instance


GLOBAL_CACHE = GlobalCache.get_instance()
