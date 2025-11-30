"""
Global cache for MTGJSON provider data and pre-computed aggregations.
"""

import gzip
import json
import logging
import pathlib
import requests
from concurrent.futures import ThreadPoolExecutor  # pylint: disable=no-name-in-module
from functools import cache
from typing import Any, Optional, TypeVar
import polars as pl

from mtgjson5 import categoricals, constants
from mtgjson5.providers import (
    CardMarketProvider,
    ScryfallProvider,
    MultiverseBridgeProvider,
    GathererProvider,
    GitHubDataProvider,
    CardKingdomProviderV2,
    WhatsInStandardProvider,
    EdhrecSaltProvider   
)

LOGGER = logging.getLogger(__name__)


T = TypeVar("T")


SCRYFALL_BULK_DATA_URL = "https://api.scryfall.com/bulk-data"
CACHE_DIR = constants.RESOURCE_PATH / ".cache"


def get_bulk_data_download_url(bulk_type: str) -> str:
    """
    Get the download URL for a specific bulk data type from Scryfall.
    :param bulk_type: Type of bulk data ("default_cards", "rulings", etc.)
    :return: Download URL for the bulk data
    """
    response = requests.get(SCRYFALL_BULK_DATA_URL)
    response.raise_for_status()
    data = response.json()

    for item in data.get("data", []):
        if item.get("type") == bulk_type:
            return item.get("download_uri", "")

    raise ValueError(f"Bulk data type '{bulk_type}' not found")


def fetch_bulk_data(url: str, destination: pathlib.Path) -> None:
    """
    Fetch bulk data from Scryfall and save as NDJSON file.
    :param url: URL to fetch from
    :param destination: File path to save data
    """
    LOGGER.info(f"Fetching bulk data from {url}...")
    response = requests.get(url)
    response.raise_for_status()

    destination.parent.mkdir(parents=True, exist_ok=True)

    # Decompress if gzipped
    content = response.content
    if content[:2] == b'\x1f\x8b':
        content = gzip.decompress(content)

    # Parse JSON array and write as NDJSON
    data = json.loads(content)
    with destination.open('w', encoding='utf-8') as f:
        for item in data:
            f.write(json.dumps(item))
            f.write('\n')

    LOGGER.info(f"Saved {len(data)} items to {destination}")


def apply_categoricals(df: pl.DataFrame) -> pl.DataFrame:
    """
    Cast string columns to categorical types using pre-defined categories.

    This enables O(1) integer comparisons instead of O(n) string comparisons
    for columns like rarity, layout, border_color, etc.

    Should be called once when loading bulk data.
    """
    cats = categoricals.STATIC_CATEGORICALS
    schema = df.collect_schema() if hasattr(df, 'collect_schema') else df.schema

    casts = []
    for col_name in cats.keys():
        if col_name in schema:
            col_type = schema[col_name]
            # Only cast String columns, skip List columns
            if col_type == pl.String or col_type == pl.Utf8:
                dtype = pl.Categorical(ordering="physical")
                casts.append(pl.col(col_name).cast(dtype))

    if casts:
        df = df.with_columns(casts)

    return df


def get_scryfall_DataFrame(ndjson_path: str) -> pl.DataFrame:
    """
    Load Scryfall NDJSON file into a Polars DataFrame with schema overrides.
    :param ndjson_path: Path to NDJSON file
    :return: Polars DataFrame with Scryfall data
    """
    LOGGER.info(f"[CACHE] Loading Scryfall NDJSON file from {ndjson_path}...")
    # lock polymorphic fields to Utf8 to prevent type errors.
    schema_overrides = {
        "power": pl.Utf8,  # to handle "*", "1/2", etc.
        "toughness": pl.Utf8,
        "loyalty": pl.Utf8,
        "defense": pl.Utf8,
        "hand_modifier": pl.Utf8,
        "life_modifier": pl.Utf8,
        "collector_number": pl.Utf8,  # "123a"
        "mtgo_id": pl.Utf8,
        "arena_id": pl.Utf8,
        "tcgplayer_id": pl.Utf8,
        "cardmarket_id": pl.Utf8,
        "illustration_id": pl.Utf8,
        "card_back_id": pl.Utf8,
    }
    
    try:
        # infer_schema_length=None:
        # Causes polars to create a "Union Schema" of every column in the file
        # and then process it in one pass via rayon's parallel iterators.
        # this solves the scryfall's Semi-Variable Schema problem lickedy-split.
        lf: pl.DataFrame = pl.scan_ndjson(
            ndjson_path,
            infer_schema_length=None,
            schema_overrides=schema_overrides,
            batch_size=4096
        )
        # Cast categorical fields before materialization (better memory usage)
        lf = apply_categoricals(lf)
        
        # Materialize
        df = lf.collect()

        LOGGER.info(f"Loaded {df.shape[0]} records from {ndjson_path}")
        return df
    except Exception as e:
        LOGGER.error(f"Error loading NDJSON file {ndjson_path}: {e}")
        raise


def extract_foreign_data(cards: pl.LazyFrame) -> pl.LazyFrame:
    """
    Extract foreign language data from non-English cards.
    Returns grouped foreign_data ready to join to English cards.

    Orders by multiverse_id to match legacy MTGJSON behavior.
    """
    foreign = (
        cards.filter(pl.col("lang") != "en")
        .select(
            [
                "set",
                "collector_number",
                "lang",
                "id",
                "name",
                "multiverse_ids",
                "printed_name",
                "printed_text",
                "printed_type_line",
                "flavor_text",
                "card_faces",
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
    )
    foreign_struct = (
        foreign
        .sort("language")  # Sort by language before grouping
        .select(
            [
                "set",
                "collector_number",
                "multiverse_id",  # Keep for sorting within group
                pl.struct(
                    [
                        "language",
                        pl.col("id").alias("scryfall_id"),
                        "multiverse_id",
                        # scryfall name for face matching logic
                        pl.col("name").alias("scryfall_name"),
                        # for single-faces: use printed_name or fallback to name
                        # for multi-faces: we overwrite at lookup
                        pl.coalesce(["printed_name", "name"]).alias("name"),
                        pl.col("printed_text").alias("text"),
                        pl.col("printed_type_line").alias("type"),
                        "flavor_text",
                        # store full card_faces array for resolution later
                        pl.col("card_faces"),
                    ]
                ).alias("foreign_entry"),
            ]
        )
        .sort(["set", "collector_number", "multiverse_id"])  # Sort within groups
        .group_by(["set", "collector_number"])
        .agg(pl.col("foreign_entry").alias("foreign_data"))
    )
    return foreign_struct



@cache
def load_json(filename: str) -> Any:
    """Load a JSON resource from the resources directory."""
    return json.loads((constants.RESOURCE_PATH / filename).read_text())


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
        LOGGER.info("[CACHE] Initializing...")

        # Bulk data files
        self.cards_df: pl.DataFrame | None = None
        self.raw_rulings_df: pl.DataFrame | None = None

        # Providers (lazy loaded)
        self._scryfall: ScryfallProvider | None = None
        self._card_kingdom: CardKingdomProviderV2 | None = None
        self._cardmarket: CardMarketProvider | None = None
        self._gatherer: GathererProvider | None = None
        self._multiverse: MultiverseBridgeProvider | None = None
        self._github: GitHubDataProvider | None = None
        self._edhrec: EdhrecSaltProvider | None = None
        self._whats_in_standard: WhatsInStandardProvider | None = None
        
        # Pre-computed aggregations
        self.printings_df: pl.DataFrame | None = None
        self.rulings_df: pl.DataFrame | None = None
        self.foreign_data_df: pl.DataFrame | None = None
        self.card_kingdom_df: pl.DataFrame | None = None
        self.multiverse_bridge_df: pl.DataFrame | None = None
        self.uuid_cache_df: pl.DataFrame | None = None
        self.salt_df: pl.DataFrame | None = None
        self.cardhoarder_df: dict[str, dict[str, float | None]] = {}

        # Metadata
        self.sets_df: pl.DataFrame | None = None

        # Resource overrides
        self.manual_overrides: dict[str, dict[str, Any]] = {}
        self.keyrune_overrides: dict[str, str] = {}
        self.tcgplayer_id_overrides: dict[str, int] = {}
        self.meld_triplets: list[list[str]] = []
        self.wc_signatures: dict[str, dict[str, str]] = {}
        self.mkm_translations: dict[str, dict[str, str]] = {}
        self.base_set_sizes: dict[str, int] = {}

        # Provider data caches
        self.gatherer_map: dict[str, list[dict[str, str]]] = {}
        self.standard_legal_sets: set[str] = set()
        self.card_kingdom_map: dict[str, list[dict[str, Any]]] = {}
        self.multiverse_bridge_cards: dict[str, list[dict[str, Any]]] = {}
        self.multiverse_bridge_sets: dict[str, int] = {}

        self._initialized = True

    # Lazy provider accessors
    @property
    def scryfall(self) -> ScryfallProvider:
        """Get or create the Scryfall provider instance."""
        if self._scryfall is None:
            self._scryfall = ScryfallProvider()
        return self._scryfall

    @property
    def card_kingdom(self) -> CardKingdomProviderV2:
        """Get or create the Card Kingdom provider instance."""
        if self._card_kingdom is None:
            self._card_kingdom = CardKingdomProviderV2()
        return self._card_kingdom

    @property
    def cardmarket(self) -> CardMarketProvider:
        """Get or create the Cardmarket provider instance."""
        if self._cardmarket is None:
            self._cardmarket = CardMarketProvider()
        return self._cardmarket
    
    @property
    def multiverse(self) -> MultiverseBridgeProvider:
        """Get or create the Multiverse Bridge provider instance."""
        if self._multiverse is None:
            self._multiverse = MultiverseBridgeProvider()
        return self._multiverse
    
    @property
    def gatherer(self) -> GathererProvider:
        """Get or create the Gatherer provider instance."""
        if self._gatherer is None:
            self._gatherer = GathererProvider()
        return self._gatherer
    
    @property
    def github(self) -> GitHubDataProvider:
        """Get or create the GitHub Data provider instance."""
        if self._github is None:
            self._github = GitHubDataProvider()
        return self._github
    
    @property
    def whats_in_standard(self) -> WhatsInStandardProvider:
        if self._whats_in_standard is None:
            self._whats_in_standard = WhatsInStandardProvider()
        return self._whats_in_standard

    @property
    def edhrec(self) -> EdhrecSaltProvider:
        """Get or create the EDHREC provider instance."""
        if self._edhrec is None:
            self._edhrec = EdhrecSaltProvider()
        return self._edhrec

    def load_all(self, force_refresh: bool = False) -> "GlobalCache":
        """Load all provider data and pre-compute aggregations."""
        LOGGER.info("Loading global cache data...")

        self._load_sets_metadata()

        # Load bulk data files
        cards_cache = CACHE_DIR / "all_cards.ndjson"
        rulings_cache = CACHE_DIR / "rulings.ndjson"
        oracle_cache = CACHE_DIR / "default_cards.ndjson"
        
        if force_refresh or not cards_cache.exists() or cards_cache.stat().st_size == 0:
            fetch_bulk_data(get_bulk_data_download_url("all_cards"), cards_cache)
        if force_refresh or not rulings_cache.exists() or rulings_cache.stat().st_size == 0:
            fetch_bulk_data(get_bulk_data_download_url("rulings"), rulings_cache)
        if force_refresh or not oracle_cache.exists() or oracle_cache.stat().st_size == 0:
            fetch_bulk_data(get_bulk_data_download_url("default_cards"), oracle_cache)
            
        self.foreign_data_df = extract_foreign_data(
            get_scryfall_DataFrame(str(cards_cache)).lazy()
        ).collect()
        
        self.cards_df = get_scryfall_DataFrame(str(cards_cache)).with_columns(
            pl.col("set").str.to_uppercase()
        )
        self.oracle_df = get_scryfall_DataFrame(str(oracle_cache))
        self.raw_rulings_df = pl.read_ndjson(str(rulings_cache))

        with ThreadPoolExecutor(max_workers=8) as executor:
            ck_future = executor.submit(self._load_card_kingdom_v2)
            mb_cards_future = executor.submit(self._load_multiverse_bridge_cards)
            mb_sets_future = executor.submit(self._load_multiverse_bridge_sets)
            gatherer_future = executor.submit(self._load_gatherer_data)
            github_future = executor.submit(self._load_github_data)
            edhrec_future = executor.submit(self._load_salt_data)
            self.card_kingdom_df = ck_future.result(timeout=180)
            self.multiverse_bridge_cards = mb_cards_future.result(timeout=120)  
            self.multiverse_bridge_sets = mb_sets_future.result(timeout=120)
            self.gatherer_map = gatherer_future.result(timeout=120)
            self.salt_df = edhrec_future.result(timeout=120)
            github_future.result(timeout=120)
        
        self._compute_aggregations()
        self._load_uuid_cache()
        self._load_overrides()
        
        return self

    def _load_sets_metadata(self) -> None:
        """Load set metadata from Scryfall."""
        LOGGER.info("Loading sets metadata...")
         
        sets_response = self.scryfall.download(self.scryfall.ALL_SETS_URL)
        
        if sets_response.get("object") == "error":
            LOGGER.error(f"Failed to load sets: {sets_response}")
            self.sets_df = pl.DataFrame()
            return
        sets_data = sets_response.get("data", [])
        
        for s in sets_data:
            s["code"] = s["code"].upper()
        
        self.sets_df = pl.DataFrame(sets_data)

    def _load_overrides(self) -> None:
        """Load all resource overrides."""
        LOGGER.info("Loading resource overrides...")
        self.manual_overrides = load_json("manual_overrides.json")
        self.keyrune_overrides = load_json("keyrune_code_overrides.json")
        self.tcgplayer_id_overrides = load_json("tcgplayer_set_id_overrides.json")
        self.meld_triplets = load_json("meld_triplets.json")
        self.wc_signatures = load_json("world_championship_signatures.json")
        self.mkm_translations = load_json("mkm_set_name_translations.json")
        self.base_set_sizes = load_json("base_set_sizes.json")
        LOGGER.info("Resource overrides loaded")

    def _load_multiverse_bridge_cards(self) -> dict[str, list[dict[str, Any]]]:
        """Load Multiverse Bridge Rosetta Stone cards."""
        LOGGER.info("[CACHE]Loading Multiverse Bridge Rosetta Stone cards...")
        return getattr(self._multiverse, "rosetta_stone_cards", {})

    def _load_multiverse_bridge_sets(self) -> dict[str, int]:
        """Load Multiverse Bridge Rosetta Stone sets."""
        LOGGER.info("[CACHE]Loading Multiverse Bridge Rosetta Stone sets...")
        return getattr(self._multiverse, "rosetta_stone_sets", {})

    def _load_card_kingdom_v2(self) -> pl.DataFrame:
        """Load Card Kingdom data using v2 provider with parquet caching."""
        LOGGER.info("[CACHE]Loading Card Kingdom data...")
        cache_path = CACHE_DIR / "ck_prices.parquet"

        if cache_path.exists():
            # Check if cache is less than 24 hours old
            import time
            cache_age_hours = (time.time() - cache_path.stat().st_mtime) / 3600
            if cache_age_hours < 24:
                LOGGER.info(f"Loading CK data from cache ({cache_age_hours:.1f}h old)")
                self.card_kingdom.load(cache_path)
                return self.card_kingdom.get_join_data()
            else:
                LOGGER.info(f"CK cache expired ({cache_age_hours:.1f}h old), refreshing...")

        self.card_kingdom.download()
        self.card_kingdom.save(cache_path)
        return self.card_kingdom.get_join_data()

    def _load_github_data(self) -> None:
        """Load GitHub data (sealed products, boosters, card map)."""
        LOGGER.info("[CACHE]Loading GitHub data...")
        self.github.load(["card_map", "sealed", "boosters"])

    def _load_gatherer_data(self) -> dict[str, list[dict[str, str]]]:
        """Load Gatherer data for cards."""
        LOGGER.info("[CACHE]Loading Gatherer data...")
        return getattr(self.gatherer, "_multiverse_id_to_data", {})
    
    def _load_salt_data(self) -> pl.DataFrame:
        """Load EDHREC salt data for cards."""
        LOGGER.info("[CACHE]Loading EDHREC salt data...")
        return self.edhrec.get_data_frame()

    def _compute_aggregations(self) -> None:
        """Build pre-computed maps from bulk data."""
        if self.cards_df is None:
            raise RuntimeError("Must load bulk data first")
        LOGGER.info("[CACHE]Computing pre-aggregated maps...")
        # Printings: oracle_id -> list of set codes (all languages, not just English)
        # Some sets like RFIN are Japanese-only promos
        self.printings_df = (
            self.cards_df.lazy()
            .group_by("oracle_id")
            .agg(pl.col("set").str.to_uppercase().unique().sort().alias("printings"))
            .collect()
        )

        # Rulings: oracle_id -> list of {date, text}
        if self.raw_rulings_df is not None:
            self.rulings_df = (
                self.raw_rulings_df.lazy()
                .sort(["published_at", "comment"])
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

    def _load_uuid_cache(self) -> None:
        """Load legacy UUID mappings if available.

        The cache file is a JSON object: {scryfall_id: {side: mtgjson_uuid}}
        We flatten this to a DataFrame with columns [scryfall_id, side, cached_uuid]
        """
        LOGGER.info("[CACHE]Loading legacy UUID cache...")
        cache_path = constants.RESOURCE_PATH / "legacy_mtgjson_v5_uuid_mapping.json"
        if not cache_path.exists():
            LOGGER.info("No UUID cache found")
            return
        try:
            import json
            with open(cache_path, "r") as f:
                raw_cache = json.load(f)

            # Flatten nested structure to rows
            rows = []
            for scryfall_id, sides_dict in raw_cache.items():
                for side, cached_uuid in sides_dict.items():
                    rows.append({
                        "scryfall_id": scryfall_id,
                        "side": side,
                        "cached_uuid": cached_uuid,
                    })

            self.uuid_cache_df = pl.DataFrame(rows)
            LOGGER.info(f"Loaded UUID cache with {len(self.uuid_cache_df)} entries")
        except Exception as e:
            LOGGER.warning(f"Failed to load UUID cache: {e}")
            self.uuid_cache_df = None

    def get_set(self, set_code: str) -> dict[str, Any] | None:
        """Get set metadata by set code."""
        if self.sets_df is None or self.sets_df.is_empty():
            return None
        result = self.sets_df.filter(pl.col("code") == set_code.upper())
        if result.is_empty():
            return None
        return result.row(0, named=True)

    def get_foreign_data(
        self, set_code: str, collector_number: str
    ) -> list[dict[str, Any]]:
        """Get foreign language data for a specific card."""
        if self.foreign_data_df is None:
            return []

        result = self.foreign_data_df.filter(
            (pl.col("set") == set_code)
            & (pl.col("collector_number") == collector_number)
        )

        if result.is_empty():
            return []

        # Extract and force to Python list
        foreign_data = result["foreign_data"].to_list()[0]

        if foreign_data is None:
            return []

        return list(foreign_data)

    def get_meld_parts(self, result_name: str) -> list[str] | None:
        """Get meld part names for a meld result card."""
        for triplet in self.meld_triplets:
            if triplet[2] == result_name:
                return triplet[:2]
        return None

    @classmethod
    def get_instance(cls) -> "GlobalCache":
        """Get the singleton instance of GlobalCache."""
        if cls._instance is None:
            cls()
        assert cls._instance is not None
        return cls._instance
    


GLOBAL_CACHE = GlobalCache.get_instance()
