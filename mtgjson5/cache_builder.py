"""
Global cache for MTGJSON provider data and pre-computed aggregations.
"""

import json
import ijson
import logging
import pathlib
import requests
from concurrent.futures import ThreadPoolExecutor  # pylint: disable=no-name-in-module
from functools import cache
from typing import Any, Optional, TypeVar
import polars as pl

from mtgjson5 import constants
from mtgjson5.constants import LANGUAGE_MAP
from mtgjson5.providers import (
    CardKingdomProvider,
    CardMarketProvider,
    ScryfallProvider,
)

LOGGER = logging.getLogger(__name__)


T = TypeVar("T")


def fetch_bulk_data(url: str, destination: str) -> None:
    """
    Fetch bulk data from Scryfall and save to destination file.
    :param url: URL to fetch from
    :param destination: File path to save data
    """
    LOGGER.info(f"Fetching bulk data from {url}...")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    dest_path = pathlib.Path(destination)
    with dest_path.open('wb') as f:
        # strip array brackets from stream
        for item in ijson.items(response.raw, 'item'):
            f.write(json.dumps(item) + b'\n')
    
    LOGGER.info(f"Saved bulk data to {destination}")


def get_scryfall_lazyframe(ndjson_path: str) -> pl.LazyFrame:
    """
    Load Scryfall NDJSON file into a Polars LazyFrame with schema overrides.
    :param ndjson_path: Path to NDJSON file
    :return: Polars LazyFrame with Scryfall data
    """
    # lock polymorphic fields to Utf8 to prevent type errors.
    schema_overrides = {
        "power": pl.Utf8, # to handle "*", "½", etc.
        "toughness": pl.Utf8, 
        "loyalty": pl.Utf8,
        "defense": pl.Utf8,
        "hand_modifier": pl.Utf8,
        "life_modifier": pl.Utf8,
        "collector_number": pl.Utf8, # "123a"
        "mtgo_id": pl.Utf8,
        "arena_id": pl.Utf8,
        "tcgplayer_id": pl.Utf8,
        "cardmarket_id": pl.Utf8,
        "face_flavor_name": pl.Utf8, # Sparse field
    }
    
    # infer_schema_length=None:
    # Causes polars to create a "Union Schema" of every column in the file
    # and then process it in one pass via rayon's parallel iterators.
    # this solves the scryfall's Semi-Variable Schema problem lickedy-split.
    return pl.scan_ndjson(
        ndjson_path,
        infer_schema_length=None, 
        schema_overrides=schema_overrides,
        batch_size=4096  # Larger batches = better SIMD vectorization
    )
    
    # Usage in pipeline
    # lf = get_scryfall_lazyframe("oracle-cards-latest.json")
    
    # Optimization Pushdown Example:
    # Because we used scan_ndjson, this filter is pushed to the READER.
    # The parser will skip decoding the JSON bodies of cards not in "LEA".
    # standard_cards = lf.filter(pl.col("set") == "lea").collect()


def extract_foreign_data(cards: pl.LazyFrame) -> pl.LazyFrame:
    """
    Extract foreign language data from non-English cards.
    Returns grouped foreign_data ready to join to English cards.
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
                .replace_strict(LANGUAGE_MAP, default=pl.col("lang"))
                .alias("language"),
                pl.col("multiverse_ids").list.first().alias("multiverse_id"),
            ]
        )
    )
    foreign_struct = (
        foreign.select(
            [
                "set",
                "collector_number",
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
        .group_by(["set", "collector_number"])
        .agg(pl.col("foreign_entry").alias("foreign_data"))
    )
    return foreign_struct


def resolve_foreign_entry(
    entry: dict[str, Any],
    card_name: str,
) -> dict[str, Any]:
    """
    Resolve a foreign data entry for a specific card face.

    Matches original parse_foreign() logic:
    - For single-faced cards: return as-is
    - For multi-faced cards:
      - Set name to joined printed names
      - Set face_name based on which face matches card_name

    :param entry: Raw foreign entry from cache
    :param card_name: The face_name or name of the card being built
    :return: Resolved foreign entry with correct name/face_name
    """
    card_faces = entry.get("card_faces")

    # Single-faced card - no special handling needed
    if not card_faces or len(card_faces) == 0:
        # Remove card_faces from output, add empty face_name
        result = {
            k: v for k, v in entry.items() if k not in ("card_faces", "scryfall_name")
        }
        result["face_name"] = None
        return result

    # Multi-faced card - need to determine which face
    scryfall_name = entry.get("scryfall_name", "")

    # Determine face index by matching card_name to first part of scryfall name
    # (card_name.lower() == foreign_card["name"].split("/")[0].strip().lower())
    first_face_name = scryfall_name.split("/")[0].strip().lower()
    face_idx = 0 if card_name.lower() == first_face_name else 1

    # Clamp to available faces
    face_idx = min(face_idx, len(card_faces) - 1)

    # Build joined name from all faces' printed names
    joined_name = " // ".join(
        face.get("printed_name") or face.get("name", "") for face in card_faces
    )

    # Get face_name from the specific face
    target_face = card_faces[face_idx]
    face_name = target_face.get("printed_name") or target_face.get("name")

    # Build resolved entry
    result = {
        "language": entry.get("language"),
        "scryfall_id": entry.get("scryfall_id"),
        "multiverse_id": entry.get("multiverse_id"),
        "name": joined_name,
        "face_name": face_name,
        "text": target_face.get("printed_text") or entry.get("text"),
        "type": target_face.get("printed_type_line") or entry.get("type"),
        "flavor_text": target_face.get("flavor_text") or entry.get("flavor_text"),
    }

    return result


@cache
def load_json(filename: str) -> Any:
    """Load a JSON resource from the resources directory."""
    json.loads((constants.RESOURCE_PATH / filename).read_text())


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

        # Bulk data files
        self.cards_df: pl.DataFrame | None = None
        self.rulings_df: pl.DataFrame | None = None

        # Pre-computed aggregations
        self.printings_map: pl.DataFrame | None = None
        self.rulings_map: pl.DataFrame | None = None
        self.foreign_data_map: pl.DataFrame | None = None
        self.card_kingdom_df: pl.DataFrame | None = None
        self.multiverse_bridge_df: pl.DataFrame | None = None
        self.uuid_cache_df: pl.DataFrame | None = None
        self.cardhoarder_map: dict[str, dict[str, float | None]] = {}

        # Metadata
        self.sets_map: dict[str, dict[str, Any]] = {}

        # Resource overrides
        self.manual_overrides: dict[str, dict[str, Any]] = {}
        self.keyrune_overrides: dict[str, str] = {}
        self.tcgplayer_id_overrides: dict[str, int] = {}
        self.meld_triplets: list[list[str]] = []
        self.wc_signatures: dict[str, dict[str, str]] = {}
        self.mkm_translations: dict[str, dict[str, str]] = {}
        self.base_set_sizes: dict[str, int] = {}

        # Providers (lazy loaded)
        self._scryfall: ScryfallProvider | None = None
        self._card_kingdom: CardKingdomProvider | None = None
        self._cardmarket: CardMarketProvider | None = None

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
    def card_kingdom(self) -> CardKingdomProvider:
        """Get or create the Card Kingdom provider instance."""
        if self._card_kingdom is None:
            self._card_kingdom = CardKingdomProvider()
        return self._card_kingdom

    @property
    def cardmarket(self) -> CardMarketProvider:
        """Get or create the Cardmarket provider instance."""
        if self._cardmarket is None:
            self._cardmarket = CardMarketProvider()
        return self._cardmarket

    def load_all(self, force_refresh: bool = False) -> "GlobalCache":
        """Load all provider data and pre-compute aggregations."""
        LOGGER.info("Loading global cache data...")

        self._load_sets_metadata()

        with ThreadPoolExecutor(max_workers=6) as executor:
            bulk_cards_future = executor.submit(
                self.scryfall.get_bulk_cards, force_refresh
            )
            bulk_rulings_future = executor.submit(
                self.scryfall.get_bulk_rulings, force_refresh
            )
            ck_future = executor.submit(
                self.card_kingdom.get_scryfall_translation_table
            )
            mb_cards_future = executor.submit(self._load_multiverse_bridge_cards)
            mb_sets_future = executor.submit(self._load_multiverse_bridge_sets)
            gatherer_future = executor.submit(self._load_gatherer_data)

            # might as well load overrides while waiting
            self._load_overrides()

            # Collect results
            self.cards_df = bulk_cards_future.result()
            self.rulings_df = bulk_rulings_future.result()
            self.card_kingdom_map = ck_future.result()
            self.multiverse_bridge_cards = mb_cards_future.result()
            self.multiverse_bridge_sets = mb_sets_future.result()
            self.gatherer_map = gatherer_future.result()

        self._compute_aggregations()
        self._load_uuid_cache()

        LOGGER.info("Global cache data loaded")
        return self

    def _load_sets_metadata(self) -> None:
        """Load set metadata from Scryfall."""
        LOGGER.info("Loading sets metadata...")
        sets_response = self.scryfall.download(self.scryfall.ALL_SETS_URL)
        if sets_response.get("object") == "error":
            LOGGER.error(f"Failed to load sets: {sets_response}")
            self.sets_map = {}
            return

        sets_data = sets_response.get("data", [])
        self.sets_map = {s["code"].upper(): s for s in sets_data}
        LOGGER.info(f"Loaded {len(self.sets_map)} sets into cache")

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
        LOGGER.info("Loading Multiverse Bridge Rosetta Stone cards...")
        from mtgjson5.providers import MultiverseBridgeProvider

        return MultiverseBridgeProvider().get_rosetta_stone_cards()

    def _load_multiverse_bridge_sets(self) -> dict[str, int]:
        """Load Multiverse Bridge Rosetta Stone sets."""
        LOGGER.info("Loading Multiverse Bridge Rosetta Stone sets...")
        from mtgjson5.providers import MultiverseBridgeProvider

        return MultiverseBridgeProvider().get_rosetta_stone_sets()

    def _load_gatherer_data(self) -> dict[str, list[dict[str, str]]]:
        """Load Gatherer data for cards."""
        LOGGER.info("Loading Gatherer data...")
        from mtgjson5.providers import GathererProvider

        provider = GathererProvider()
        return getattr(provider, "_cache", {})

    def _compute_aggregations(self) -> None:
        """Build pre-computed maps from bulk data."""
        if self.cards_df is None:
            raise RuntimeError("Must load bulk data first")

        LOGGER.info("Computing pre-aggregated maps...")

        # Printings: oracle_id -> list of set codes
        self.printings_map = (
            self.cards_df.lazy()
            .filter(pl.col("lang") == "en")
            .group_by("oracle_id")
            .agg(pl.col("set").str.to_uppercase().unique().sort().alias("printings"))
            .collect()
        )

        # Rulings: oracle_id -> list of {date, text}
        if self.rulings_df is not None:
            self.rulings_map = (
                self.rulings_df.lazy()
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

        # Foreign data: (set, collector_number) -> list of foreign entries
        self.foreign_data_map = extract_foreign_data(self.cards_df.lazy()).collect()

    def _load_uuid_cache(self) -> None:
        """Load legacy UUID mappings if available."""
        cache_path = constants.RESOURCE_PATH / "legacy_mtgjson_v5_uuid_mapping.json"
        if not cache_path.exists():
            LOGGER.info("No UUID cache found")
            return
        try:
            self.uuid_cache_df = pl.read_json(cache_path)
            LOGGER.info(f"Loaded UUID cache with {len(self.uuid_cache_df)} entries")
        except Exception as e:
            LOGGER.warning(f"Failed to load UUID cache: {e}")
            self.uuid_cache_df = None

    def get_set(self, set_code: str) -> dict[str, Any] | None:
        """Get set metadata by set code."""
        return self.sets_map.get(set_code.upper())

    def get_foreign_data(
        self, set_code: str, collector_number: str
    ) -> list[dict[str, Any]]:
        """Get foreign language data for a specific card."""
        if self.foreign_data_map is None:
            return []

        # Note: Scryfall stores set codes in lowercase
        result = self.foreign_data_map.filter(
            (pl.col("set") == set_code.lower())
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
