<<<<<<< HEAD
import pathlib
from typing import Optional

from mtgjson5 import constants
=======
"""
Global cache for MTGJSON provider data and pre-computed aggregations.
"""

import json
import pathlib
import time
from argparse import Namespace
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, cast, overload

import polars as pl

from mtgjson5 import constants
from mtgjson5.categoricals import DynamicCategoricals, discover_categoricals
from mtgjson5.providers import (
    BulkDataProvider,
    CardHoarderProvider,
    CardMarketProvider,
    CKProvider,
    EdhrecSaltProvider,
    GathererProvider,
    ManapoolPricesProvider,
    MtgWikiProviderSecretLair,
    MultiverseBridgeProvider,
    ScryfallProvider,
    ScryfallProviderOrientationDetector,
    SealedDataProvider,
    TCGProvider,
    WhatsInStandardProvider,
)
from mtgjson5.utils import LOGGER


def _format_size(
    df: pl.DataFrame | pl.LazyFrame | None, _show_schema: bool = False
) -> str:
    """Format DataFrame size for logging."""
    if df is None:
        return "None"
    if isinstance(df, pl.LazyFrame):
        schema = df.collect_schema()
        return f"LazyFrame ({len(schema)} cols)"
    rows = len(df)
    cols = len(df.columns)
    return f"{rows:,} rows x {cols} cols"


def _format_dict_size(d: dict | None) -> str:
    """Format dict size for logging."""
    if d is None:
        return "None"
    return f"{len(d):,} entries"


def load_resource_json(filename: str) -> dict | list:
    """Load a JSON resource file and return raw data."""
    file_path = constants.RESOURCE_PATH / filename
    if not file_path.exists():
        LOGGER.warning(f"Resource file not found: {file_path}")
        return {}
    with file_path.open("rb") as f:
        return cast(dict | list, json.loads(f.read()))


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
>>>>>>> a158cad (Introduces a singleton-based cache to manage bulk provider data,)


class GlobalCache:
    """Global shared access cache for provider data."""

    _instance: Optional["GlobalCache"] = None

<<<<<<< HEAD
    def __new__(cls) -> "GlobalCache":
=======
    def __new__(cls, args: Namespace | None = None) -> "GlobalCache":
>>>>>>> a158cad (Introduces a singleton-based cache to manage bulk provider data,)
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

<<<<<<< HEAD
    def __init__(self) -> None:
        if getattr(self, "_initialized", False):
            return
        
        self.CACHE_DIR: pathlib.Path = constants.CACHE_PATH
        self.CACHE_DIR.mkdir(parents=True, exist_ok=True)
        
        self._initialized = True
        
        @classmethod
        def get_instance(cls) -> "GlobalCache":
            if cls._instance is None:
                cls()
            return cls._instance


GLOBAL_CACHE = GlobalCache.get_instance()
=======
    def __init__(self, _args: Namespace | None = None) -> None:
        if getattr(self, "_initialized", False):
            return

        constants.CACHE_PATH.mkdir(parents=True, exist_ok=True)
        self.cache_path = constants.CACHE_PATH

        # Bulk data
        self.cards_df: pl.LazyFrame | None = None
        self.raw_rulings_df: pl.LazyFrame | None = None

        # Pre-computed aggregations
        self.oracle_lookup: pl.LazyFrame | None = None
        self.sets_df: pl.LazyFrame | None = None
        self.rulings_df: pl.DataFrame | None = None
        self.foreign_data_df: pl.DataFrame | None = None
        self.uuid_cache_df: pl.DataFrame | None = None

        # Provider Data DataFrames
        self.card_kingdom_df: pl.DataFrame | None = None
        self.card_kingdom_raw_df: pl.DataFrame | None = None
        self.mcm_lookup_df: pl.DataFrame | None = None
        self.salt_df: pl.DataFrame | None = None
        self.spellbook_df: pl.DataFrame | None = None
        self.meld_lookup_df: pl.DataFrame | None = None
        self.sld_subsets_df: pl.DataFrame | None = None
        self.orientation_df: pl.DataFrame | None = None
        self.gatherer_df: pl.DataFrame | None = None

        self.sealed_cards_df: pl.LazyFrame | None = None
        self.sealed_products_df: pl.LazyFrame | None = None
        self.sealed_contents_df: pl.LazyFrame | None = None
        self.decks_df: pl.LazyFrame | None = None
        self.boosters_df: pl.LazyFrame | None = None

        self.tcg_skus_lf: pl.LazyFrame | None = None
        self.tcg_sku_map_df: pl.DataFrame | None = None
        self.tcg_to_uuid_df: pl.DataFrame | None = None
        self.tcg_etched_to_uuid_df: pl.DataFrame | None = None
        self.mtgo_to_uuid_df: pl.DataFrame | None = None
        self.cardmarket_to_uuid_df: pl.DataFrame | None = None

        # UUID -> oracle_id mapping for joining rulings to cards
        self.uuid_to_oracle_df: pl.DataFrame | None = None

        # Final pipeline LazyFrame (pre-rename, all joins applied)
        # Set by build_cards() after all transforms, before rename_all_the_things
        self.final_cards_lf: pl.LazyFrame | None = None

        # Raw resource data
        self.duel_deck_sides: dict = {}
        self.meld_data: dict | list = {}
        self.meld_triplets: dict[str, list[str]] = {}
        self.world_championship_signatures: dict = {}
        self.manual_overrides: dict = {}
        self.gatherer_map: dict = {}
        self.multiverse_bridge_cards: dict = {}
        self.multiverse_bridge_sets: dict = {}
        self.standard_legal_sets: set[str] = set()
        self.unlimited_cards: set[str] = set()  # Cards that can have unlimited copies
        self._categoricals: DynamicCategoricals | None = None
        # Provider instances (lazy)
        self._scryfall: ScryfallProvider | None = None
        self._cardkingdom: CKProvider | None = None
        self._cardmarket: CardMarketProvider | None = None
        self._gatherer: GathererProvider | None = None
        self._multiverse: MultiverseBridgeProvider | None = None
        self._github: SealedDataProvider | None = None
        self._edhrec: EdhrecSaltProvider | None = None
        self._standard: WhatsInStandardProvider | None = None
        self._manapool: ManapoolPricesProvider | None = None
        self._cardhoarder: CardHoarderProvider | None = None
        self._tcgplayer: TCGProvider | None = None
        self._secretlair: MtgWikiProviderSecretLair | None = None
        self._orientations: ScryfallProviderOrientationDetector | None = None
        self._bulkdata: BulkDataProvider | None = None

        self._initialized = True
        self._loaded = False
        self._set_filter: list[str] | None = None
        self.scryfall_id_filter: set[str] | None = None  # For deck-only builds
        self._output_types: set[str] = set()
        self._export_formats: set[str] | None = None  # For format-specific builds

    def release(self, *attrs: str) -> None:
        """Release specific cached data to free memory.

        Args:
            *attrs: Attribute names to clear (e.g., 'cards_df', 'rulings_df')
        """
        for attr in attrs:
            if hasattr(self, attr):
                setattr(self, attr, None)

    def clear(self) -> None:
        """Clear all cached data to free memory."""
        self.release(
            "cards_df",
            "raw_rulings_df",
            "oracle_lookup",
            "sets_df",
            "rulings_df",
            "foreign_data_df",
            "uuid_cache_df",
            "card_kingdom_df",
            "card_kingdom_raw_df",
            "mcm_lookup_df",
            "salt_df",
            "spellbook_df",
            "meld_lookup_df",
            "sld_subsets_df",
            "orientation_df",
            "gatherer_df",
            "sealed_cards_df",
            "sealed_products_df",
            "sealed_contents_df",
            "decks_df",
            "boosters_df",
            "tcg_skus_lf",
            "tcg_sku_map_df",
            "tcg_to_uuid_df",
            "tcg_etched_to_uuid_df",
            "mtgo_to_uuid_df",
            "cardmarket_to_uuid_df",
            "uuid_to_oracle_df",
            "final_cards_lf",
        )
        self._loaded = False

    def load_all(
        self,
        set_codes: list[str] | None = None,
        output_types: set[str] | None = None,
        export_formats: set[str] | None = None,
    ) -> "GlobalCache":
        """
        Load all data sources and pre-compute aggregations.

        Args:
            set_codes: Optional list of set codes to filter
            output_types: Optional set of output types (e.g., {"decks"}).
                When "decks" is the only output, computes scryfall_id_filter
                to limit card processing to only deck cards.
            export_formats: Optional set of export formats (e.g., {"parquet", "csv"}).
                When specified, can be used to skip loading data not needed
                for the requested formats.
        """
        self._output_types = output_types or set()
        self._export_formats = export_formats
        self._set_filter = [s.upper() for s in set_codes] if set_codes else None

        self._download_bulk_data()
        self._load_bulk_data()
        self._load_resources()
        self._load_sets_metadata()

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(self._load_orientations): "orientations",
                executor.submit(self._load_card_kingdom): "card_kingdom",
                executor.submit(self._load_edhrec_salt): "edhrec",
                executor.submit(self._load_multiverse_bridge): "multiverse_bridge",
                executor.submit(self._load_gatherer): "gatherer",
                executor.submit(self._load_whats_in_standard): "standard",
                executor.submit(self._load_github_data): "github",
                executor.submit(self._load_secretlair_subsets): "secretlair",
                executor.submit(self._load_mcm_lookup): "mcm",
            }

            for future in as_completed(futures):
                name = futures[future]
                try:
                    future.result()
                    LOGGER.info(f"Loaded {name}")
                except Exception as e:
                    LOGGER.error(f"Failed to load {name}: {e}")

            if self.cards_df is None:
                raise RuntimeError("Bulk data not loaded")

            self.unlimited_cards = self.scryfall.cards_without_limits

            # Build scryfall_id filter for deck-only builds
            if self._output_types == {"decks"} and self.decks_df is not None:
                self._build_deck_scryfall_filter()

            self._normalize_all_columns()
            self._apply_categoricals()
            self._dump_and_reload_as_lazy()

            self._loaded = True
            return self

    def _dump_and_reload_as_lazy(self) -> None:
        """
        Dump all DataFrames to parquet and reload as LazyFrames to free memory.
        """
        import gc

        lazy_cache_path = self.cache_path / "lazy"
        lazy_cache_path.mkdir(parents=True, exist_ok=True)

        # Map of attribute -> parquet filename
        dataframes_to_dump = {
            # Core data (already LazyFrames, but ensure on disk)
            "cards_df": "cards.parquet",
            "raw_rulings_df": "raw_rulings.parquet",
            "sets_df": "sets.parquet",
            # Provider DataFrames
            "card_kingdom_df": "card_kingdom.parquet",
            "card_kingdom_raw_df": "card_kingdom_raw.parquet",
            "mcm_lookup_df": "mcm_lookup.parquet",
            "salt_df": "salt.parquet",
            "spellbook_df": "spellbook.parquet",
            "meld_lookup_df": "meld_lookup.parquet",
            "sld_subsets_df": "sld_subsets.parquet",
            "orientation_df": "orientations.parquet",
            "gatherer_df": "gatherer.parquet",
            "uuid_cache_df": "uuid_cache.parquet",
            "rulings_df": "rulings.parquet",
            "foreign_data_df": "foreign_data.parquet",
            # GitHub data
            "sealed_cards_df": "sealed_cards.parquet",
            "sealed_products_df": "sealed_products.parquet",
            "sealed_contents_df": "sealed_contents.parquet",
            "decks_df": "decks.parquet",
            "boosters_df": "boosters.parquet",
            # TCG data
            "tcg_skus_lf": "tcg_skus.parquet",
            "tcg_sku_map_df": "tcg_sku_map.parquet",
            "tcg_to_uuid_df": "tcg_to_uuid.parquet",
            "tcg_etched_to_uuid_df": "tcg_etched_to_uuid.parquet",
            "mtgo_to_uuid_df": "mtgo_to_uuid.parquet",
            "cardmarket_to_uuid_df": "cardmarket_to_uuid.parquet",
            "uuid_to_oracle_df": "uuid_to_oracle.parquet",
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
        if self._categoricals is None and self.cards_df is not None:
            self._categoricals = discover_categoricals(self.cards_df, self.sets_df)

    def _build_deck_scryfall_filter(self) -> None:
        """
        Build scryfall_id filter from deck UUIDs for deck-only builds.

        Extracts all UUIDs from deck card lists (mainBoard, sideBoard, commander, tokens),
        then uses uuid_cache to reverse-lookup the corresponding scryfall_ids.
        """
        if self.decks_df is None:
            return

        # Collect deck data (small - just metadata, not full cards)
        decks = (
            self.decks_df.collect()
            if isinstance(self.decks_df, pl.LazyFrame)
            else self.decks_df
        )

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
        uuid_cache = self.uuid_cache_df
        if uuid_cache is not None:
            if isinstance(uuid_cache, pl.LazyFrame):
                uuid_cache = uuid_cache.collect()
            if len(uuid_cache) > 0:
                scryfall_ids = (
                    uuid_cache.filter(pl.col("cachedUuid").is_in(deck_uuids))
                    .select("scryfallId")
                    .unique()
                    .to_series()
                    .to_list()
                )
                self.scryfall_id_filter = set(scryfall_ids)
                LOGGER.info(
                    f"Built scryfall_id filter: {len(self.scryfall_id_filter):,} IDs "
                    f"for {len(deck_uuids):,} deck UUIDs"
                )
                return
        LOGGER.warning("uuid_cache not available, cannot build scryfall_id filter")

    def _download_bulk_data(self, force_refresh: bool = False) -> None:
        """Download Scryfall bulk data if missing or stale."""
        cards_path = self.cache_path / "all_cards.ndjson"
        rulings_path = self.cache_path / "rulings.ndjson"

        needs_download = force_refresh
        if not cards_path.exists() or cards_path.stat().st_size == 0:
            needs_download = True
        if not rulings_path.exists() or rulings_path.stat().st_size == 0:
            needs_download = True

        # Check age (24h max)
        if not needs_download and cards_path.exists():
            age_hours = (time.time() - cards_path.stat().st_mtime) / 3600
            if age_hours > 72:
                LOGGER.info(f"Bulk data is {age_hours:.1f}h old, refreshing...")
                needs_download = True

        if needs_download:
            LOGGER.info("Downloading bulk scryfall data...")
            self.bulkdata.download_bulk_files_sync(
                self.cache_path, ["all_cards", "rulings"], force_refresh
            )
        else:
            LOGGER.info("Using cached bulk data")

    def _load_bulk_data(self) -> None:
        """Load bulk NDJSON files into LazyFrames."""
        LOGGER.info("Loading LazyFrames...")

        cards_path = self.cache_path / "all_cards.ndjson"
        rulings_path = self.cache_path / "rulings.ndjson"

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

        # Scan without schema overrides first
        # Keep infer_schema_length high to capture optional fields that may not appear early
        self.cards_df = pl.scan_ndjson(
            cards_path,
            infer_schema_length=10000,
        )

        schema = self.cards_df.collect_schema()

        # Build cast expressions
        cast_exprs = []
        for col_name in string_cast_columns:
            if col_name in schema:
                cast_exprs.append(pl.col(col_name).cast(pl.Utf8))

        if cast_exprs:
            self.cards_df = self.cards_df.with_columns(cast_exprs)

        # Ensure optional columns exist (fields that may not appear in first N rows)
        optional_columns = {
            "defense": pl.Utf8,
            "flavor_name": pl.Utf8,
        }
        missing_cols = []
        for col_name, dtype in optional_columns.items():
            if col_name not in schema:
                missing_cols.append(pl.lit(None).cast(dtype).alias(col_name))

        if missing_cols:
            self.cards_df = self.cards_df.with_columns(missing_cols)

        self.raw_rulings_df = pl.scan_ndjson(rulings_path, infer_schema_length=1000)

       
    def _load_resources(self) -> None:
        """Load local JSON resource files."""
        LOGGER.info("Loading resource files...")

        self.duel_deck_sides = cast(dict, load_resource_json("duel_deck_sides.json"))
        self.meld_data = load_resource_json("meld_triplets.json")
        self.world_championship_signatures = cast(
            dict, load_resource_json("world_championship_signatures.json")
        )
        self.manual_overrides = cast(dict, load_resource_json("manual_overrides.json"))
        uuid_raw = cast(dict, load_resource_json("legacy_mtgjson_v5_uuid_mapping.json"))
        if uuid_raw:
            rows = [
                {"scryfallId": sid, "side": side, "cachedUuid": uuid}
                for sid, sides in uuid_raw.items()
                for side, uuid in sides.items()
            ]
            self.uuid_cache_df = pl.DataFrame(rows)
       
        meld_triplets_expanded: dict[str, list[str]] = {}
        if isinstance(self.meld_data, list):
            for triplet in self.meld_data:
                if len(triplet) == 3:
                    for name in triplet:
                        meld_triplets_expanded[name] = triplet
       
        self.meld_triplets = meld_triplets_expanded
        LOGGER.info("Loaded resource files")
       
    def _load_sets_metadata(self) -> None:
        """Load set metadata from Scryfall."""
        cache_path = self.cache_path / "sets.parquet"

        if _cache_fresh(cache_path):
            self.sets_df = pl.read_parquet(cache_path).lazy()
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
        self.sets_df = sets_df.lazy()

    def _load_card_kingdom(self) -> None:
        """Load Card Kingdom data with caching.

        Stores both:
        - Pivoted ID/URL data for card identifiers (ck_pivoted.parquet)
        - Raw pricing data for price generation (ck_raw.parquet)
        """
        pivoted_cache = self.cache_path / "ck_pivoted.parquet"
        raw_cache = self.cache_path / "ck_raw.parquet"

        # Check if both caches exist and are fresh
        if _cache_fresh(pivoted_cache) and _cache_fresh(raw_cache):
            self.card_kingdom_df = pl.read_parquet(pivoted_cache)
            self.card_kingdom_raw_df = pl.read_parquet(raw_cache)
            return

        # Initialize empty DataFrames
        self.card_kingdom_df = pl.DataFrame(
            {
                "id": [],
                "cardKingdomId": [],
                "cardKingdomFoilId": [],
                "cardKingdomUrl": [],
                "cardKingdomFoilUrl": [],
            }
        ).cast(
            {
                "id": pl.String,
                "cardKingdomId": pl.String,
                "cardKingdomFoilId": pl.String,
                "cardKingdomUrl": pl.String,
                "cardKingdomFoilUrl": pl.String,
            }
        )
        self.card_kingdom_raw_df = pl.DataFrame()

        # Fetch fresh data
        try:
            self.card_kingdom.fetch_sync()

            # Store pivoted data (for identifiers)
            ck_data = self.card_kingdom.get_join_data()
            if ck_data is not None and len(ck_data) > 0:
                self.card_kingdom_df = ck_data
                self.card_kingdom_df.write_parquet(pivoted_cache)

            # Store raw data (for pricing)
            # pylint: disable=protected-access
            if (
                self.card_kingdom._raw_df is not None
                and len(self.card_kingdom._raw_df) > 0
            ):
                self.card_kingdom_raw_df = self.card_kingdom._raw_df
                self.card_kingdom_raw_df.write_parquet(raw_cache)

        except Exception as e:
            LOGGER.warning(f"Failed to fetch Card Kingdom data: {e}")

    def _load_edhrec_salt(self) -> None:
        """Load EDHREC saltiness data."""
        cache_path = self.cache_path / "edhrec_salt.parquet"

        if _cache_fresh(cache_path):
            self.salt_df = pl.read_parquet(cache_path)
            return

        self.salt_df = self.edhrec.get_data_frame()
        if self.salt_df is not None and len(self.salt_df) > 0:
            self.salt_df.write_parquet(cache_path)
           
    def _load_multiverse_bridge(self) -> None:
        """Load MultiverseBridge Rosetta Stone data."""
        cards_cache = self.cache_path / "multiverse_bridge_cards.json"
        sets_cache = self.cache_path / "multiverse_bridge_sets.json"

        if _cache_fresh(cards_cache) and _cache_fresh(sets_cache):
            with cards_cache.open("rb") as f:
                self.multiverse_bridge_cards = json.loads(f.read())
            with sets_cache.open("rb") as f:
                self.multiverse_bridge_sets = json.loads(f.read())
            return

        self.multiverse_bridge_cards = self.multiverse.get_rosetta_stone_cards()
        self.multiverse_bridge_sets = self.multiverse.get_rosetta_stone_sets()

        with cards_cache.open("w", encoding="utf-8") as f:
            json.dump(self.multiverse_bridge_cards, f)
        with sets_cache.open("w", encoding="utf-8") as f:
            json.dump(self.multiverse_bridge_sets, f)


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
                self.gatherer_df = pl.DataFrame(rows)
                LOGGER.info(
                    f"  [gatherer] Built gatherer_df: {_format_size(self.gatherer_df)}"
                )

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
        """Load GitHub sealed/deck/booster data."""
        card_to_products_cache = self.cache_path / "github_card_to_products.parquet"
        sealed_products_cache = self.cache_path / "github_sealed_products.parquet"
        sealed_contents_cache = self.cache_path / "github_sealed_contents.parquet"
        decks_cache = self.cache_path / "github_decks.parquet"
        booster_cache = self.cache_path / "github_booster.parquet"

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
            # Load directly into GlobalCache properties
            self.sealed_cards_df = pl.read_parquet(card_to_products_cache).lazy()
            self.sealed_products_df = pl.read_parquet(sealed_products_cache).lazy()
            self.sealed_contents_df = pl.read_parquet(sealed_contents_cache).lazy()
            self.decks_df = pl.read_parquet(decks_cache).lazy()
            self.boosters_df = pl.read_parquet(booster_cache).lazy()
            return

        # Start async load in background with callback
        def on_github_complete(provider: SealedDataProvider) -> None:
            """Called when GitHub data finishes loading."""
            LOGGER.info("Sealedd loaded - transferring to GlobalCache...")

            # Save to cache
            if provider.card_to_products_df is not None:
                provider.card_to_products_df.collect().write_parquet(
                    card_to_products_cache
                )
                self.sealed_cards_df = provider.card_to_products_df

            if provider.sealed_products_df is not None:
                provider.sealed_products_df.collect().write_parquet(
                    sealed_products_cache
                )
                self.sealed_products_df = provider.sealed_products_df

            if provider.sealed_contents_df is not None:
                provider.sealed_contents_df.collect().write_parquet(
                    sealed_contents_cache
                )
                self.sealed_contents_df = provider.sealed_contents_df

            if provider.decks_df is not None:
                provider.decks_df.collect().write_parquet(decks_cache)
                self.decks_df = provider.decks_df
                
            if provider.boosters_df is not None:
                provider.boosters_df.collect().write_parquet(booster_cache)
                self.boosters_df = provider.boosters_df
                
        self.github.load_async_background(on_complete=on_github_complete)

    def _load_orientations(self) -> None:
        """Load orientation data for Art Series cards from Scryfall."""
        cache_path = self.cache_path / "orientations.parquet"
        if _cache_fresh(cache_path):
            self.orientation_df = pl.read_parquet(cache_path)
            return
        
        detector = ScryfallProviderOrientationDetector()
        sets_df_raw = self.sets_df
        if sets_df_raw is None:
            LOGGER.warning("Sets not loaded, skipping orientations")
            return
        sets_df: pl.DataFrame = (
            sets_df_raw.collect()
            if isinstance(sets_df_raw, pl.LazyFrame)
            else sets_df_raw
        )
        art_series_sets = sets_df.filter(pl.col("name").str.contains("Art Series"))[
            "code"
        ].to_list()

        rows = []
        for set_code in art_series_sets:
            orientation_map = detector.get_uuid_to_orientation_map(set_code)
            for scryfall_id, orientation in (orientation_map or {}).items():
                rows.append({"scryfallId": scryfall_id, "orientation": orientation})

        self.orientation_df = pl.DataFrame(rows) if rows else pl.DataFrame()
        if len(self.orientation_df) > 0:
            self.orientation_df.write_parquet(cache_path)
        
    def _load_secretlair_subsets(self) -> None:
        """Load Secret Lair subset mappings."""
        cache_path = self.cache_path / "sld_subsets.parquet"

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
        cache_path = self.cache_path / "mcm_lookup.parquet"

        if _cache_fresh(cache_path):
            self.mcm_lookup_df = pl.read_parquet(cache_path)
            return

        sets_df_raw = self.sets_df
        if sets_df_raw is None:
            LOGGER.warning("Sets not loaded, skipping MCM lookup table")
            return
        sets_df: pl.DataFrame = (
            sets_df_raw.collect()
            if isinstance(sets_df_raw, pl.LazyFrame)
            else sets_df_raw
        )
        if sets_df.height == 0:
            LOGGER.warning("Sets not loaded, skipping MCM lookup table")
            return

        try:
            self.mcm_lookup_df = pl.DataFrame(
                {
                    "mcmId": [],
                    "mcmMetaId": [],
                    "setCode": [],
                    "nameLower": [],
                    "number": [],
                }
            ).cast(
                {
                    "mcmId": pl.String,
                    "mcmMetaId": pl.String,
                    "setCode": pl.String,
                    "nameLower": pl.String,
                    "number": pl.String,
                }
            )
        except Exception as e:
            LOGGER.error(f"Failed to build MCM lookup: {e}")
            self.mcm_lookup_df = pl.DataFrame()

    def _normalize_all_columns(self) -> None:
        """Normalize ALL DataFrame columns to camelCase"""
        self.cards_df = _normalize_columns(self.cards_df)
        self.raw_rulings_df = _normalize_columns(self.raw_rulings_df)
        self.sets_df = _normalize_columns(self.sets_df)
        self.rulings_df = _normalize_columns(self.rulings_df)
        self.foreign_data_df = _normalize_columns(self.foreign_data_df)
        self.uuid_cache_df = _normalize_columns(self.uuid_cache_df)
        self.card_kingdom_df = _normalize_columns(self.card_kingdom_df)
        self.mcm_lookup_df = _normalize_columns(self.mcm_lookup_df)
        self.salt_df = _normalize_columns(self.salt_df)
        self.spellbook_df = _normalize_columns(self.spellbook_df)
        self.meld_lookup_df = _normalize_columns(self.meld_lookup_df)
        self.sld_subsets_df = _normalize_columns(self.sld_subsets_df)
        self.orientation_df = _normalize_columns(self.orientation_df)
        self.gatherer_df = _normalize_columns(self.gatherer_df)
        if self._github is not None:
            self._github.card_to_products_df = _normalize_columns(
                self._github.card_to_products_df
            )
            self._github.sealed_products_df = _normalize_columns(
                self._github.sealed_products_df
            )
            self._github.sealed_contents_df = _normalize_columns(
                self._github.sealed_contents_df
            )
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
    def multiverse(self) -> MultiverseBridgeProvider:
        """Get or create the MultiverseBridge provider instance."""
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
    def manapool(self) -> ManapoolPricesProvider:
        """Get or create the Manapool prices provider instance."""
        if self._manapool is None:
            self._manapool = ManapoolPricesProvider()
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
    def secretlair(self) -> MtgWikiProviderSecretLair:
        """Get or create the Secret Lair provider instance."""
        if self._secretlair is None:
            self._secretlair = MtgWikiProviderSecretLair()
        return self._secretlair

    @property
    def bulkdata(self) -> BulkDataProvider:
        """Get or create the bulk data provider instance."""
        if self._bulkdata is None:
            self._bulkdata = BulkDataProvider()
        return self._bulkdata

    @property
    def categoricals(self) -> DynamicCategoricals | None:
        """Get the DynamicCategoricals instance (set during load_all)."""
        return self._categoricals

    def get_tcg_to_uuid_map(self) -> dict[str, str]:
        """
        Get mapping from TCGPlayer product ID to MTGJSON UUID.

        Returns empty dict if mapping not loaded.
        """
        if self.tcg_to_uuid_df is None or self.tcg_to_uuid_df.is_empty():
            return {}
        return dict(
            zip(
                self.tcg_to_uuid_df["tcgplayerProductId"].to_list(),
                self.tcg_to_uuid_df["uuid"].to_list(),
            )
        )

    def get_tcg_etched_to_uuid_map(self) -> dict[str, str]:
        """
        Get mapping from TCGPlayer etched product ID to MTGJSON UUID.

        Returns empty dict if mapping not loaded.
        """
        if self.tcg_etched_to_uuid_df is None or self.tcg_etched_to_uuid_df.is_empty():
            return {}
        return dict(
            zip(
                self.tcg_etched_to_uuid_df["tcgplayerEtchedProductId"].to_list(),
                self.tcg_etched_to_uuid_df["uuid"].to_list(),
            )
        )

    def get_mtgo_to_uuid_map(self) -> dict[str, str]:
        """
        Get mapping from MTGO ID to MTGJSON UUID.

        Returns empty dict if mapping not loaded.
        """
        if self.mtgo_to_uuid_df is None or self.mtgo_to_uuid_df.is_empty():
            return {}
        return dict(
            zip(
                self.mtgo_to_uuid_df["mtgoId"].to_list(),
                self.mtgo_to_uuid_df["uuid"].to_list(),
            )
        )

    def get_cardmarket_to_uuid_map(self) -> dict[str, str]:
        """
        Get mapping from CardMarket (MCM) ID to MTGJSON UUID.

        Returns empty dict if mapping not loaded.
        """
        if self.cardmarket_to_uuid_df is None or self.cardmarket_to_uuid_df.is_empty():
            return {}
        return dict(
            zip(
                self.cardmarket_to_uuid_df["mcmId"].to_list(),
                self.cardmarket_to_uuid_df["uuid"].to_list(),
            )
        )

    @classmethod
    def get_instance(cls) -> "GlobalCache":
        """Get the singleton GlobalCache instance."""
        if cls._instance is None:
            cls()
        assert cls._instance is not None
        return cls._instance


GLOBAL_CACHE = GlobalCache.get_instance()
>>>>>>> a158cad (Introduces a singleton-based cache to manage bulk provider data,)
