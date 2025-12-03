"""
Unified GitHub data provider for MTGJSON.

Combines sealed products, boosters, decks, and card-to-product mappings
from the mtgjson/mtg-sealed-content repository.
"""

import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional

import polars as pl
import requests
from singleton_decorator import singleton

from mtgjson5.mtgjson_config import MtgjsonConfig
from mtgjson5.utils import recursive_sort

LOGGER = logging.getLogger(__name__)


@singleton
class GitHubDataProvider:
    """
    Unified provider for all GitHub-hosted MTGJSON supplemental data.
    
    Combines:
    - Card to sealed product mappings (card_map.json)
    - Sealed product definitions (products.json)
    - Sealed product contents (contents.json)
    - Preconstructed deck data (decks_v2.json, deck_map.json)
    - Booster configuration (experimental_export_for_mtgjson.json)
    
    Usage:
        provider = GitHubDataProvider()
        provider.load()  # Fetch all data
        
        # Access raw data
        booster_config = provider.get_booster_data("NEO")
        source_products = provider.get_source_products("card-uuid")
        
        # Access as DataFrames for Polars joins
        df = cards_df.join(provider.card_to_products_df, on="uuid", how="left")
    """
    
    GITHUB_URLS = {
        "card_map": "https://github.com/mtgjson/mtg-sealed-content/raw/main/outputs/card_map.json?raw=True",
        "sealed_products": "https://github.com/mtgjson/mtg-sealed-content/blob/main/outputs/products.json?raw=true",
        "sealed_contents": "https://github.com/mtgjson/mtg-sealed-content/blob/main/outputs/contents.json?raw=true",
        "deck_map": "https://github.com/mtgjson/mtg-sealed-content/blob/main/outputs/deck_map.json?raw=True",
        "decks": "https://github.com/taw/magic-preconstructed-decks-data/blob/master/decks_v2.json?raw=true",
        "boosters": "https://github.com/taw/magic-sealed-data/blob/master/experimental_export_for_mtgjson.json?raw=true",
    }
    def __init__(
        self,
        api_token: Optional[str] = None,
        timeout: float = 60.0,
    ) -> None:
        """
        Initialize the provider.
        
        Args:
            api_token: GitHub API token. If None, reads from MtgjsonConfig.
            timeout: HTTP request timeout in seconds.
        """
        self._api_token = api_token or self._get_config_token()
        self._timeout = timeout
        self._session: Optional[requests.Session] = None
        
        # Raw data (lazily loaded)
        self._card_to_products: Optional[Dict[str, Dict[str, List[str]]]] = None
        self._sealed_products: Optional[Dict[str, Any]] = None
        self._sealed_contents: Optional[Dict[str, Any]] = None
        self._deck_map: Optional[Dict[str, Any]] = None
        self._decks_raw: Optional[List[Dict[str, Any]]] = None
        self._booster_data: Optional[Dict[str, Any]] = None
        
        # Processed data
        self._decks_by_set: Optional[Dict[str, List[Dict[str, Any]]]] = None
        
        # DataFrames (lazily built)
        self._card_to_products_df: Optional[pl.DataFrame] = None
        self._sealed_products_df: Optional[pl.DataFrame] = None
        self._sealed_contents_df: Optional[pl.DataFrame] = None
        self._decks_df: Optional[pl.DataFrame] = None
        self._booster_df: Optional[pl.DataFrame] = None
    
    @staticmethod
    def _get_config_token() -> Optional[str]:
        """Get GitHub token from MTGJSON config."""
        try:
            return MtgjsonConfig().get("GitHub", "api_token")
        except Exception:
            LOGGER.warning("No GitHub API token found in config")
            return None
    
    @property
    def _http_session(self) -> requests.Session:
        if self._session is None:
            self._session = requests.Session()
            if self._api_token:
                self._session.headers["Authorization"] = f"Bearer {self._api_token}"
        return self._session
    
    def _download(self, url: str) -> Any:
        """
        Download JSON from GitHub.
        
        Args:
            url: GitHub raw content URL
            
        Returns:
            Parsed JSON data, or empty dict/list on error
        """
        try:
            response = self._http_session.get(url, timeout=self._timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            LOGGER.error(f"Error downloading {url}: {e}")
            return {}
    
    def close(self) -> None:
        if self._session is not None:
            self._session.close()
            self._session = None
     
    def load(self, components: Optional[List[str]] = None) -> "GitHubDataProvider":
        """
        Load data from GitHub.
        
        Args:
            components: List of components to load. If None, loads all.
                Options: "card_map", "sealed", "decks", "boosters"
        
        Returns:
            Self for chaining.
        """
        if components is None:
            components = ["card_map", "sealed", "decks", "boosters"]
        
        if "card_map" in components:
            self._load_card_map()
        if "sealed" in components:
            self._load_sealed()
        if "decks" in components:
            self._load_decks()
        if "boosters" in components:
            self._load_boosters()
        
        return self
    
    def _load_card_map(self) -> None:
        """Load card UUID to sealed products mapping."""
        LOGGER.info("Loading card-to-products map...")
        self._card_to_products = self._download(self.GITHUB_URLS["card_map"])
        self._card_to_products_df = None
    
    def _load_sealed(self) -> None:
        """Load sealed products and contents."""
        LOGGER.info("Loading sealed products...")
        self._sealed_products = self._download(self.GITHUB_URLS["sealed_products"])
        self._sealed_contents = self._download(self.GITHUB_URLS["sealed_contents"])
        self._sealed_products_df = None
        self._sealed_contents_df = None
    
    def _load_decks(self) -> None:
        """Load deck data."""
        LOGGER.info("Loading decks...")
        self._deck_map = self._download(self.GITHUB_URLS["deck_map"])
        self._decks_raw = self._download(self.GITHUB_URLS["decks"])
        self._decks_by_set = None
        self._decks_df = None
    
    def _load_boosters(self) -> None:
        """Load booster configuration."""
        LOGGER.info("Loading booster data...")
        self._booster_data = self._download(self.GITHUB_URLS["boosters"])
        self._booster_df = None
        
    @property
    def card_to_products(self) -> Dict[str, Dict[str, List[str]]]:
        """
        Card UUID to sealed products mapping.
        
        Format: {card_uuid: {"foil": [product_uuids], "nonfoil": [product_uuids], ...}}
        """
        if self._card_to_products is None:
            self._load_card_map()
        return self._card_to_products
    
    @property
    def sealed_products(self) -> Dict[str, Any]:
        """
        Sealed products by set code.
        
        Format: {set_code: {product_name: {product_data}}}
        """
        if self._sealed_products is None:
            self._load_sealed()
        return self._sealed_products
    
    @property
    def sealed_contents(self) -> Dict[str, Any]:
        """
        Sealed product contents by set code.
        
        Format: {set_code: {product_name: {contents_data}}}
        """
        if self._sealed_contents is None:
            self._load_sealed()
        return self._sealed_contents
    
    @property
    def booster_data(self) -> Dict[str, Any]:
        """
        Booster configuration by set code.
        
        Format: {SET_CODE: {booster_config}}
        """
        if self._booster_data is None:
            self._load_boosters()
        return self._booster_data
    
    @property
    def decks_by_set(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Decks grouped by set code.
        
        Format: {SET_CODE: [deck_dicts]}
        """
        if self._decks_by_set is None:
            self._build_decks_by_set()
        return self._decks_by_set
        
    @property
    def card_to_products_df(self) -> pl.DataFrame:
        """
        Card to products mapping as DataFrame.
        
        Columns:
        - uuid: Card UUID (join key)
        - foil: List[str] of product UUIDs where card appears as foil
        - nonfoil: List[str] of product UUIDs where card appears as nonfoil
        - etched: List[str] of product UUIDs where card appears as etched
        """
        if self._card_to_products_df is None:
            self._card_to_products_df = self._build_card_to_products_df()
        return self._card_to_products_df
    
    @property
    def sealed_products_df(self) -> pl.DataFrame:
        """
        Sealed products as DataFrame.
        
        Columns: set_code, product_name, uuid, release_date, category, subtype, ...
        """
        if self._sealed_products_df is None:
            self._sealed_products_df = self._build_sealed_products_df()
        return self._sealed_products_df
    
    @property
    def sealed_contents_df(self) -> pl.DataFrame:
        """
        Sealed contents as DataFrame (normalized/exploded).
        
        Columns: set_code, product_name, content_type, item_uuid, count, ...
        """
        if self._sealed_contents_df is None:
            self._sealed_contents_df = self._build_sealed_contents_df()
        return self._sealed_contents_df
    
    @property
    def decks_df(self) -> pl.DataFrame:
        """
        Decks as DataFrame.
        
        Columns: name, set_code, type, release_date, sealed_product_uuids, ...
        """
        if self._decks_df is None:
            self._decks_df = self._build_decks_df()
        return self._decks_df
    
    @property
    def booster_df(self) -> pl.DataFrame:
        """
        Booster configurations as DataFrame.
        
        Columns: set_code, config (as JSON string or struct)
        """
        if self._booster_df is None:
            self._booster_df = self._build_booster_df()
        return self._booster_df
    
    
    def _build_card_to_products_df(self) -> pl.DataFrame:
        """Build card-to-products DataFrame."""
        data = self.card_to_products
        
        if not data:
            return pl.DataFrame(...)
        
        records = [
            {
                "uuid": card_uuid,
                "foil": products.get("foil"),
                "nonfoil": products.get("nonfoil"),
                "etched": products.get("etched"),
            }
            for card_uuid, products in data.items()
        ]
        
        df = pl.DataFrame(records, infer_schema_length=None)

        self._card_to_products = None  # Free memory
        return df
        
    def _build_sealed_products_df(self) -> pl.DataFrame:
        """Build sealed products DataFrame."""
        data = self.sealed_products
        
        if not data:
            return pl.DataFrame()
        
        records = []
        for set_code, products in data.items():
            if not isinstance(products, dict):
                continue
            for product_name, product_data in products.items():
                if not isinstance(product_data, dict):
                    continue
                record = {
                    "set_code": set_code.upper(),
                    "product_name": product_name,
                    **product_data,
                }
                records.append(record)
        
        if not records:
            return pl.DataFrame()
        
        return pl.DataFrame(records, infer_schema_length=None)
    
    def _build_sealed_contents_df(self) -> pl.DataFrame:
        """Build sealed contents DataFrame (normalized)."""
        data = self.sealed_contents
        
        if not data:
            return pl.DataFrame()
        
        records = []
        for set_code, products in data.items():
            if not isinstance(products, dict):
                continue
            for product_name, contents in products.items():
                if not isinstance(contents, dict):
                    continue
                
                # Extract size/card_count at product level
                product_size = contents.get("size")
                card_count = contents.get("card_count")
                
                # Iterate content types (pack, card, deck, sealed, variable, etc.)
                for content_type, items in contents.items():
                    if content_type in ("size", "card_count"):
                        continue
                    
                    if isinstance(items, list):
                        for item in items:
                            if isinstance(item, dict):
                                record = {
                                    "set_code": set_code.upper(),
                                    "product_name": product_name,
                                    "product_size": product_size,
                                    "card_count": card_count,
                                    "content_type": content_type,
                                    **item,
                                }
                            else:
                                record = {
                                    "set_code": set_code.upper(),
                                    "product_name": product_name,
                                    "product_size": product_size,
                                    "card_count": card_count,
                                    "content_type": content_type,
                                    "item": item,
                                }
                            records.append(record)
        
        if not records:
            return pl.DataFrame()
        
        return pl.DataFrame(records, infer_schema_length=None)
    
    def _build_decks_by_set(self) -> None:
        """Process raw decks into decks_by_set dict."""
        if self._decks_raw is None:
            self._load_decks()
        
        self._decks_by_set = defaultdict(list)
        deck_map = self._deck_map or {}
        
        for deck in (self._decks_raw or []):
            set_code = deck.get("set_code", "").upper()
            deck_name = deck.get("name", "")
            
            # Get sealed product UUIDs from deck_map
            sealed_uuids = deck_map.get(set_code.lower(), {}).get(deck_name)
            
            processed_deck = {
                "name": deck_name,
                "code": set_code,
                "type": deck.get("type"),
                "release_date": deck.get("release_date"),
                "source_set_codes": [s.upper() for s in deck.get("sourceSetCodes", [])],
                "sealed_product_uuids": sealed_uuids,
                "main_board": self._extract_deck_cards(deck.get("cards", [])),
                "side_board": self._extract_deck_cards(deck.get("sideboard", [])),
                "commander": self._extract_deck_cards(deck.get("commander", [])),
                "display_commander": self._extract_deck_cards(deck.get("displayCommander", [])),
                "planes": self._extract_deck_cards(deck.get("planarDeck", [])),
                "schemes": self._extract_deck_cards(deck.get("schemeDeck", [])),
                "tokens": self._extract_deck_cards(deck.get("tokens", [])),
            }
            
            self._decks_by_set[set_code].append(processed_deck)
    
    @staticmethod
    def _extract_deck_cards(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract minimal card info for deck lists."""
        return [
            {
                "uuid": card.get("mtgjson_uuid"),
                "count": card.get("count", 1),
                "is_foil": card.get("foil", False),
                "is_etched": card.get("etched", False),
            }
            for card in cards
            if card.get("mtgjson_uuid")
        ]
    
    def _build_decks_df(self) -> pl.DataFrame:
        """Build decks DataFrame."""
        decks = self.decks_by_set
        
        if not decks:
            return pl.DataFrame()
        
        records = []
        for set_code, set_decks in decks.items():
            for deck in set_decks:
                # Flatten for DataFrame, keep card lists as-is
                records.append({
                    "name": deck["name"],
                    "set_code": deck["code"],
                    "type": deck["type"],
                    "release_date": deck["release_date"],
                    "source_set_codes": deck["source_set_codes"],
                    "sealed_product_uuids": deck["sealed_product_uuids"],
                    "main_board_count": len(deck["main_board"]),
                    "side_board_count": len(deck["side_board"]),
                    "commander_count": len(deck["commander"]),
                })
        
        if not records:
            return pl.DataFrame()
        
        return pl.DataFrame(records, infer_schema_length=None)
    
    def _build_booster_df(self) -> pl.DataFrame:
        """Build booster configuration DataFrame."""
        data = self.booster_data
        
        if not data:
            return pl.DataFrame()
        
        records = [
            {"set_code": set_code, "config": config}
            for set_code, config in data.items()
        ]
        
        if not records:
            return pl.DataFrame()
        
        return pl.DataFrame(records, infer_schema_length=None)
    
    def get_source_products(self, card_uuid: str) -> Optional[Dict[str, List[str]]]:
        """
        Get sealed products containing a specific card.
        
        Args:
            card_uuid: MTGJSON card UUID
            
        Returns:
            Dict with keys "foil", "nonfoil", "etched" (each a list of product UUIDs),
            or None if card not found.
        """
        return self.card_to_products.get(card_uuid)
    
    def get_booster_data(self, set_code: str) -> Optional[Dict[str, Any]]:
        """
        Get booster configuration for a set.
        
        Args:
            set_code: Set code (case-insensitive)
            
        Returns:
            Booster configuration dict, or None if not found.
        """
        data = self.booster_data.get(set_code.upper())
        return recursive_sort(data) if data else None
    
    def get_sealed_products(self, set_code: str) -> Dict[str, Any]:
        """
        Get sealed products for a set.
        
        Args:
            set_code: Set code (case-insensitive)
            
        Returns:
            Dict of {product_name: product_data}
        """
        return self.sealed_products.get(set_code.lower(), {})
    
    def get_sealed_contents(self, set_code: str) -> Dict[str, Any]:
        """
        Get sealed product contents for a set.
        
        Args:
            set_code: Set code (case-insensitive)
            
        Returns:
            Dict of {product_name: contents_data}
        """
        return self.sealed_contents.get(set_code.lower(), {})
    
    def get_decks(self, set_code: str) -> List[Dict[str, Any]]:
        """
        Get preconstructed decks for a set.
        
        Args:
            set_code: Set code (case-insensitive)
            
        Returns:
            List of deck dicts
        """
        return self.decks_by_set.get(set_code.upper(), [])
    
    def join_source_products(
        self,
        cards_df: pl.DataFrame,
        uuid_col: str = "uuid",
    ) -> pl.DataFrame:
        """
        Join source products to a cards DataFrame.
        
        Args:
            cards_df: DataFrame with card UUIDs
            uuid_col: Name of the UUID column to join on
            
        Returns:
            DataFrame with source_products struct column added
        """
        return (
            cards_df
            .join(
                self.card_to_products_df.rename({"uuid": uuid_col}),
                on=uuid_col,
                how="left",
            )
            .with_columns(
                pl.struct([
                    pl.col("foil"),
                    pl.col("nonfoil"),
                    pl.col("etched"),
                ]).alias("sourceProducts")
            )
            .drop(["foil", "nonfoil", "etched"])
        )
    
    def get_products_for_cards(self, card_uuids: List[str]) -> pl.DataFrame:
        """
        Get source products for a list of card UUIDs.
        
        Args:
            card_uuids: List of MTGJSON card UUIDs
            
        Returns:
            Filtered card_to_products_df
        """
        return self.card_to_products_df.filter(pl.col("uuid").is_in(card_uuids))
       
    def clear_cache(self) -> None:
        """Clear all cached data and DataFrames."""
        self._card_to_products = None
        self._sealed_products = None
        self._sealed_contents = None
        self._deck_map = None
        self._decks_raw = None
        self._booster_data = None
        self._decks_by_set = None
        self._card_to_products_df = None
        self._sealed_products_df = None
        self._sealed_contents_df = None
        self._decks_df = None
        self._booster_df = None
    
    def preload_all(self) -> "GitHubDataProvider":
        """Preload all data and build all DataFrames."""
        self.load()
        _ = self.card_to_products_df
        _ = self.sealed_products_df
        _ = self.sealed_contents_df
        _ = self.decks_df
        _ = self.booster_df
        return self
    
    def __enter__(self) -> "GitHubDataProvider":
        return self
    
    def __exit__(self, *args) -> None:
        self.close()
    
    def __repr__(self) -> str:
        loaded = []
        if self._card_to_products is not None:
            loaded.append(f"card_map({len(self._card_to_products)})")
        if self._sealed_products is not None:
            loaded.append(f"sealed({len(self._sealed_products)} sets)")
        if self._booster_data is not None:
            loaded.append(f"boosters({len(self._booster_data)} sets)")
        if self._decks_by_set is not None:
            total_decks = sum(len(d) for d in self._decks_by_set.values())
            loaded.append(f"decks({total_decks})")
        
        status = ", ".join(loaded) if loaded else "empty"
        return f"GitHubDataProvider({status})"
