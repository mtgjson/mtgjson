"""
Scryfall Data Source Abstraction.

Provides a unified interface for accessing Scryfall card data from either:
- Direct API calls (default behavior)
- Bulk NDJSON files (--bulk-files flag)

This module uses Polars SQL mode to translate Scryfall search syntax
into SQL queries against the bulk data.
"""

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Protocol, runtime_checkable

import polars as pl

from mtgjson5 import constants


if TYPE_CHECKING:
    from .monolith import ScryfallProvider

LOGGER = logging.getLogger(__name__)


@dataclass
class ParsedQuery:
    """Parsed Scryfall search query components."""

    set_code: str | None = None
    oracle_id: str | None = None  # oracleid:{uuid} search
    is_booster: bool = False
    is_alchemy: bool = False
    lang_any: bool = False  # lang:any - include all languages
    oracle_regex: str | None = None
    oracle_contains: list[str] = field(default_factory=list)
    oracle_or_groups: list[list[str]] = field(
        default_factory=list
    )  # For complex OR logic
    spellbook_name: str | None = None
    include_extras: bool = False
    include_variations: bool = False
    unique: str | None = None  # "prints", "art", "cards"
    order: str | None = None  # "set", "name", etc.


def parse_scryfall_query(url: str) -> ParsedQuery:
    """
    Parse a Scryfall API URL into query components.

    Supports the subset of Scryfall syntax used by MTGJSON:
    - e:{set} or set:{set} - filter by set code
    - is:booster - cards found in boosters
    - is:alchemy - alchemy cards
    - o:{text} - oracle text contains
    - oracle:/{regex}/ - oracle text regex
    - spellbook:"{name}" - cards in a spellbook
    - unique=prints - unique printings
    - include_extras=true - include extras
    - include_variations=true - include variations
    """
    query = ParsedQuery()

    # Extract URL parameters
    if "include_extras=true" in url:
        query.include_extras = True
    if "include_variations=true" in url:
        query.include_variations = True

    # Extract unique parameter
    unique_match = re.search(r"unique[=:](\w+)", url)
    if unique_match:
        query.unique = unique_match.group(1)

    # Extract order parameter
    order_match = re.search(r"order[=:](\w+)", url)
    if order_match:
        query.order = order_match.group(1)

    # Extract the q= parameter (URL decoded)
    q_match = re.search(r"[?&]q=([^&]+)", url)
    if not q_match:
        return query

    q_value = q_match.group(1)
    # URL decode common patterns
    q_value = q_value.replace("%3A", ":").replace("%20", " ").replace("%22", '"')
    q_value = q_value.replace("%27", "'").replace("%7C", "|")

    # Parse e:{set} or set:{set}
    set_match = re.search(r"(?:e|set):(\w+)", q_value, re.IGNORECASE)
    if set_match:
        query.set_code = set_match.group(1).upper()

    # Parse oracleid:{uuid} - for printing lookups
    oracle_id_match = re.search(r"oracleid[=:]([a-f0-9-]+)", q_value, re.IGNORECASE)
    if oracle_id_match:
        query.oracle_id = oracle_id_match.group(1).lower()

    # Parse lang:any - include all languages (not just English)
    if "lang:any" in q_value.lower() or "+lang%3aany" in url.lower():
        query.lang_any = True

    # Parse is:booster
    if "is:booster" in q_value.lower():
        query.is_booster = True

    # Parse is:alchemy
    if "is:alchemy" in q_value.lower():
        query.is_alchemy = True

    # Parse oracle regex: oracle:/{pattern}/
    oracle_regex_match = re.search(r"oracle:/([^/]+)/", q_value)
    if oracle_regex_match:
        query.oracle_regex = oracle_regex_match.group(1)

    # Parse complex OR groups: (o:word1 o:word2) or (o:word3 o:word4)
    # This handles CARDS_WITHOUT_LIMITS_URL pattern
    or_groups = re.findall(r"\(([^)]+)\)", q_value)
    if or_groups:
        for group in or_groups:
            words = re.findall(r"o:(\w+)", group)
            if words:
                query.oracle_or_groups.append(words)
    else:
        # Simple o:word patterns (AND logic)
        oracle_matches = re.findall(r"o:(\w+)", q_value)
        if oracle_matches:
            query.oracle_contains = oracle_matches

    # Parse spellbook:"{name}"
    spellbook_match = re.search(r'spellbook:"([^"]+)"', q_value)
    if spellbook_match:
        query.spellbook_name = spellbook_match.group(1)

    return query


def build_sql_query(query: ParsedQuery, table_name: str = "cards") -> str:
    """
    Convert a ParsedQuery into a SQL query string.

    Returns a SQL SELECT statement that can be executed against the bulk data.
    """
    conditions = []

    # Language filter - default to English unless lang:any specified
    if not query.lang_any:
        conditions.append("lang = 'en'")

    # Oracle ID filter (most specific - for printings lookup)
    if query.oracle_id:
        conditions.append(f"oracle_id = '{query.oracle_id}'")

    # Set code filter
    if query.set_code:
        conditions.append(f"UPPER(set) = '{query.set_code}'")

    # is:booster - cards that appear in boosters
    if query.is_booster:
        conditions.append("booster = true")

    # is:alchemy - alchemy format cards
    if query.is_alchemy:
        conditions.append("(set_type = 'alchemy' OR name LIKE 'A-%')")

    # Oracle text regex (use SIMILAR TO or REGEXP depending on support)
    if query.oracle_regex:
        # Escape single quotes in regex
        escaped_regex = query.oracle_regex.replace("'", "''")
        conditions.append(f"COALESCE(oracle_text, '') LIKE '%{escaped_regex}%'")

    # Oracle text contains - simple AND logic
    if query.oracle_contains:
        for word in query.oracle_contains:
            escaped_word = word.replace("'", "''").lower()
            conditions.append(
                f"LOWER(COALESCE(oracle_text, '')) LIKE '%{escaped_word}%'"
            )

    # Oracle OR groups - complex (group1 AND words) OR (group2 AND words)
    if query.oracle_or_groups:
        or_clauses = []
        for group in query.oracle_or_groups:
            and_clauses = []
            for word in group:
                escaped_word = word.replace("'", "''").lower()
                and_clauses.append(
                    f"LOWER(COALESCE(oracle_text, '')) LIKE '%{escaped_word}%'"
                )
            if and_clauses:
                or_clauses.append(f"({' AND '.join(and_clauses)})")
        if or_clauses:
            conditions.append(f"({' OR '.join(or_clauses)})")

    # Spellbook filter - requires join with spellbook data
    if query.spellbook_name:
        # This would need a subquery or join - log warning for now
        LOGGER.warning(
            f"Spellbook search '{query.spellbook_name}' requires spellbook table join"
        )

    # Build WHERE clause
    where_clause = " AND ".join(conditions) if conditions else "1=1"

    # Build ORDER BY clause
    order_clause = ""
    if query.order == "set":
        order_clause = "ORDER BY set, collector_number"
    elif query.order == "name":
        order_clause = "ORDER BY name"
    elif query.order == "released":
        order_clause = "ORDER BY released_at DESC"

    # Build the full query
    sql = f"SELECT * FROM {table_name} WHERE {where_clause}"
    if order_clause:
        sql += f" {order_clause}"

    return sql


# Fields that should be float but Polars may read as strings from NDJSON
FLOAT_FIELDS = {"cmc"}

# Fields that should be int but Polars may read as strings from NDJSON
INT_FIELDS = {
    "edhrec_rank",
    "penny_rank",
    "mtgo_id",
    "mtgo_foil_id",
    "tcgplayer_id",
    "tcgplayer_etched_id",
    "cardmarket_id",
    "arena_id",
}


def _coerce_types(cards: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Coerce bulk data to match API response format.

    1. Convert string numeric values to proper types (Polars NDJSON issue)
    2. Strip None-valued keys (API omits them, bulk includes them)
    """
    result = []
    for card in cards:
        # Strip None values to match API sparse response format
        card = {k: v for k, v in card.items() if v is not None}

        # Coerce numeric types
        for field_name in FLOAT_FIELDS:
            if field_name in card:
                val = card[field_name]
                if isinstance(val, str):
                    try:
                        card[field_name] = float(val)
                    except (ValueError, TypeError):
                        pass
        for field_name in INT_FIELDS:
            if field_name in card:
                val = card[field_name]
                if isinstance(val, str):
                    try:
                        card[field_name] = int(float(val))  # Handle "123.0" strings
                    except (ValueError, TypeError):
                        pass
        result.append(card)
    return result


# =============================================================================
# Data Source Protocol and Implementations
# =============================================================================


@runtime_checkable
class ScryfallDataSource(Protocol):
    """Protocol for Scryfall data access."""

    def search(self, url: str) -> list[dict[str, Any]]:
        """
        Execute a Scryfall search query.

        Args:
            url: Full Scryfall API URL with query parameters

        Returns:
            List of card objects as dicts (same format as API response)
        """

    def get_card_by_id(self, scryfall_id: str) -> dict[str, Any] | None:
        """Get a single card by its Scryfall ID."""


class BulkDataSource:
    """
    Data source that reads from Scryfall bulk NDJSON files.

    Uses Polars SQL mode to execute queries against the bulk data,
    translating Scryfall search syntax to SQL.

    Supports fallback to API calls when:
    - Bulk data files don't exist
    - Query returns no results (e.g., new set not in bulk data)
    - Specific card not found in bulk data
    """

    def __init__(
        self,
        cache_path: Path | None = None,
        api_fallback: bool = True,
        api_provider: Optional["ScryfallProvider"] = None,
    ):
        """
        Initialize the bulk data source.

        Args:
            cache_path: Path to bulk data cache directory
            api_fallback: If True, fall back to API when bulk data missing/empty
            api_provider: ScryfallProvider instance for API fallback (lazy-loaded if None)
        """
        self.cache_path = cache_path or constants.CACHE_PATH
        self.api_fallback = api_fallback
        self._api_provider: ScryfallProvider | None = api_provider
        self._cards_df: pl.LazyFrame | None = None
        self._rulings_df: pl.LazyFrame | None = None
        self._sql_context: pl.SQLContext | None = None
        self._loaded = False
        self._bulk_available = False
        # Caches for frequently accessed lookups
        self._oracle_id_cache: dict[str, list[dict[str, Any]]] | None = None
        self._oracle_id_cache_built = False

    @property
    def api_provider(self) -> "ScryfallProvider":
        """Lazy-load the API provider for fallback."""
        if self._api_provider is None:
            # Import here to avoid circular imports
            from .monolith import ScryfallProvider

            self._api_provider = ScryfallProvider()
        return self._api_provider

    @property
    def is_loaded(self) -> bool:
        """Whether bulk data loading has been attempted."""
        return self._loaded

    @property
    def is_available(self) -> bool:
        """Whether bulk data is available for queries."""
        return self._bulk_available

    def ensure_loaded(self) -> None:
        """Public method to ensure bulk data is loaded."""
        self._ensure_loaded()

    def _ensure_loaded(self) -> None:
        """Lazy-load bulk data on first access."""
        if self._loaded:
            return

        cards_path = self.cache_path / "all_cards.ndjson"
        rulings_path = self.cache_path / "rulings.ndjson"

        if not cards_path.exists():
            if self.api_fallback:
                LOGGER.warning(
                    f"Bulk cards file not found: {cards_path}. "
                    "Will use API fallback for all queries."
                )
                self._loaded = True
                self._bulk_available = False
                return
            raise FileNotFoundError(
                f"Bulk cards file not found: {cards_path}. "
                "Run with --polars first to download bulk data, or remove --bulk-files flag."
            )

        LOGGER.info(f"Loading bulk data from {self.cache_path}")

        self._cards_df = pl.scan_ndjson(
            cards_path,
            infer_schema_length=10000,
        )

        if rulings_path.exists():
            self._rulings_df = pl.scan_ndjson(
                rulings_path,
                infer_schema_length=1000,
            )

        # Create SQL context and register tables
        self._sql_context = pl.SQLContext()
        self._sql_context.register("cards", self._cards_df)
        if self._rulings_df is not None:
            self._sql_context.register("rulings", self._rulings_df)

        self._loaded = True
        self._bulk_available = True
        LOGGER.info("Bulk data loaded into SQL context")

    def _build_oracle_id_cache(self) -> None:
        """
        Build an in-memory cache of all cards grouped by oracle_id.

        This allows O(1) lookups for printings queries instead of
        re-scanning the NDJSON file for each card.
        """
        if self._oracle_id_cache_built or self._sql_context is None:
            return

        LOGGER.info("Building oracle_id lookup cache (one-time operation)...")

        # Get all cards and group by oracle_id
        sql = "SELECT * FROM cards"
        result = self._sql_context.execute(sql)

        # Build the lookup dictionary
        self._oracle_id_cache = {}
        # SQLContext.execute() returns LazyFrame, need to collect first
        result_df = result.collect() if isinstance(result, pl.LazyFrame) else result
        for card in _coerce_types(result_df.to_dicts()):
            oracle_id = card.get("oracle_id")
            if oracle_id:
                if oracle_id not in self._oracle_id_cache:
                    self._oracle_id_cache[oracle_id] = []
                self._oracle_id_cache[oracle_id].append(card)

        self._oracle_id_cache_built = True
        LOGGER.info(
            f"Oracle ID cache built: {len(self._oracle_id_cache)} unique oracle IDs"
        )

    def _get_by_oracle_id(
        self, oracle_id: str, lang_any: bool = False
    ) -> list[dict[str, Any]]:
        """
        Get all printings for a given oracle_id using the cache.

        Args:
            oracle_id: The oracle ID to look up
            lang_any: If True, include all languages; if False, only English

        Returns:
            List of card dicts matching the oracle_id
        """
        self._build_oracle_id_cache()

        if self._oracle_id_cache is None:
            return []

        cards = self._oracle_id_cache.get(oracle_id, [])

        if not lang_any:
            cards = [c for c in cards if c.get("lang") == "en"]

        return cards

    def search(self, url: str) -> list[dict[str, Any]]:
        """
        Execute a Scryfall-style search query against bulk data using SQL.

        Parses the URL to extract query parameters and translates them
        to a SQL query executed via Polars SQLContext.

        For oracle_id queries, uses an in-memory cache for O(1) lookups.

        Falls back to API if:
        - Bulk data not available
        - Query returns no results and fallback is enabled
        """
        self._ensure_loaded()

        # If bulk data not available, use API directly
        if not self._bulk_available:
            if self.api_fallback:
                LOGGER.debug(f"Using API fallback (no bulk data): {url}")
                return list(self.api_provider.download_all_pages_api(url))
            return []

        if self._sql_context is None:
            return []

        # Parse the query
        parsed = parse_scryfall_query(url)
        LOGGER.debug(f"Parsed query: {parsed}")

        # Fast path: oracle_id queries use the cache
        if parsed.oracle_id:
            results = self._get_by_oracle_id(parsed.oracle_id, parsed.lang_any)
            if not results and self.api_fallback:
                LOGGER.info(f"Oracle ID not in cache, falling back to API: {url}")
                return list(self.api_provider.download_all_pages_api(url))
            return results

        # Build SQL query for other query types
        sql = build_sql_query(parsed)
        LOGGER.debug(f"Generated SQL: {sql}")

        # Execute query
        try:
            result_lf = self._sql_context.execute(sql)

            # Apply unique parameter (post-SQL since DISTINCT on specific columns is complex)
            if parsed.unique == "cards":
                result_lf = result_lf.unique(subset=["oracle_id"], keep="first")
            elif parsed.unique == "art":
                result_lf = result_lf.unique(subset=["illustration_id"], keep="first")
            # unique=prints is default - no deduplication

            # Collect and convert to dicts
            collected = (
                result_lf
                if isinstance(result_lf, pl.DataFrame)
                else result_lf.collect()
            )
            # If no results and fallback enabled, try API
            if collected.is_empty() and self.api_fallback:
                LOGGER.info(f"No bulk results, falling back to API: {url}")
                return list(self.api_provider.download_all_pages_api(url))

            # Coerce types to match API response format
            return _coerce_types(collected.to_dicts())

        except Exception as e:
            LOGGER.error(f"SQL query failed: {e}\nQuery: {sql}")
            if self.api_fallback:
                LOGGER.info(f"SQL failed, falling back to API: {url}")
                return list(self.api_provider.download_all_pages_api(url))
            return []

    def search_sql(self, sql: str) -> list[dict[str, Any]]:
        """
        Execute a raw SQL query directly.

        Useful for complex queries that don't map to Scryfall syntax.
        """
        self._ensure_loaded()

        if self._sql_context is None:
            return []

        try:
            result_lf = self._sql_context.execute(sql).lazy()
            collected = result_lf.collect()
            return list(_coerce_types(collected.to_dicts()))
        except Exception as e:
            LOGGER.error(f"SQL query failed: {e}\nQuery: {sql}")
            return []

    def get_card_by_id(self, scryfall_id: str) -> dict[str, Any] | None:
        """Get a single card by its Scryfall ID using SQL."""
        self._ensure_loaded()

        # If bulk data not available, use API directly
        if not self._bulk_available:
            if self.api_fallback:
                LOGGER.debug(f"Using API fallback for card: {scryfall_id}")
                result = self.api_provider.download(
                    f"https://api.scryfall.com/cards/{scryfall_id}"
                )
                return result if result else None
            return None

        if self._sql_context is None:
            return None

        sql = f"SELECT * FROM cards WHERE id = '{scryfall_id}' LIMIT 1"
        try:
            result = self._sql_context.execute(sql).lazy().collect()
            if result.is_empty():
                # Card not in bulk data - try API fallback
                if self.api_fallback:
                    LOGGER.info(
                        f"Card {scryfall_id} not in bulk data, falling back to API"
                    )
                    api_result = self.api_provider.download(
                        f"https://api.scryfall.com/cards/{scryfall_id}"
                    )
                    return api_result if api_result else None
                return None
            dicts = result.to_dicts()
            return dicts[0] if dicts else None
        except Exception as e:
            LOGGER.error(f"SQL query failed: {e}")
            if self.api_fallback:
                api_result = self.api_provider.download(
                    f"https://api.scryfall.com/cards/{scryfall_id}"
                )
                return api_result if api_result else None
            return None

    def get_cards_by_set(self, set_code: str) -> list[dict[str, Any]]:
        """
        Convenience method: Get all cards for a set.

        Equivalent to CARDS_URL_ALL_DETAIL_BY_SET_CODE query.
        """
        url = f"https://api.scryfall.com/cards/search?include_extras=true&include_variations=true&order=set&q=e%3A{set_code}&unique=prints"
        return self.search(url)

    def get_booster_cards(self, set_code: str) -> list[dict[str, Any]]:
        """
        Get cards that appear in boosters for a set.

        Equivalent to CARDS_IN_BASE_SET_URL query.
        """
        url = f"https://api.scryfall.com/cards/search?order=set&q=set:{set_code}%20is:booster%20unique:prints"
        return self.search(url)

    def get_cards_without_limits(self) -> list[dict[str, Any]]:
        """
        Get cards that can have unlimited copies in a deck.

        Equivalent to CARDS_WITHOUT_LIMITS_URL query.
        """
        sql = """
        SELECT * FROM cards
        WHERE lang = 'en' AND (
            (LOWER(COALESCE(oracle_text, '')) LIKE '%deck%'
             AND LOWER(COALESCE(oracle_text, '')) LIKE '%any%'
             AND LOWER(COALESCE(oracle_text, '')) LIKE '%number%'
             AND LOWER(COALESCE(oracle_text, '')) LIKE '%cards%'
             AND LOWER(COALESCE(oracle_text, '')) LIKE '%named%')
            OR
            (LOWER(COALESCE(oracle_text, '')) LIKE '%deck%'
             AND LOWER(COALESCE(oracle_text, '')) LIKE '%have%'
             AND LOWER(COALESCE(oracle_text, '')) LIKE '%up%'
             AND LOWER(COALESCE(oracle_text, '')) LIKE '%to%'
             AND LOWER(COALESCE(oracle_text, '')) LIKE '%cards%'
             AND LOWER(COALESCE(oracle_text, '')) LIKE '%named%')
        )
        """
        return self.search_sql(sql)

    def get_alchemy_spellbook_cards(self) -> list[dict[str, Any]]:
        """
        Get alchemy cards with spellbooks.

        Equivalent to CARDS_WITH_ALCHEMY_SPELLBOOK_URL query.
        """
        sql = """
        SELECT * FROM cards
        WHERE lang = 'en'
          AND (set_type = 'alchemy' OR name LIKE 'A-%')
          AND (
            LOWER(COALESCE(oracle_text, '')) LIKE '%conjure%'
            OR LOWER(COALESCE(oracle_text, '')) LIKE '%draft%'
            OR LOWER(COALESCE(oracle_text, '')) LIKE '%spellbook%'
          )
        """
        return self.search_sql(sql)

    def get_rulings(self, oracle_id: str) -> list[dict[str, Any]]:
        """Get rulings for a card by oracle ID."""
        self._ensure_loaded()

        if self._sql_context is None or self._rulings_df is None:
            return []

        sql = f"""
        SELECT * FROM rulings
        WHERE oracle_id = '{oracle_id}'
        ORDER BY published_at
        """
        return self.search_sql(sql)

    def get_foreign_cards(
        self, set_code: str, collector_number: str
    ) -> list[dict[str, Any]]:
        """
        Get non-English printings of a card.

        Used for foreignData field in MTGJSON.
        """
        self._ensure_loaded()

        if self._sql_context is None:
            return []

        # Escape collector_number for SQL
        escaped_cn = collector_number.replace("'", "''")
        sql = f"""
        SELECT * FROM cards
        WHERE UPPER(set) = '{set_code.upper()}'
          AND collector_number = '{escaped_cn}'
          AND lang != 'en'
        """
        return self.search_sql(sql)


# =============================================================================
# Module-level singleton management
# =============================================================================


class _SingletonHolder:
    """Container class to avoid global statement for singleton management."""

    instance: BulkDataSource | None = None


def get_bulk_data_source() -> BulkDataSource:
    """Get or create the singleton bulk data source."""
    if _SingletonHolder.instance is None:
        _SingletonHolder.instance = BulkDataSource()
    return _SingletonHolder.instance


def reset_bulk_data_source() -> None:
    """Reset the singleton (useful for testing)."""
    _SingletonHolder.instance = None
