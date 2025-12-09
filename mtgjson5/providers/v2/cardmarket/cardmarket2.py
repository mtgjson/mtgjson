"""
CardMarket (MKM) Provider V2 - Async wrapper for background fetching.

Wraps sync mkmsdk calls in asyncio.to_thread() to allow concurrent execution
with CPU-bound Polars operations while GIL is released.
"""

import asyncio
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

import mkmsdk.exceptions
import polars as pl
from mkmsdk.api_map import _API_MAP
from mkmsdk.mkm import Mkm

from .... import constants
from ....mtgjson_config import MtgjsonConfig

LOGGER = logging.getLogger(__name__)

# Progress callback type: (completed_sets, total_sets, set_name)
ProgressCallback = Callable[[int, int, str], None]


@dataclass
class CardMarketConfig:
    """CardMarket API credentials."""

    app_token: str
    app_secret: str
    access_token: str = ""
    access_token_secret: str = ""
    prices_api_url: str = ""

    @classmethod
    def from_mtgjson_config(cls) -> Optional["CardMarketConfig"]:
        """Load config from mtgjson.properties [CardMarket] section."""
        config = MtgjsonConfig()
        if not config.has_section("CardMarket"):
            return None

        app_token = config.get("CardMarket", "app_token", fallback="")
        app_secret = config.get("CardMarket", "app_secret", fallback="")

        if not app_token or not app_secret:
            return None

        return cls(
            app_token=app_token,
            app_secret=app_secret,
            access_token=config.get("CardMarket", "mkm_access_token", fallback=""),
            access_token_secret=config.get(
                "CardMarket", "mkm_access_token_secret", fallback=""
            ),
            prices_api_url=config.get("CardMarket", "prices_api_url", fallback=""),
        )


@dataclass
class MkmExpansion:
    """MKM expansion (set) info."""

    id: int
    name: str


class CardMarketClient:
    """Sync CardMarket client using mkmsdk."""

    def __init__(self, config: CardMarketConfig):
        self.config = config
        self._connection: Optional[Mkm] = None
        self._set_map: dict[str, MkmExpansion] = {}

    def connect(self) -> None:
        """Initialize mkmsdk connection with OAuth credentials."""
        os.environ["MKM_APP_TOKEN"] = self.config.app_token
        os.environ["MKM_APP_SECRET"] = self.config.app_secret
        os.environ["MKM_ACCESS_TOKEN"] = self.config.access_token
        os.environ["MKM_ACCESS_TOKEN_SECRET"] = self.config.access_token_secret

        self._connection = Mkm(_API_MAP["2.0"]["api"], _API_MAP["2.0"]["api_root"])
        LOGGER.debug("CardMarket connection initialized")

    def load_expansions(self) -> list[MkmExpansion]:
        """Fetch all Magic expansions from MKM."""
        if not self._connection:
            self.connect()

        try:
            if self._connection is None:
                LOGGER.error("Connection not initialized")
                return []
            resp = self._connection.market_place.expansions(game=1)
        except mkmsdk.exceptions.ConnectionError as e:
            LOGGER.error(f"Failed to fetch MKM expansions: {e}")
            return []

        if resp.status_code != 200:
            LOGGER.error(f"MKM expansions request failed: {resp.status_code}")
            return []

        try:
            data = resp.json()
        except json.JSONDecodeError as e:
            LOGGER.error(f"Failed to parse MKM expansions: {e}")
            return []

        expansions = []
        for exp in data.get("expansion", []):
            expansion = MkmExpansion(
                id=exp["idExpansion"],
                name=exp["enName"],
            )
            expansions.append(expansion)
            self._set_map[exp["enName"].lower()] = expansion

        # Apply manual fixes
        self._apply_set_name_fixes()

        LOGGER.info(f"Loaded {len(expansions)} MKM expansions")
        return expansions

    def _apply_set_name_fixes(self) -> None:
        """Apply manual set name overrides from resource file."""
        fixes_path = constants.RESOURCE_PATH / "mkm_set_name_fixes.json"
        if not fixes_path.exists():
            return

        with fixes_path.open(encoding="utf-8") as f:
            fixes = json.load(f)

        for old_name, new_name in fixes.items():
            old_key = old_name.lower()
            if old_key in self._set_map:
                self._set_map[new_name.lower()] = self._set_map[old_key]
                del self._set_map[old_key]

    def get_expansion_cards(
        self, expansion: MkmExpansion, retries: int = 3
    ) -> list[dict]:
        """
        Fetch all cards for an expansion.

        :param expansion: MKM expansion to fetch
        :param retries: Number of retry attempts
        :return: List of card dicts with mcmId, name, number, etc.
        """
        if not self._connection:
            self.connect()

        for attempt in range(retries):
            try:
                if self._connection is None:
                    LOGGER.error("Connection not initialized")
                    return []
                resp = self._connection.market_place.expansion_singles(
                    1, expansion=expansion.id
                )
                break
            except mkmsdk.exceptions.ConnectionError as e:
                LOGGER.warning(
                    f"MKM connection error for {expansion.name} (attempt {attempt + 1}): {e}"
                )
                if attempt < retries - 1:
                    import time

                    time.sleep(5)
        else:
            LOGGER.error(
                f"Failed to fetch cards for {expansion.name} after {retries} attempts"
            )
            return []

        if resp.status_code != 200:
            LOGGER.warning(
                f"MKM request failed for {expansion.name}: {resp.status_code}"
            )
            return []

        try:
            data = resp.json()
        except json.JSONDecodeError as e:
            LOGGER.warning(f"Failed to parse MKM response for {expansion.name}: {e}")
            return []

        cards = []
        for card in data.get("single", []):
            cards.append(
                {
                    "mcmId": card.get("idProduct"),
                    "mcmMetaId": card.get("idMetaproduct"),
                    "name": card.get("enName", ""),
                    "number": (card.get("number") or "").lstrip("0"),
                    "expansionId": expansion.id,
                    "expansionName": expansion.name,
                }
            )

        return cards


class CardMarketFetcher:
    """
    Async orchestrator for CardMarket data fetching.

    Wraps sync mkmsdk calls in asyncio.to_thread() for background execution.
    """

    def __init__(
        self,
        output_path: Path,
        config: CardMarketConfig | None = None,
        on_progress: Optional[ProgressCallback] = None,
    ):
        self.output_path = output_path
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.config = config or CardMarketConfig.from_mtgjson_config()
        self.on_progress = on_progress

    async def fetch_all_cards(self) -> pl.LazyFrame:
        """
        Fetch card data from all MKM expansions.

        Runs sequentially (MKM rate limits) but in background thread
        so it doesn't block the main async loop.
        """
        if not self.config:
            LOGGER.error("No CardMarket config available")
            return pl.LazyFrame()

        # Run the sync fetching in a thread
        df = await asyncio.to_thread(self._fetch_all_sync)
        return df.lazy()

    def _fetch_all_sync(self) -> pl.DataFrame:
        """Synchronous fetch of all expansion cards."""
        if self.config is None:
            LOGGER.error("No CardMarket config available")
            return pl.DataFrame()
        client = CardMarketClient(self.config)
        expansions = client.load_expansions()

        if not expansions:
            return pl.DataFrame()

        all_cards: list[dict] = []
        total = len(expansions)

        for i, expansion in enumerate(expansions):
            cards = client.get_expansion_cards(expansion)
            all_cards.extend(cards)

            if self.on_progress:
                self.on_progress(i + 1, total, expansion.name)

            LOGGER.debug(f"[{i + 1}/{total}] {expansion.name}: {len(cards)} cards")

        if not all_cards:
            return pl.DataFrame()

        df = pl.DataFrame(all_cards)
        df.write_parquet(self.output_path)
        LOGGER.info(f"Fetched {len(df)} cards from {total} expansions")

        return df


async def build_mkm_cards_df(
    output_path: Path | None = None,
    on_progress: Optional[ProgressCallback] = None,
) -> pl.LazyFrame:
    """
    Build CardMarket cards DataFrame.

    Fetches all card data from MKM expansions in background thread.

    :param output_path: Path for parquet output (uses cache dir if None)
    :param on_progress: Optional callback for progress updates
    :return: LazyFrame with mcmId, mcmMetaId, name, number, expansionId, expansionName
    """
    if output_path is None:
        output_path = Path(MtgjsonConfig().output_path) / "cache" / "mkm_cards.parquet"

    fetcher = CardMarketFetcher(output_path, on_progress=on_progress)
    return await fetcher.fetch_all_cards()


def build_mkm_cards_df_sync(
    output_path: Path | None = None,
    on_progress: Optional[ProgressCallback] = None,
) -> pl.LazyFrame:
    """Sync wrapper for build_mkm_cards_df."""
    return asyncio.run(build_mkm_cards_df(output_path, on_progress))


async def start_mkm_fetch(
    output_path: Path | None = None,
    on_progress: Optional[ProgressCallback] = None,
) -> asyncio.Task[pl.LazyFrame]:
    """
    Start MKM fetch as background task.

    Returns immediately with a Task that can be awaited later.

    Usage:
        # Start early
        mkm_task = await start_mkm_fetch()

        # Do other work (Polars computations, etc.)
        build_cards(...)

        # Await when needed
        mkm_df = await mkm_task
    """
    if output_path is None:
        output_path = Path(MtgjsonConfig().output_path) / "cache" / "mkm_cards.parquet"

    fetcher = CardMarketFetcher(output_path, on_progress=on_progress)
    return asyncio.create_task(fetcher.fetch_all_cards())
