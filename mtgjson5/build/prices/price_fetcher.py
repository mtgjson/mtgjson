"""Background raw price fetcher.

Runs all provider network fetches concurrently in a background thread,
saving raw data (provider-native IDs, no UUIDs) to parquet files.
Designed to overlap with build_cards() and assembly stages.

Thread-safe: does NOT touch GlobalCache. Creates fresh provider instances
that read credentials from MtgjsonConfig (file-based, no shared state).
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections.abc import Awaitable
from dataclasses import dataclass, field
from pathlib import Path

from mtgjson5 import constants

LOGGER = logging.getLogger(__name__)


@dataclass
class PriceFetcher:
    """Orchestrates raw price fetches in a background thread."""

    cache_dir: Path = field(default_factory=lambda: constants.CACHE_PATH)
    _done: threading.Event = field(default_factory=threading.Event, repr=False)
    _error: BaseException | None = field(default=None, repr=False)
    _thread: threading.Thread | None = field(default=None, repr=False)
    _timings: dict[str, float] = field(default_factory=dict, repr=False)

    @classmethod
    def start_background(cls) -> PriceFetcher:
        """Start raw fetches in a daemon thread. Returns immediately."""
        fetcher = cls()
        fetcher._thread = threading.Thread(
            target=fetcher._run,
            name="price-raw-fetcher",
            daemon=True,
        )
        fetcher._thread.start()
        LOGGER.info("PriceFetcher: background thread started")
        return fetcher

    def wait(self, timeout: float | None = None) -> bool:
        """Block until done. Returns True if finished, False if timed out."""
        return self._done.wait(timeout=timeout)

    def is_done(self) -> bool:
        """Non-blocking check."""
        return self._done.is_set()

    @property
    def timings(self) -> dict[str, float]:
        """Per-provider fetch timings in seconds. Available after wait()."""
        return self._timings

    def raise_if_error(self) -> None:
        """Re-raise background thread exception in caller."""
        if self._error is not None:
            raise RuntimeError(f"PriceFetcher failed: {self._error}") from self._error

    def _run(self) -> None:
        """Thread entry point: run async fetches in a new event loop."""
        try:
            asyncio.run(self._fetch_all_raw())
        except Exception as exc:
            LOGGER.error(f"PriceFetcher: background fetch failed: {exc}")
            self._error = exc
        finally:
            self._done.set()

    async def _fetch_all_raw(self) -> None:
        """Run all provider raw fetches concurrently."""
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        async def _timed(name: str, coro: Awaitable[None]) -> None:
            t0 = time.perf_counter()
            await coro
            self._timings[name] = round(time.perf_counter() - t0, 1)

        provider_names = ["TCGPlayer", "CardHoarder", "Manapool", "CardMarket", "CardKingdom"]
        results = await asyncio.gather(
            _timed("TCGPlayer", self._fetch_tcg_raw()),
            _timed("CardHoarder", self._fetch_cardhoarder_raw()),
            _timed("Manapool", self._fetch_manapool_raw()),
            _timed("CardMarket", self._fetch_cardmarket_raw()),
            _timed("CardKingdom", self._fetch_cardkingdom_raw()),
            return_exceptions=True,
        )

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                LOGGER.warning(f"PriceFetcher: {provider_names[i]} failed: {result}")

        LOGGER.info("PriceFetcher: all raw fetches complete (%s)", self._timings)

    async def _fetch_tcg_raw(self) -> None:
        """Fetch raw TCGPlayer prices to parquet cache."""
        from mtgjson5.providers.tcgplayer.prices import TCGPlayerPriceProvider

        provider = TCGPlayerPriceProvider()
        if provider._config is None:
            LOGGER.info("PriceFetcher: TCGPlayer not configured, skipping")
            return
        df = await provider.fetch_raw_prices()
        LOGGER.info(f"PriceFetcher: TCGPlayer raw: {len(df):,} rows")

    async def _fetch_cardhoarder_raw(self) -> None:
        """Fetch raw CardHoarder MTGO prices to parquet cache."""
        from mtgjson5.providers.cardhoarder.provider import CardHoarderPriceProvider

        provider = CardHoarderPriceProvider()
        if provider._config is None:
            LOGGER.info("PriceFetcher: CardHoarder not configured, skipping")
            return
        df = await provider.fetch_raw_prices()
        LOGGER.info(f"PriceFetcher: CardHoarder raw: {len(df):,} rows")

    async def _fetch_manapool_raw(self) -> None:
        """Fetch raw Manapool prices to parquet cache."""
        from mtgjson5.providers.manapool.provider import ManapoolPriceProvider

        provider = ManapoolPriceProvider()
        df = await provider.fetch_raw_prices()
        LOGGER.info(f"PriceFetcher: Manapool raw: {len(df):,} rows")

    async def _fetch_cardmarket_raw(self) -> None:
        """Fetch raw CardMarket prices to parquet cache."""
        from mtgjson5.providers.cardmarket.provider import CardMarketProvider

        provider = CardMarketProvider()
        df = await provider.fetch_raw_prices()
        await provider.close()
        LOGGER.info(f"PriceFetcher: CardMarket raw: {len(df):,} rows")

    async def _fetch_cardkingdom_raw(self) -> None:
        """Fetch raw CardKingdom prices to parquet cache."""
        from mtgjson5.providers.cardkingdom.provider import CKProvider

        provider = CKProvider()
        await provider.load_or_fetch_async(self.cache_dir / "ck_raw.parquet")
        LOGGER.info(f"PriceFetcher: CardKingdom raw: {len(provider.raw_df):,} rows")
