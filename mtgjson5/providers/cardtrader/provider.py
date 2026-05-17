"""CardTrader async price provider."""

from __future__ import annotations

import asyncio
import datetime
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import aiohttp
import polars as pl

from mtgjson5 import constants
from mtgjson5.models.containers import MtgjsonPriceEntry
from mtgjson5.mtgjson_config import MtgjsonConfig

LOGGER = logging.getLogger(__name__)

CARDTRADER_API_BASE_URL = "https://api.cardtrader.com/api/v2"
MTGJSON_USER_AGENT = "MTGJSON/5.0 (https://mtgjson.com)"
MAGIC_GAME_ID = 1
MAX_LISTINGS_FOR_MARKET_PRICE = 15
CARDTRADER_MAX_REQUEST_ATTEMPTS = 3
CARDTRADER_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
CARDTRADER_HIGH_PRICE_WARNING = 10_000.0

RAW_SCHEMA = {
    "scryfallId": pl.String,
    "blueprintId": pl.String,
    "finish": pl.String,
    "price": pl.Float64,
    "currency": pl.String,
}

LOOKUP_SCHEMA = {
    "scryfallId": pl.String,
    "cardtraderId": pl.String,
}

PRICE_SCHEMA = {
    "uuid": pl.String,
    "date": pl.String,
    "source": pl.String,
    "provider": pl.String,
    "price_type": pl.String,
    "finish": pl.String,
    "price": pl.Float64,
    "currency": pl.String,
}

ProgressCallback = Callable[[int, int, str], None]


@dataclass(frozen=True)
class CardTraderPriceCalculation:
    """Internal diagnostics for a computed CardTrader marketplace price."""

    price: float
    currency: str
    listing_count: int
    min_price: float
    max_price: float


@dataclass
class CardTraderConfig:
    """CardTrader API credentials and validation settings."""

    auth_token: str
    expected_currency: str = "EUR"
    base_url: str = CARDTRADER_API_BASE_URL

    @classmethod
    def from_mtgjson_config(cls) -> CardTraderConfig | None:
        """Load config from mtgjson.properties [CardTrader] section."""
        config = MtgjsonConfig()
        if not config.has_section("CardTrader"):
            return None

        auth_token = config.get("CardTrader", "auth_token", fallback="")
        if not auth_token:
            return None

        return cls(
            auth_token=auth_token,
            expected_currency=config.get("CardTrader", "expected_currency", fallback="EUR") or "EUR",
            base_url=config.get("CardTrader", "base_url", fallback=CARDTRADER_API_BASE_URL) or CARDTRADER_API_BASE_URL,
        )


@dataclass
class CardTraderPriceProvider:
    """
    Async CardTrader pricing provider.

    Approximates CardTrader CT Market Price by averaging the first 15 returned
    marketplace listings, matching CardTrader's published site definition.
    """

    config: CardTraderConfig | None = None
    output_path: Path | None = None
    on_progress: ProgressCallback | None = None
    market_request_delay: float = 0.13
    request_retry_base_delay: float = 1.0
    today_date: str = field(default_factory=lambda: datetime.date.today().strftime("%Y-%m-%d"))

    def __post_init__(self) -> None:
        if self.config is None:
            self.config = CardTraderConfig.from_mtgjson_config()
        if self.output_path is None:
            self.output_path = constants.CACHE_PATH / "cardtrader_raw_prices.parquet"

    async def fetch_raw_prices(self) -> pl.DataFrame:
        """Fetch CardTrader marketplace prices without UUID mapping."""
        if not self.config:
            LOGGER.warning("No CardTrader config available, skipping provider")
            return pl.DataFrame(schema=RAW_SCHEMA)

        records: list[dict[str, Any]] = []

        async with aiohttp.ClientSession(headers=self._build_headers()) as session:
            try:
                expansions = self._parse_expansions(await self._request_json(session, "expansions"))
            except (aiohttp.ClientError, TimeoutError, ValueError) as exc:
                LOGGER.error("CardTrader: failed to fetch expansions: %s", exc)
                return pl.DataFrame(schema=RAW_SCHEMA)
            except Exception as exc:
                LOGGER.error("CardTrader: unexpected error fetching expansions: %s", exc)
                return pl.DataFrame(schema=RAW_SCHEMA)

            magic_expansions = [exp for exp in expansions if self._extract_game_id(exp) == MAGIC_GAME_ID]
            total = len(magic_expansions)

            for index, expansion in enumerate(magic_expansions, start=1):
                expansion_id = self._extract_expansion_id(expansion)
                if expansion_id is None:
                    continue

                expansion_name = str(expansion.get("name") or expansion.get("code") or expansion_id)
                if self.on_progress:
                    self.on_progress(index - 1, total, f"CardTrader {expansion_name}")

                try:
                    blueprints = self._parse_blueprints(
                        await self._request_json(session, "blueprints/export", {"expansion_id": expansion_id})
                    )
                    blueprint_map = self._build_blueprint_map(blueprints)
                    if not blueprint_map:
                        continue

                    records.extend(await self._fetch_expansion_records(session, expansion_id, blueprint_map))
                except (aiohttp.ClientError, TimeoutError, ValueError) as exc:
                    LOGGER.warning("CardTrader: skipping expansion %s (%s): %s", expansion_name, expansion_id, exc)
                    continue
                except Exception as exc:
                    LOGGER.warning(
                        "CardTrader: unexpected error in expansion %s (%s): %s",
                        expansion_name,
                        expansion_id,
                        exc,
                    )
                    continue

                if self.on_progress:
                    self.on_progress(index, total, f"CardTrader {expansion_name}")

        if not records:
            LOGGER.warning("CardTrader: no price data retrieved")
            return pl.DataFrame(schema=RAW_SCHEMA)

        df = pl.DataFrame(records, schema=RAW_SCHEMA)
        if self.output_path:
            self.output_path.parent.mkdir(parents=True, exist_ok=True)
            df.write_parquet(self.output_path, compression="zstd")
            LOGGER.info("Saved %s CardTrader raw price records to %s", f"{len(df):,}", self.output_path)

        return df

    async def _fetch_expansion_records(
        self,
        session: aiohttp.ClientSession,
        expansion_id: int,
        blueprint_map: dict[str, str],
    ) -> list[dict[str, Any]]:
        """Fetch and price one CardTrader expansion from product finish flags."""
        try:
            payload = await self._request_json(
                session,
                "marketplace/products",
                {"expansion_id": expansion_id, "language": "en"},
            )
        except (aiohttp.ClientError, TimeoutError, ValueError) as exc:
            LOGGER.warning(
                "CardTrader: skipping marketplace products for expansion %s: %s",
                expansion_id,
                self._format_api_exception(exc),
            )
            return []
        except Exception as exc:
            LOGGER.warning(
                "CardTrader: unexpected marketplace error for expansion %s: %s",
                expansion_id,
                exc,
            )
            return []
        finally:
            await self._sleep_between_marketplace_requests()

        return self._build_raw_records(
            self._parse_marketplace_products(payload),
            blueprint_map,
        )

    async def fetch_identifier_lookup(self) -> pl.DataFrame:
        """Fetch CardTrader blueprint identifiers keyed by Scryfall ID."""
        if not self.config:
            LOGGER.warning("No CardTrader config available, skipping provider")
            return pl.DataFrame(schema=LOOKUP_SCHEMA)

        records: list[dict[str, str]] = []

        async with aiohttp.ClientSession(headers=self._build_headers()) as session:
            try:
                expansions = self._parse_expansions(await self._request_json(session, "expansions"))
            except (aiohttp.ClientError, TimeoutError, ValueError) as exc:
                LOGGER.error("CardTrader: failed to fetch expansions for identifier lookup: %s", exc)
                return pl.DataFrame(schema=LOOKUP_SCHEMA)
            except Exception as exc:
                LOGGER.error("CardTrader: unexpected error fetching identifier lookup expansions: %s", exc)
                return pl.DataFrame(schema=LOOKUP_SCHEMA)

            magic_expansions = [exp for exp in expansions if self._extract_game_id(exp) == MAGIC_GAME_ID]
            for expansion in magic_expansions:
                expansion_id = self._extract_expansion_id(expansion)
                if expansion_id is None:
                    continue

                expansion_name = str(expansion.get("name") or expansion.get("code") or expansion_id)
                try:
                    blueprints = self._parse_blueprints(
                        await self._request_json(session, "blueprints/export", {"expansion_id": expansion_id})
                    )
                    records.extend(self._build_identifier_records(blueprints))
                except (aiohttp.ClientError, TimeoutError, ValueError) as exc:
                    LOGGER.warning(
                        "CardTrader: skipping identifier lookup for expansion %s (%s): %s",
                        expansion_name,
                        expansion_id,
                        exc,
                    )
                    continue
                except Exception as exc:
                    LOGGER.warning(
                        "CardTrader: unexpected identifier lookup error in expansion %s (%s): %s",
                        expansion_name,
                        expansion_id,
                        exc,
                    )
                    continue

        if not records:
            LOGGER.warning("CardTrader: no identifier lookup data retrieved")
            return pl.DataFrame(schema=LOOKUP_SCHEMA)

        df = pl.DataFrame(records, schema=LOOKUP_SCHEMA)
        deduped = df.unique(subset=["scryfallId"], keep="first")
        dropped = len(df) - len(deduped)
        if dropped > 0:
            LOGGER.warning("CardTrader: dropped %s duplicate blueprint mappings by scryfallId", dropped)
        return deduped

    async def fetch_prices(self, scryfall_to_uuid_map: dict[str, str]) -> pl.DataFrame:
        """Fetch CardTrader prices and map them to MTGJSON UUIDs."""
        if not scryfall_to_uuid_map:
            LOGGER.warning("CardTrader: no scryfall_to_uuid map available")
            return pl.DataFrame(schema=PRICE_SCHEMA)

        raw_df = await self.fetch_raw_prices()
        if raw_df.is_empty():
            return pl.DataFrame(schema=PRICE_SCHEMA)

        mapping_df = pl.DataFrame(
            {
                "scryfallId": list(scryfall_to_uuid_map.keys()),
                "uuid": list(scryfall_to_uuid_map.values()),
            }
        )
        return raw_df.join(mapping_df, on="scryfallId", how="inner").select(
            pl.col("uuid"),
            pl.lit(self.today_date).alias("date"),
            pl.lit("paper").alias("source"),
            pl.lit("cardtrader").alias("provider"),
            pl.lit("retail").alias("price_type"),
            pl.col("finish"),
            pl.col("price"),
            pl.col("currency"),
        )

    async def generate_today_price_dict(
        self,
        scryfall_to_uuid_map: dict[str, str],
    ) -> dict[str, MtgjsonPriceEntry]:
        """
        Generate MTGJSON-format price dict for compatibility with legacy code.

        Returns dict mapping UUID -> MtgjsonPriceEntry.
        """
        df = await self.fetch_prices(scryfall_to_uuid_map)
        return self._dataframe_to_price_dict(df)

    def _dataframe_to_price_dict(self, df: pl.DataFrame) -> dict[str, MtgjsonPriceEntry]:
        """Convert DataFrame to MTGJSON price dict format."""
        result: dict[str, MtgjsonPriceEntry] = {}

        for row in df.iter_rows(named=True):
            uuid = row["uuid"]
            finish = row["finish"]
            price = row["price"]
            currency = row["currency"]

            if uuid not in result:
                result[uuid] = MtgjsonPriceEntry("paper", "cardtrader", self.today_date, currency)

            prices_obj = result[uuid]
            if finish == "normal":
                prices_obj.sell_normal = price
            elif finish == "foil":
                prices_obj.sell_foil = price

        return result

    def _build_headers(self) -> dict[str, str]:
        """Build HTTP headers for CardTrader API requests."""
        if not self.config:
            return {"Accept": "application/json", "User-Agent": MTGJSON_USER_AGENT}

        return {
            "Authorization": f"Bearer {self.config.auth_token}",
            "Accept": "application/json",
            "User-Agent": MTGJSON_USER_AGENT,
        }

    async def _sleep_between_marketplace_requests(self) -> None:
        """Throttle marketplace requests to avoid rate-limit trouble."""
        if self.market_request_delay > 0:
            await asyncio.sleep(self.market_request_delay)

    async def _request_json(
        self,
        session: aiohttp.ClientSession,
        path: str,
        params: dict[str, str | int] | None = None,
    ) -> Any:
        """Perform a GET request against the CardTrader API."""
        if not self.config:
            return {}

        for attempt in range(1, CARDTRADER_MAX_REQUEST_ATTEMPTS + 1):
            try:
                return await self._request_json_once(session, path, params)
            except aiohttp.ClientResponseError as exc:
                if exc.status not in CARDTRADER_RETRYABLE_STATUS_CODES or attempt == CARDTRADER_MAX_REQUEST_ATTEMPTS:
                    raise
                await self._sleep_before_retry(path, params, attempt, exc)
            except (aiohttp.ClientConnectionError, aiohttp.ServerTimeoutError, TimeoutError) as exc:
                if attempt == CARDTRADER_MAX_REQUEST_ATTEMPTS:
                    raise
                await self._sleep_before_retry(path, params, attempt, exc)

        return {}

    async def _request_json_once(
        self,
        session: aiohttp.ClientSession,
        path: str,
        params: dict[str, str | int] | None = None,
    ) -> Any:
        """Perform a single GET request against the CardTrader API."""
        if not self.config:
            return {}

        url = f"{self.config.base_url.rstrip('/')}/{path.lstrip('/')}"
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=120)) as response:
            response.raise_for_status()
            return await response.json()

    async def _sleep_before_retry(
        self,
        path: str,
        params: dict[str, str | int] | None,
        attempt: int,
        exc: BaseException,
    ) -> None:
        """Back off before retrying a transient CardTrader API failure."""
        retry_after = None
        if isinstance(exc, aiohttp.ClientResponseError) and exc.headers:
            retry_after_header = exc.headers.get("Retry-After")
            if retry_after_header:
                try:
                    retry_after = float(retry_after_header)
                except ValueError:
                    retry_after = None

        delay = retry_after if retry_after is not None else self.request_retry_base_delay * (2 ** (attempt - 1))
        LOGGER.warning(
            "CardTrader: retrying %s %s after attempt %s failed: %s",
            path,
            params or {},
            attempt,
            self._format_api_exception(exc),
        )
        if delay > 0:
            await asyncio.sleep(delay)

    @staticmethod
    def _format_api_exception(exc: BaseException) -> str:
        """Format aiohttp exceptions without requiring complete request metadata."""
        if isinstance(exc, aiohttp.ClientResponseError):
            return f"HTTP {exc.status} {exc.message}"
        return repr(exc)

    @staticmethod
    def _parse_expansions(payload: Any) -> list[dict[str, Any]]:
        """Normalize expansions payload to a list."""
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict):
            for key in ("data", "results", "expansions"):
                value = payload.get(key)
                if isinstance(value, list):
                    return [item for item in value if isinstance(item, dict)]
        return []

    @staticmethod
    def _parse_blueprints(payload: Any) -> list[dict[str, Any]]:
        """Normalize blueprint export payload to a list."""
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict):
            for key in ("data", "results", "blueprints"):
                value = payload.get(key)
                if isinstance(value, list):
                    return [item for item in value if isinstance(item, dict)]
        return []

    @staticmethod
    def _parse_marketplace_products(payload: Any) -> dict[str, list[dict[str, Any]]]:
        """Normalize marketplace payload to {blueprint_id: [listing, ...]}."""
        raw_groups = (
            payload.get("data") if isinstance(payload, dict) and isinstance(payload.get("data"), dict) else payload
        )
        if not isinstance(raw_groups, dict):
            return {}

        result: dict[str, list[dict[str, Any]]] = {}
        for blueprint_id, value in raw_groups.items():
            listings: list[dict[str, Any]] = []
            if isinstance(value, dict):
                products = value.get("products")
                if isinstance(products, list):
                    listings = [item for item in products if isinstance(item, dict)]
            elif isinstance(value, list):
                listings = [item for item in value if isinstance(item, dict)]

            if listings:
                result[str(blueprint_id)] = listings

        return result

    @staticmethod
    def _build_blueprint_map(blueprints: list[dict[str, Any]]) -> dict[str, str]:
        """Build blueprint_id -> scryfall_id map."""
        result: dict[str, str] = {}
        for blueprint in blueprints:
            blueprint_id = blueprint.get("id")
            scryfall_id = blueprint.get("scryfall_id")
            if blueprint_id is None or not scryfall_id:
                continue
            result[str(blueprint_id)] = str(scryfall_id)
        return result

    def _build_raw_records(
        self,
        marketplace_groups: dict[str, list[dict[str, Any]]],
        blueprint_to_scryfall: dict[str, str],
    ) -> list[dict[str, Any]]:
        """Convert grouped marketplace listings to raw price records."""
        records: list[dict[str, Any]] = []

        for blueprint_id, listings in marketplace_groups.items():
            scryfall_id = blueprint_to_scryfall.get(blueprint_id)
            if not scryfall_id:
                continue

            for finish, finish_listings in self._group_listings_by_finish(listings, blueprint_id).items():
                calculation = self._calculate_listing_price_detail(finish_listings)
                if calculation is None:
                    continue

                price = calculation.price
                currency = calculation.currency
                expected_currency = self.config.expected_currency if self.config else ""
                if expected_currency and currency != expected_currency:
                    LOGGER.error(
                        "CardTrader currency mismatch for blueprint %s: expected %s, got %s",
                        blueprint_id,
                        expected_currency,
                        currency,
                    )
                    continue
                LOGGER.debug(
                    "CardTrader %s price for blueprint %s: %.2f %s from %s listings (min %.2f, max %.2f)",
                    finish,
                    blueprint_id,
                    price,
                    currency,
                    calculation.listing_count,
                    calculation.min_price,
                    calculation.max_price,
                )
                if price >= CARDTRADER_HIGH_PRICE_WARNING:
                    LOGGER.warning(
                        "CardTrader high %s price for blueprint %s: %.2f %s from %s listings (min %.2f, max %.2f)",
                        finish,
                        blueprint_id,
                        price,
                        currency,
                        calculation.listing_count,
                        calculation.min_price,
                        calculation.max_price,
                    )

                records.append(
                    {
                        "scryfallId": scryfall_id,
                        "blueprintId": blueprint_id,
                        "finish": finish,
                        "price": price,
                        "currency": currency,
                    }
                )

        return records

    def _group_listings_by_finish(
        self,
        listings: list[dict[str, Any]],
        blueprint_id: str,
    ) -> dict[str, list[dict[str, Any]]]:
        """Group products by CardTrader's MTG foil property."""
        grouped: dict[str, list[dict[str, Any]]] = {"normal": [], "foil": []}

        for listing in listings:
            mtg_foil = self._extract_mtg_foil(listing)
            if mtg_foil is None:
                LOGGER.warning(
                    "CardTrader: skipping blueprint %s listing without usable mtg_foil property",
                    blueprint_id,
                )
                continue
            grouped["foil" if mtg_foil else "normal"].append(listing)

        return grouped

    @staticmethod
    def _extract_mtg_foil(listing: dict[str, Any]) -> bool | None:
        """Extract CardTrader's MTG foil flag from a marketplace listing if present."""
        properties_hash = listing.get("properties_hash")
        if not isinstance(properties_hash, dict) or "mtg_foil" not in properties_hash:
            return None

        value = properties_hash.get("mtg_foil")
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "1", "yes"}:
                return True
            if normalized in {"false", "0", "no"}:
                return False
        return None

    @staticmethod
    def _build_identifier_records(blueprints: list[dict[str, Any]]) -> list[dict[str, str]]:
        """Convert CardTrader blueprints to card identifier records."""
        records: list[dict[str, str]] = []
        for blueprint in blueprints:
            blueprint_id = blueprint.get("id")
            scryfall_id = blueprint.get("scryfall_id")
            if blueprint_id is None or not scryfall_id:
                continue
            records.append(
                {
                    "scryfallId": str(scryfall_id),
                    "cardtraderId": str(blueprint_id),
                }
            )
        return records

    def _calculate_listing_price(self, listings: list[dict[str, Any]]) -> tuple[float, str] | None:
        """Approximate CardTrader market price from returned listings."""
        calculation = self._calculate_listing_price_detail(listings)
        if calculation is None:
            return None
        return calculation.price, calculation.currency

    def _calculate_listing_price_detail(self, listings: list[dict[str, Any]]) -> CardTraderPriceCalculation | None:
        """Approximate CardTrader market price with internal diagnostics."""
        parsed: list[tuple[int, str]] = []
        for listing in listings:
            extracted = self._extract_listing_price(listing)
            if extracted is None:
                continue
            parsed.append(extracted)

        if not parsed:
            return None

        selected = parsed[:MAX_LISTINGS_FOR_MARKET_PRICE]
        currencies = {currency for _, currency in selected}
        if len(currencies) != 1:
            LOGGER.warning("CardTrader listings mixed currencies in one listing slice: %s", sorted(currencies))
            return None

        avg_cents = sum(cents for cents, _ in selected) / len(selected)
        cents_values = [cents for cents, _ in selected]
        return CardTraderPriceCalculation(
            price=avg_cents / 100.0,
            currency=selected[0][1],
            listing_count=len(selected),
            min_price=min(cents_values) / 100.0,
            max_price=max(cents_values) / 100.0,
        )

    @staticmethod
    def _extract_listing_price(listing: dict[str, Any]) -> tuple[int, str] | None:
        """Extract cents + currency from a marketplace listing."""
        price_obj = listing.get("price")
        if isinstance(price_obj, dict):
            cents = price_obj.get("cents")
            currency = price_obj.get("currency")
            if isinstance(cents, int) and isinstance(currency, str) and currency:
                return cents, currency

        cents = listing.get("price_cents")
        currency = listing.get("currency") or listing.get("price_currency")
        if isinstance(cents, int) and isinstance(currency, str) and currency:
            return cents, currency

        return None

    @staticmethod
    def _extract_game_id(expansion: dict[str, Any]) -> int | None:
        """Extract game id from an expansion row."""
        game_id = expansion.get("game_id")
        return game_id if isinstance(game_id, int) else None

    @staticmethod
    def _extract_expansion_id(expansion: dict[str, Any]) -> int | None:
        """Extract expansion id from an expansion row."""
        expansion_id = expansion.get("id")
        return expansion_id if isinstance(expansion_id, int) else None


async def get_cardtrader_prices(
    scryfall_to_uuid_map: dict[str, str],
    on_progress: ProgressCallback | None = None,
) -> pl.DataFrame:
    """Fetch CardTrader paper prices as a DataFrame."""
    provider = CardTraderPriceProvider(on_progress=on_progress)
    return await provider.fetch_prices(scryfall_to_uuid_map)
