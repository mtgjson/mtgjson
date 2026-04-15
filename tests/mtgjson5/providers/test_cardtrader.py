"""Tests for the CardTrader price provider."""

import asyncio
from typing import Any, TypeAlias

import aiohttp
import polars as pl

from mtgjson5.providers.cardtrader import CardTraderConfig, CardTraderPriceProvider

ResponseKey: TypeAlias = tuple[str, tuple[tuple[str, str | int], ...]]
ResponseMap: TypeAlias = dict[ResponseKey, Any]


class StubCardTraderProvider(CardTraderPriceProvider):
    def __init__(self, responses: ResponseMap) -> None:
        super().__init__(
            config=CardTraderConfig(auth_token="token", expected_currency="EUR"),
            market_request_delay=0,
        )
        self.output_path = None
        self.responses = responses

    async def _request_json(
        self,
        session: aiohttp.ClientSession,
        path: str,
        params: dict[str, str | int] | None = None,
    ) -> Any:
        key = (path, tuple(sorted((params or {}).items())))
        response = self.responses[key]
        if isinstance(response, Exception):
            raise response
        return response


def test_cardtrader_averages_the_first_15_listings() -> None:
    provider = CardTraderPriceProvider(config=CardTraderConfig(auth_token="token"))

    listings = [
        {"price": {"cents": 100, "currency": "EUR"}},
        {"price": {"cents": 120, "currency": "EUR"}},
        {"price": {"cents": 300, "currency": "EUR"}},
        {"price": {"cents": 330, "currency": "EUR"}},
        {"price": {"cents": 360, "currency": "EUR"}},
    ]

    result = provider._calculate_listing_price(listings)

    assert result is not None
    price, currency = result
    assert currency == "EUR"
    assert price == 2.42


def test_cardtrader_caps_the_average_at_the_first_15_listings() -> None:
    provider = CardTraderPriceProvider(config=CardTraderConfig(auth_token="token"))

    listings = [{"price": {"cents": cents, "currency": "EUR"}} for cents in range(100, 1800, 100)]

    result = provider._calculate_listing_price(listings)

    assert result is not None
    price, currency = result
    assert currency == "EUR"
    assert price == 8.0


def test_cardtrader_fetch_raw_prices_builds_normal_and_foil_rows() -> None:
    responses = {
        ("expansions", ()): [
            {"id": 10, "game_id": 1, "name": "Test Expansion"},
            {"id": 11, "game_id": 2, "name": "Other Game"},
        ],
        ("blueprints/export", (("expansion_id", 10),)): [
            {"id": 101, "scryfall_id": "sf-normal"},
            {"id": 102, "scryfall_id": "sf-foil"},
            {"id": 103, "scryfall_id": "sf-unmapped"},
        ],
        ("marketplace/products", (("expansion_id", 10), ("foil", "false"), ("language", "en"))): {
            "101": {
                "products": [
                    {"price": {"cents": 200, "currency": "EUR"}},
                    {"price": {"cents": 400, "currency": "EUR"}},
                ]
            }
        },
        ("marketplace/products", (("expansion_id", 10), ("foil", "true"), ("language", "en"))): {
            "102": {
                "products": [
                    {"price": {"cents": 500, "currency": "EUR"}},
                    {"price": {"cents": 700, "currency": "EUR"}},
                ]
            },
            "103": {
                "products": [
                    {"price": {"cents": 999, "currency": "EUR"}},
                ]
            },
        },
    }

    provider = StubCardTraderProvider(responses)
    df = asyncio.run(provider.fetch_raw_prices())

    assert isinstance(df, pl.DataFrame)
    assert len(df) == 3
    assert sorted(df.to_dicts(), key=lambda row: (row["finish"], row["scryfallId"])) == [
        {"scryfallId": "sf-foil", "blueprintId": "102", "finish": "foil", "price": 6.0, "currency": "EUR"},
        {"scryfallId": "sf-unmapped", "blueprintId": "103", "finish": "foil", "price": 9.99, "currency": "EUR"},
        {"scryfallId": "sf-normal", "blueprintId": "101", "finish": "normal", "price": 3.0, "currency": "EUR"},
    ]


def test_cardtrader_fetch_prices_maps_to_uuid() -> None:
    responses = {
        ("expansions", ()): [
            {"id": 10, "game_id": 1, "name": "Test Expansion"},
        ],
        ("blueprints/export", (("expansion_id", 10),)): [
            {"id": 101, "scryfall_id": "sf-normal"},
            {"id": 102, "scryfall_id": "sf-foil"},
        ],
        ("marketplace/products", (("expansion_id", 10), ("foil", "false"), ("language", "en"))): {
            "101": {
                "products": [
                    {"price": {"cents": 200, "currency": "EUR"}},
                    {"price": {"cents": 400, "currency": "EUR"}},
                ]
            }
        },
        ("marketplace/products", (("expansion_id", 10), ("foil", "true"), ("language", "en"))): {
            "102": {
                "products": [
                    {"price": {"cents": 500, "currency": "EUR"}},
                    {"price": {"cents": 700, "currency": "EUR"}},
                ]
            }
        },
    }

    provider = StubCardTraderProvider(responses)
    df = asyncio.run(
        provider.fetch_prices(
            {
                "sf-normal": "uuid-normal",
                "sf-foil": "uuid-foil",
            }
        )
    )

    assert isinstance(df, pl.DataFrame)
    assert len(df) == 2

    rows = sorted(df.iter_rows(named=True), key=lambda row: row["finish"])
    assert rows == [
        {
            "uuid": "uuid-foil",
            "date": provider.today_date,
            "source": "paper",
            "provider": "cardtrader",
            "price_type": "retail",
            "finish": "foil",
            "price": 6.0,
            "currency": "EUR",
        },
        {
            "uuid": "uuid-normal",
            "date": provider.today_date,
            "source": "paper",
            "provider": "cardtrader",
            "price_type": "retail",
            "finish": "normal",
            "price": 3.0,
            "currency": "EUR",
        },
    ]


def test_cardtrader_fetch_identifier_lookup_builds_scryfall_to_blueprint_rows() -> None:
    responses = {
        ("expansions", ()): [
            {"id": 10, "game_id": 1, "name": "Test Expansion"},
            {"id": 11, "game_id": 2, "name": "Other Game"},
        ],
        ("blueprints/export", (("expansion_id", 10),)): [
            {"id": 101, "scryfall_id": "sf-normal"},
            {"id": 102, "scryfall_id": "sf-foil"},
            {"id": 103, "scryfall_id": None},
        ],
    }

    provider = StubCardTraderProvider(responses)
    df = asyncio.run(provider.fetch_identifier_lookup())

    assert isinstance(df, pl.DataFrame)
    assert sorted(df.to_dicts(), key=lambda row: row["scryfallId"]) == [
        {"scryfallId": "sf-foil", "cardtraderId": "102"},
        {"scryfallId": "sf-normal", "cardtraderId": "101"},
    ]


def test_cardtrader_returns_empty_frame_without_config() -> None:
    provider = CardTraderPriceProvider(config=None)
    provider.config = None
    provider.output_path = None

    df = asyncio.run(provider.fetch_prices({"sfid": "uuid"}))

    assert df.is_empty()


def test_cardtrader_skips_unexpected_currency() -> None:
    provider = CardTraderPriceProvider(config=CardTraderConfig(auth_token="token", expected_currency="EUR"))

    records = provider._build_raw_records(
        {
            "101": [
                {"price": {"cents": 250, "currency": "USD"}},
            ]
        },
        {"101": "sf-normal"},
        finish="normal",
    )

    assert not records


def test_cardtrader_returns_empty_frame_when_expansions_fetch_fails() -> None:
    provider = StubCardTraderProvider(
        {
            ("expansions", ()): aiohttp.ClientError("401 Unauthorized"),
        }
    )

    df = asyncio.run(provider.fetch_prices({"sfid": "uuid"}))

    assert isinstance(df, pl.DataFrame)
    assert df.is_empty()


def test_cardtrader_skips_failed_expansion_and_keeps_other_results() -> None:
    responses = {
        ("expansions", ()): [
            {"id": 10, "game_id": 1, "name": "Broken Expansion"},
            {"id": 11, "game_id": 1, "name": "Working Expansion"},
        ],
        ("blueprints/export", (("expansion_id", 10),)): aiohttp.ClientError("429 Too Many Requests"),
        ("blueprints/export", (("expansion_id", 11),)): [
            {"id": 201, "scryfall_id": "sf-working"},
        ],
        ("marketplace/products", (("expansion_id", 11), ("foil", "false"), ("language", "en"))): {
            "201": {
                "products": [
                    {"price": {"cents": 350, "currency": "EUR"}},
                ]
            }
        },
        ("marketplace/products", (("expansion_id", 11), ("foil", "true"), ("language", "en"))): {},
    }

    provider = StubCardTraderProvider(responses)
    df = asyncio.run(provider.fetch_prices({"sf-working": "uuid-working"}))

    assert isinstance(df, pl.DataFrame)
    assert len(df) == 1
    assert df.to_dicts() == [
        {
            "uuid": "uuid-working",
            "date": provider.today_date,
            "source": "paper",
            "provider": "cardtrader",
            "price_type": "retail",
            "finish": "normal",
            "price": 3.5,
            "currency": "EUR",
        }
    ]
