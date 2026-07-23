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
            request_retry_base_delay=0,
        )
        self.output_path = None
        self.responses = responses
        self.seen_user_agents: list[str | None] = []

    async def _request_json_once(
        self,
        session: aiohttp.ClientSession,
        path: str,
        params: dict[str, str | int] | None = None,
    ) -> Any:
        self.seen_user_agents.append(session.headers.get("User-Agent"))
        key = (path, tuple(sorted((params or {}).items())))
        response = self.responses[key]
        if isinstance(response, tuple):
            next_response = response[0]
            self.responses[key] = response[1:] if len(response) > 2 else response[1]
            response = next_response
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
        ("marketplace/products", (("expansion_id", 10), ("language", "en"))): {
            "101": [
                {"price": {"cents": 200, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
                {"price": {"cents": 400, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
                {"price": {"cents": 600, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
            ],
            "102": [
                {"price": {"cents": 500, "currency": "EUR"}, "properties_hash": {"mtg_foil": True}},
                {"price": {"cents": 700, "currency": "EUR"}, "properties_hash": {"mtg_foil": True}},
                {"price": {"cents": 900, "currency": "EUR"}, "properties_hash": {"mtg_foil": True}},
            ],
            "103": [
                {"price": {"cents": 999, "currency": "EUR"}, "properties_hash": {"mtg_foil": True}},
                {"price": {"cents": 1099, "currency": "EUR"}, "properties_hash": {"mtg_foil": True}},
                {"price": {"cents": 1199, "currency": "EUR"}, "properties_hash": {"mtg_foil": True}},
            ],
        },
    }

    provider = StubCardTraderProvider(responses)
    df = asyncio.run(provider.fetch_raw_prices())

    assert isinstance(df, pl.DataFrame)
    assert len(df) == 3
    assert sorted(df.to_dicts(), key=lambda row: (row["finish"], row["scryfallId"])) == [
        {"scryfallId": "sf-foil", "blueprintId": "102", "finish": "foil", "price": 7.0, "currency": "EUR"},
        {"scryfallId": "sf-unmapped", "blueprintId": "103", "finish": "foil", "price": 10.99, "currency": "EUR"},
        {"scryfallId": "sf-normal", "blueprintId": "101", "finish": "normal", "price": 4.0, "currency": "EUR"},
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
        ("marketplace/products", (("expansion_id", 10), ("language", "en"))): {
            "101": [
                {"price": {"cents": 200, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
                {"price": {"cents": 400, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
                {"price": {"cents": 600, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
            ],
            "102": [
                {"price": {"cents": 500, "currency": "EUR"}, "properties_hash": {"mtg_foil": True}},
                {"price": {"cents": 700, "currency": "EUR"}, "properties_hash": {"mtg_foil": True}},
                {"price": {"cents": 900, "currency": "EUR"}, "properties_hash": {"mtg_foil": True}},
            ],
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
            "price": 7.0,
            "currency": "EUR",
        },
        {
            "uuid": "uuid-normal",
            "date": provider.today_date,
            "source": "paper",
            "provider": "cardtrader",
            "price_type": "retail",
            "finish": "normal",
            "price": 4.0,
            "currency": "EUR",
        },
    ]
    assert provider.seen_user_agents
    assert all(user_agent == "MTGJSON/5.0 (https://mtgjson.com)" for user_agent in provider.seen_user_agents)


def test_cardtrader_generate_today_price_dict_maps_to_legacy_price_entries() -> None:
    responses = {
        ("expansions", ()): [
            {"id": 10, "game_id": 1, "name": "Test Expansion"},
        ],
        ("blueprints/export", (("expansion_id", 10),)): [
            {"id": 101, "scryfall_id": "sf-normal"},
            {"id": 102, "scryfall_id": "sf-foil"},
        ],
        ("marketplace/products", (("expansion_id", 10), ("language", "en"))): {
            "101": [
                {"price": {"cents": 200, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
                {"price": {"cents": 400, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
                {"price": {"cents": 600, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
            ],
            "102": [
                {"price": {"cents": 500, "currency": "EUR"}, "properties_hash": {"mtg_foil": True}},
                {"price": {"cents": 700, "currency": "EUR"}, "properties_hash": {"mtg_foil": True}},
                {"price": {"cents": 900, "currency": "EUR"}, "properties_hash": {"mtg_foil": True}},
            ],
        },
    }

    provider = StubCardTraderProvider(responses)
    result = asyncio.run(
        provider.generate_today_price_dict(
            {
                "sf-normal": "uuid-normal",
                "sf-foil": "uuid-foil",
            }
        )
    )

    assert sorted(result) == ["uuid-foil", "uuid-normal"]

    normal_entry = result["uuid-normal"]
    assert normal_entry.source == "paper"
    assert normal_entry.provider == "cardtrader"
    assert normal_entry.date == provider.today_date
    assert normal_entry.currency == "EUR"
    assert normal_entry.sell_normal == 4.0
    assert normal_entry.sell_foil is None

    foil_entry = result["uuid-foil"]
    assert foil_entry.source == "paper"
    assert foil_entry.provider == "cardtrader"
    assert foil_entry.date == provider.today_date
    assert foil_entry.currency == "EUR"
    assert foil_entry.sell_foil == 7.0
    assert foil_entry.sell_normal is None


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
    assert provider.seen_user_agents
    assert all(user_agent == "MTGJSON/5.0 (https://mtgjson.com)" for user_agent in provider.seen_user_agents)


def test_cardtrader_returns_empty_frame_without_config() -> None:
    provider = CardTraderPriceProvider(config=None)
    provider.config = None
    provider.output_path = None

    df = asyncio.run(provider.fetch_prices({"sfid": "uuid"}))

    assert df.is_empty()


def test_cardtrader_generate_today_price_dict_returns_empty_dict_without_config() -> None:
    provider = CardTraderPriceProvider(config=None)
    provider.config = None
    provider.output_path = None

    result = asyncio.run(provider.generate_today_price_dict({"sfid": "uuid"}))

    assert result == {}


def test_cardtrader_skips_unexpected_currency() -> None:
    provider = CardTraderPriceProvider(config=CardTraderConfig(auth_token="token", expected_currency="EUR"))

    records = provider._build_raw_records(
        {
            "101": [
                {"price": {"cents": 250, "currency": "USD"}, "properties_hash": {"mtg_foil": False}},
            ]
        },
        {"101": "sf-normal"},
    )

    assert not records


def test_cardtrader_skips_listings_without_usable_finish_flag() -> None:
    provider = CardTraderPriceProvider(config=CardTraderConfig(auth_token="token", expected_currency="EUR"))

    records = provider._build_raw_records(
        {
            "101": [
                {"price": {"cents": 200, "currency": "EUR"}, "properties_hash": {}},
                {"price": {"cents": 400, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
                {"price": {"cents": 600, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
                {"price": {"cents": 800, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
            ]
        },
        {"101": "sf-normal"},
    )

    assert records == [
        {"scryfallId": "sf-normal", "blueprintId": "101", "finish": "normal", "price": 6.0, "currency": "EUR"}
    ]


def test_cardtrader_requires_at_least_three_eligible_listings() -> None:
    provider = CardTraderPriceProvider(config=CardTraderConfig(auth_token="token", expected_currency="EUR"))

    records = provider._build_raw_records(
        {
            "101": [
                {"price": {"cents": 200, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
                {"price": {"cents": 400, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
            ]
        },
        {"101": "sf-normal"},
    )

    assert not records


def test_cardtrader_trims_prices_above_ten_times_median() -> None:
    provider = CardTraderPriceProvider(config=CardTraderConfig(auth_token="token", expected_currency="EUR"))

    records = provider._build_raw_records(
        {
            "101": [
                {"price": {"cents": 100, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
                {"price": {"cents": 110, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
                {"price": {"cents": 120, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
                {"price": {"cents": 100000, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
            ]
        },
        {"101": "sf-normal"},
    )

    assert records == [
        {"scryfallId": "sf-normal", "blueprintId": "101", "finish": "normal", "price": 1.1, "currency": "EUR"}
    ]


def test_cardtrader_skips_when_trimming_leaves_too_few_prices() -> None:
    provider = CardTraderPriceProvider(config=CardTraderConfig(auth_token="token", expected_currency="EUR"))

    records = provider._build_raw_records(
        {
            "101": [
                {"price": {"cents": 100, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
                {"price": {"cents": 110, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
                {"price": {"cents": 20000, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
            ]
        },
        {"101": "sf-normal"},
    )

    assert not records


def test_cardtrader_excludes_special_listings() -> None:
    provider = CardTraderPriceProvider(config=CardTraderConfig(auth_token="token", expected_currency="EUR"))

    records = provider._build_raw_records(
        {
            "101": [
                {"price": {"cents": 10000, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}, "graded": True},
                {
                    "price": {"cents": 10000, "currency": "EUR"},
                    "properties_hash": {"mtg_foil": False, "signed": True},
                },
                {"price": {"cents": 100, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
                {"price": {"cents": 110, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
                {"price": {"cents": 120, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
            ]
        },
        {"101": "sf-normal"},
    )

    assert records == [
        {"scryfallId": "sf-normal", "blueprintId": "101", "finish": "normal", "price": 1.1, "currency": "EUR"}
    ]


def test_cardtrader_retries_retryable_marketplace_errors() -> None:
    retry_error = aiohttp.ClientResponseError(
        request_info=None,
        history=(),
        status=429,
        message="Too Many Requests",
        headers={},
    )
    responses = {
        ("expansions", ()): [
            {"id": 10, "game_id": 1, "name": "Test Expansion"},
        ],
        ("blueprints/export", (("expansion_id", 10),)): [
            {"id": 101, "scryfall_id": "sf-normal"},
        ],
        ("marketplace/products", (("expansion_id", 10), ("language", "en"))): (
            retry_error,
            {
                "101": [
                    {"price": {"cents": 250, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
                    {"price": {"cents": 350, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
                    {"price": {"cents": 450, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
                ]
            },
        ),
    }

    provider = StubCardTraderProvider(responses)
    df = asyncio.run(provider.fetch_raw_prices())

    assert df.to_dicts() == [
        {"scryfallId": "sf-normal", "blueprintId": "101", "finish": "normal", "price": 3.5, "currency": "EUR"}
    ]


def test_cardtrader_fallback_verifies_low_count_finish_groups() -> None:
    responses = {
        ("expansions", ()): [
            {"id": 10, "game_id": 1, "name": "Test Expansion"},
        ],
        ("blueprints/export", (("expansion_id", 10),)): [
            {"id": 101, "scryfall_id": "sf-normal"},
        ],
        ("marketplace/products", (("expansion_id", 10), ("language", "en"))): {
            "101": [
                {"price": {"cents": 250, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
            ]
        },
        ("marketplace/products", (("blueprint_id", "101"), ("foil", "false"), ("language", "en"))): {
            "101": [
                {"price": {"cents": 250, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
                {"price": {"cents": 350, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
                {"price": {"cents": 450, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
            ]
        },
    }

    provider = StubCardTraderProvider(responses)
    df = asyncio.run(provider.fetch_raw_prices())

    assert df.to_dicts() == [
        {"scryfallId": "sf-normal", "blueprintId": "101", "finish": "normal", "price": 3.5, "currency": "EUR"}
    ]


def test_cardtrader_fallback_verifies_suspicious_spread() -> None:
    responses = {
        ("expansions", ()): [
            {"id": 10, "game_id": 1, "name": "Test Expansion"},
        ],
        ("blueprints/export", (("expansion_id", 10),)): [
            {"id": 101, "scryfall_id": "sf-normal"},
        ],
        ("marketplace/products", (("expansion_id", 10), ("language", "en"))): {
            "101": [
                {"price": {"cents": 100, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
                {"price": {"cents": 110, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
                {"price": {"cents": 120, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
                {"price": {"cents": 100000, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
            ]
        },
        ("marketplace/products", (("blueprint_id", "101"), ("foil", "false"), ("language", "en"))): {
            "101": [
                {"price": {"cents": 100, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
                {"price": {"cents": 110, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
                {"price": {"cents": 120, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
                {"price": {"cents": 130, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
            ]
        },
    }

    provider = StubCardTraderProvider(responses)
    df = asyncio.run(provider.fetch_raw_prices())

    assert df.to_dicts() == [
        {"scryfallId": "sf-normal", "blueprintId": "101", "finish": "normal", "price": 1.15, "currency": "EUR"}
    ]


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
        ("marketplace/products", (("expansion_id", 11), ("language", "en"))): {
            "201": [
                {"price": {"cents": 350, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
                {"price": {"cents": 450, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
                {"price": {"cents": 550, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
            ]
        },
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
            "price": 4.5,
            "currency": "EUR",
        }
    ]


def test_cardtrader_skips_failed_marketplace_expansion_and_keeps_other_results() -> None:
    responses = {
        ("expansions", ()): [
            {"id": 10, "game_id": 1, "name": "Broken Expansion"},
            {"id": 11, "game_id": 1, "name": "Working Expansion"},
        ],
        ("blueprints/export", (("expansion_id", 10),)): [
            {"id": 201, "scryfall_id": "sf-broken"},
        ],
        ("marketplace/products", (("expansion_id", 10), ("language", "en"))): aiohttp.ClientError("broken"),
        ("blueprints/export", (("expansion_id", 11),)): [
            {"id": 202, "scryfall_id": "sf-working"},
        ],
        ("marketplace/products", (("expansion_id", 11), ("language", "en"))): {
            "202": [
                {"price": {"cents": 350, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
                {"price": {"cents": 450, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
                {"price": {"cents": 550, "currency": "EUR"}, "properties_hash": {"mtg_foil": False}},
            ]
        },
    }

    provider = StubCardTraderProvider(responses)
    df = asyncio.run(provider.fetch_prices({"sf-broken": "uuid-broken", "sf-working": "uuid-working"}))

    assert df.to_dicts() == [
        {
            "uuid": "uuid-working",
            "date": provider.today_date,
            "source": "paper",
            "provider": "cardtrader",
            "price_type": "retail",
            "finish": "normal",
            "price": 4.5,
            "currency": "EUR",
        }
    ]
