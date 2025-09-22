import json
import pathlib
from typing import List, TextIO
from unittest.mock import patch

from mtgjson5.classes import MtgjsonPricesObject
from mtgjson5.price_builder import PriceBuilder
from mtgjson5.providers import (
    CardKingdomProvider,
    CardHoarderProvider,
    TCGPlayerProvider,
    MultiverseBridgeProvider,
)
from mtgjson5.providers.abstract import AbstractProvider
from mtgjson5.providers.cardmarket.monolith import CardMarketProvider


def get_slim_all_printings_path() -> pathlib.Path:
    return (
        pathlib.Path(__file__)
        .resolve()
        .parent.joinpath("resources")
        .joinpath("today_price_builder")
        .joinpath("slim_all_printings.json")
    )


def get_resource_file_buffer(file_name: str) -> TextIO:
    return (
        pathlib.Path(__file__)
        .resolve()
        .parent.joinpath("resources")
        .joinpath("today_price_builder")
        .joinpath(file_name)
        .open()
    )


def assert_build_today_prices(
    provider: AbstractProvider, expected_results: List[MtgjsonPricesObject]
) -> None:
    builder = PriceBuilder(provider, all_printings_path=get_slim_all_printings_path())
    today_prices = builder.build_today_prices()

    actual_results = today_prices.values()
    assert len(expected_results) == len(actual_results)
    for expected, actual in zip(expected_results, actual_results):
        assert expected.to_json() == actual


def test_card_kingdom_build_today_prices():
    provider = CardKingdomProvider()
    patch.object(
        provider,
        "download",
        return_value=json.load(
            get_resource_file_buffer("card_kingdom_api_response.json")
        ),
    ).start()

    expected_results = [
        MtgjsonPricesObject(
            "paper",
            "cardkingdom",
            provider.today_date,
            "USD",
            111.02,
            222.02,
            None,
            111.01,
            222.01,
            None,
        ),
        MtgjsonPricesObject(
            "paper",
            "cardkingdom",
            provider.today_date,
            "USD",
            None,
            None,
            333.02,
            None,
            None,
            333.01,
        ),
    ]

    assert_build_today_prices(provider, expected_results)


def test_card_market_build_today_prices():
    provider = CardMarketProvider(init_map=False)
    patch.object(
        provider,
        "download",
        return_value=json.load(
            get_resource_file_buffer("card_market_api_response.json")
        ),
    ).start()

    expected_results = [
        MtgjsonPricesObject(
            "paper",
            "cardmarket",
            provider.today_date,
            "EUR",
            None,
            None,
            None,
            111.01,
            222.01,
            None,
        ),
        MtgjsonPricesObject(
            "paper",
            "cardmarket",
            provider.today_date,
            "EUR",
            None,
            None,
            None,
            None,
            None,
            333.01,
        ),
    ]

    assert_build_today_prices(provider, expected_results)


def test_card_hoarder_build_today_prices():
    provider = CardHoarderProvider()
    patch.object(
        provider,
        "download",
        side_effect=[
            get_resource_file_buffer("card_hoarder_non_foil_api_response.txt").read(),
            get_resource_file_buffer("card_hoarder_foil_api_response.txt").read(),
        ],
    ).start()

    expected_results = [
        MtgjsonPricesObject(
            "mtgo",
            "cardhoarder",
            provider.today_date,
            "USD",
            None,
            None,
            None,
            111.01,
            222.01,
            None,
        )
    ]

    assert_build_today_prices(provider, expected_results)


def test_tcgplayer_build_today_prices():
    provider = TCGPlayerProvider()
    patch.object(
        provider,
        "get_tcgplayer_magic_set_ids",
        return_value=json.load(
            get_resource_file_buffer("tcgplayer_magic_set_ids.json")
        ),
    ).start()
    patch.object(
        provider,
        "get_api_results",
        side_effect=[
            json.load(
                get_resource_file_buffer("tcgplayer_buylist_group_response.json")
            ),
            json.load(
                get_resource_file_buffer("tcgplayer_pricing_group_response.json")
            ),
        ],
    ).start()
    patch.object(
        provider,
        "get_tcgplayer_sku_data",
        return_value=json.load(
            get_resource_file_buffer("tcgplayer_sku_data_response.json")
        ),
    ).start()

    expected_results = [
        MtgjsonPricesObject(
            "paper",
            "tcgplayer",
            provider.today_date,
            "USD",
            111.02,
            222.02,
            None,
            111.01,
            222.01,
            None,
            # Enhanced pricing fields from test data
            sell_normal_low=0.01,
            sell_normal_mid=0.02,
            sell_normal_high=0.03,
            sell_normal_direct_low=0.04,
            sell_foil_low=0.01,
            sell_foil_mid=0.02,
            sell_foil_high=0.03,
            sell_foil_direct_low=0.04,
        ),
        MtgjsonPricesObject(
            "paper",
            "tcgplayer",
            provider.today_date,
            "USD",
            None,
            None,
            333.02,
            None,
            None,
            333.01,
            # Enhanced pricing fields for etched
            sell_etched_low=0.01,
            sell_etched_mid=0.02,
            sell_etched_high=0.03,
            sell_etched_direct_low=0.04,
        ),
    ]

    assert_build_today_prices(provider, expected_results)


def test_multiverse_bridge_cardsphere_build_today_prices():
    provider = MultiverseBridgeProvider()
    patch.object(
        provider,
        "download",
        return_value=json.load(
            get_resource_file_buffer("multiverse_bridge_prices_responses.json")
        ),
    ).start()

    expected_results = [
        MtgjsonPricesObject(
            "paper",
            "cardsphere",
            provider.today_date,
            "USD",
            None,
            None,
            None,
            111.01,
            222.01,
            None,
        )
    ]

    assert_build_today_prices(provider, expected_results)
