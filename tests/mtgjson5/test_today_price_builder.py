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
