"""Tests for mtgjson5.providers.cardsphere.provider: CardSphereProvider."""

from __future__ import annotations

import json
import logging
from unittest.mock import patch

import polars as pl

from mtgjson5.providers.cardsphere.provider import CardSphereProvider

# -- Fixtures --

SAMPLE_RESOURCE = [
    # Regular card: nonfoil + foil
    {
        "scryfallId": "cd94f624-9b35-4468-829f-106366f61c1a",
        "cardsphereId": "90538",
        "cardsphereFoilId": "90589",
    },
    # Alternative foil only
    {
        "scryfallId": "355dcac7-120b-4c52-b077-bf36c41c579d",
        "cardsphereAlternativeFoilId": "90989",
    },
    # Card with only nonfoil
    {
        "scryfallId": "bbb-only-nonfoil",
        "cardsphereId": "2001",
    },
    # Card with only foil
    {
        "scryfallId": "ccc-only-foil",
        "cardsphereFoilId": "3002",
    },
    # Etched only
    {
        "scryfallId": "eee-only-etched",
        "cardsphereEtchedId": "5001",
    },
    # Card with all four
    {
        "scryfallId": "ddd-all-four",
        "cardsphereId": "4001",
        "cardsphereFoilId": "4002",
        "cardsphereEtchedId": "4004",
        "cardsphereAlternativeFoilId": "4003",
    },
]


# -- load() tests --


class TestLoad:
    def test_loads_from_resource_file(self, tmp_path):
        resource_file = tmp_path / "cardsphere_data.json"
        resource_file.write_text(json.dumps(SAMPLE_RESOURCE))

        provider = CardSphereProvider()
        with patch("mtgjson5.providers.cardsphere.provider.constants") as mock_constants:
            mock_constants.RESOURCE_PATH = tmp_path
            result = provider.load()

        assert isinstance(result, pl.DataFrame)
        assert result.height == 6
        assert set(result.columns) == {
            "scryfallId",
            "cardsphereId",
            "cardsphereFoilId",
            "cardsphereEtchedId",
            "cardsphereAlternativeFoilId",
        }

    def test_regular_card_ids(self, tmp_path):
        resource_file = tmp_path / "cardsphere_data.json"
        resource_file.write_text(json.dumps(SAMPLE_RESOURCE))

        provider = CardSphereProvider()
        with patch("mtgjson5.providers.cardsphere.provider.constants") as mock_constants:
            mock_constants.RESOURCE_PATH = tmp_path
            result = provider.load()

        row = result.filter(pl.col("scryfallId") == "cd94f624-9b35-4468-829f-106366f61c1a")
        assert row["cardsphereId"][0] == "90538"
        assert row["cardsphereFoilId"][0] == "90589"
        assert row["cardsphereEtchedId"][0] is None
        assert row["cardsphereAlternativeFoilId"][0] is None

    def test_alternative_foil_only_card(self, tmp_path):
        resource_file = tmp_path / "cardsphere_data.json"
        resource_file.write_text(json.dumps(SAMPLE_RESOURCE))

        provider = CardSphereProvider()
        with patch("mtgjson5.providers.cardsphere.provider.constants") as mock_constants:
            mock_constants.RESOURCE_PATH = tmp_path
            result = provider.load()

        row = result.filter(pl.col("scryfallId") == "355dcac7-120b-4c52-b077-bf36c41c579d")
        assert row["cardsphereAlternativeFoilId"][0] == "90989"
        assert row["cardsphereFoilId"][0] is None
        assert row["cardsphereEtchedId"][0] is None
        assert row["cardsphereId"][0] is None

    def test_nonfoil_only_card(self, tmp_path):
        resource_file = tmp_path / "cardsphere_data.json"
        resource_file.write_text(json.dumps(SAMPLE_RESOURCE))

        provider = CardSphereProvider()
        with patch("mtgjson5.providers.cardsphere.provider.constants") as mock_constants:
            mock_constants.RESOURCE_PATH = tmp_path
            result = provider.load()

        row = result.filter(pl.col("scryfallId") == "bbb-only-nonfoil")
        assert row["cardsphereId"][0] == "2001"
        assert row["cardsphereFoilId"][0] is None
        assert row["cardsphereEtchedId"][0] is None
        assert row["cardsphereAlternativeFoilId"][0] is None

    def test_foil_only_card(self, tmp_path):
        resource_file = tmp_path / "cardsphere_data.json"
        resource_file.write_text(json.dumps(SAMPLE_RESOURCE))

        provider = CardSphereProvider()
        with patch("mtgjson5.providers.cardsphere.provider.constants") as mock_constants:
            mock_constants.RESOURCE_PATH = tmp_path
            result = provider.load()

        row = result.filter(pl.col("scryfallId") == "ccc-only-foil")
        assert row["cardsphereId"][0] is None
        assert row["cardsphereFoilId"][0] == "3002"
        assert row["cardsphereEtchedId"][0] is None
        assert row["cardsphereAlternativeFoilId"][0] is None

    def test_etched_only_card(self, tmp_path):
        resource_file = tmp_path / "cardsphere_data.json"
        resource_file.write_text(json.dumps(SAMPLE_RESOURCE))

        provider = CardSphereProvider()
        with patch("mtgjson5.providers.cardsphere.provider.constants") as mock_constants:
            mock_constants.RESOURCE_PATH = tmp_path
            result = provider.load()

        row = result.filter(pl.col("scryfallId") == "eee-only-etched")
        assert row["cardsphereId"][0] is None
        assert row["cardsphereFoilId"][0] is None
        assert row["cardsphereEtchedId"][0] == "5001"
        assert row["cardsphereAlternativeFoilId"][0] is None

    def test_all_four_ids(self, tmp_path):
        resource_file = tmp_path / "cardsphere_data.json"
        resource_file.write_text(json.dumps(SAMPLE_RESOURCE))

        provider = CardSphereProvider()
        with patch("mtgjson5.providers.cardsphere.provider.constants") as mock_constants:
            mock_constants.RESOURCE_PATH = tmp_path
            result = provider.load()

        row = result.filter(pl.col("scryfallId") == "ddd-all-four")
        assert row["cardsphereId"][0] == "4001"
        assert row["cardsphereFoilId"][0] == "4002"
        assert row["cardsphereEtchedId"][0] == "4004"
        assert row["cardsphereAlternativeFoilId"][0] == "4003"

    def test_unique_scryfall_ids(self, tmp_path):
        resource_file = tmp_path / "cardsphere_data.json"
        resource_file.write_text(json.dumps(SAMPLE_RESOURCE))

        provider = CardSphereProvider()
        with patch("mtgjson5.providers.cardsphere.provider.constants") as mock_constants:
            mock_constants.RESOURCE_PATH = tmp_path
            result = provider.load()

        assert result["scryfallId"].n_unique() == result.height

    def test_returns_empty_when_file_missing(self, tmp_path, caplog):
        provider = CardSphereProvider()
        with patch("mtgjson5.providers.cardsphere.provider.constants") as mock_constants:
            mock_constants.RESOURCE_PATH = tmp_path
            with caplog.at_level(logging.WARNING, logger="mtgjson5.providers.cardsphere.provider"):
                result = provider.load()

        assert result.height == 0
        assert any("not found" in r.message for r in caplog.records)

    def test_returns_empty_when_file_empty(self, tmp_path, caplog):
        resource_file = tmp_path / "cardsphere_data.json"
        resource_file.write_text("[]")

        provider = CardSphereProvider()
        with patch("mtgjson5.providers.cardsphere.provider.constants") as mock_constants:
            mock_constants.RESOURCE_PATH = tmp_path
            with caplog.at_level(logging.WARNING, logger="mtgjson5.providers.cardsphere.provider"):
                result = provider.load()

        assert result.height == 0
        assert any("empty" in r.message for r in caplog.records)

    def test_cards_df_property(self, tmp_path):
        resource_file = tmp_path / "cardsphere_data.json"
        resource_file.write_text(json.dumps(SAMPLE_RESOURCE))

        provider = CardSphereProvider()
        assert provider.cards_df is None

        with patch("mtgjson5.providers.cardsphere.provider.constants") as mock_constants:
            mock_constants.RESOURCE_PATH = tmp_path
            provider.load()

        assert provider.cards_df is not None
        assert provider.cards_df.height == 6
