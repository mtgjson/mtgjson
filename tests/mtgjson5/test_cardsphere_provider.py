"""Tests for mtgjson5.providers.cardsphere.provider: CardSphereProvider."""

from __future__ import annotations

import json
import logging
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
import requests

from mtgjson5.providers.cardsphere.provider import CardSphereProvider, _NextDataParser

# -- Fixtures --

NEXT_DATA_HTML = """
<html>
<head>
<script id="__NEXT_DATA__" type="application/json">{next_data}</script>
</head>
<body></body>
</html>
"""


def _make_next_data_html(sets_list: list[dict]) -> str:
    next_data = json.dumps(
        {
            "props": {
                "pageProps": {
                    "sets": sets_list,
                }
            }
        }
    )
    return NEXT_DATA_HTML.format(next_data=next_data)


SAMPLE_SETS = [
    {"id": 100, "name": "Alpha", "code": "LEA"},
    {"id": 200, "name": "Beta", "code": "LEB"},
    {"id": 300, "name": "Strixhaven Mystical Archive", "code": "STA"},
]


@pytest.fixture
def raw_csv_df() -> pl.DataFrame:
    """Raw CSV DataFrame mimicking CardSphere's 2X2 data.

    CardSphere only uses N and F for foil column. Etched cards get a
    different scryfallId from Scryfall (separate printing).
    """
    return pl.DataFrame(
        {
            "scryfallId": [
                # Regular card: nonfoil + foil
                "cd94f624-9b35-4468-829f-106366f61c1a",
                "cd94f624-9b35-4468-829f-106366f61c1a",
                # Etched variant (different scryfall ID, only F in CS)
                "355dcac7-120b-4c52-b077-bf36c41c579d",
                # Card with only nonfoil
                "bbb-only-nonfoil",
                # Card with only foil
                "ccc-only-foil",
            ],
            "cardsphereId": [
                "90538",
                "90589",
                "90989",
                "2001",
                "3002",
            ],
            "foil": [
                "N",
                "F",
                "F",
                "N",
                "F",
            ],
        }
    )


@pytest.fixture
def finishes_df() -> pl.DataFrame:
    """Scryfall finishes data for the cards in raw_csv_df."""
    return pl.DataFrame(
        {
            "scryfallId": [
                "cd94f624-9b35-4468-829f-106366f61c1a",
                "355dcac7-120b-4c52-b077-bf36c41c579d",
                "bbb-only-nonfoil",
                "ccc-only-foil",
            ],
            "finishes": [
                ["nonfoil", "foil"],
                ["etched"],
                ["nonfoil"],
                ["foil"],
            ],
        }
    )


@pytest.fixture
def raw_csv_df_dupes() -> pl.DataFrame:
    """DataFrame with duplicate scryfallIds per foil type."""
    return pl.DataFrame(
        {
            "scryfallId": ["aaa-111", "aaa-111", "aaa-111"],
            "cardsphereId": ["1001", "1099", "1002"],
            "foil": ["N", "N", "F"],
        }
    )


# -- _parse_sets_from_html tests --


class TestParseSetsFromHtml:
    def test_extracts_sets_from_next_data(self):
        html = _make_next_data_html(SAMPLE_SETS)
        result = CardSphereProvider._parse_sets_from_html(html)
        assert len(result) == 3
        assert result[0]["code"] == "LEA"
        assert result[2]["id"] == 300

    def test_returns_empty_when_no_script_tag(self):
        html = "<html><body>No data here</body></html>"
        result = CardSphereProvider._parse_sets_from_html(html)
        assert result == []

    def test_returns_empty_when_json_invalid(self):
        html = '<html><script id="__NEXT_DATA__">not json{</script></html>'
        result = CardSphereProvider._parse_sets_from_html(html)
        assert result == []

    def test_returns_empty_when_no_sets_key(self):
        next_data = json.dumps({"props": {"pageProps": {"other": "data"}}})
        html = f'<html><script id="__NEXT_DATA__">{next_data}</script></html>'
        result = CardSphereProvider._parse_sets_from_html(html)
        assert result == []

    def test_handles_data_nested_sets(self):
        next_data = json.dumps({"props": {"pageProps": {"data": {"sets": SAMPLE_SETS[:1]}}}})
        html = f'<html><script id="__NEXT_DATA__">{next_data}</script></html>'
        result = CardSphereProvider._parse_sets_from_html(html)
        assert len(result) == 1
        assert result[0]["code"] == "LEA"


# -- _NextDataParser tests --


class TestNextDataParser:
    def test_extracts_content(self):
        parser = _NextDataParser()
        parser.feed('<script id="__NEXT_DATA__">{"key": "value"}</script>')
        assert parser.content == '{"key": "value"}'

    def test_ignores_other_scripts(self):
        parser = _NextDataParser()
        parser.feed('<script id="other">not this</script>')
        assert parser.content is None

    def test_ignores_body_text(self):
        parser = _NextDataParser()
        parser.feed('<div>body text</div><script id="__NEXT_DATA__">{"a":1}</script>')
        assert parser.content == '{"a":1}'


# -- _pivot_to_card_mapping tests --


class TestPivotToCardMapping:
    def test_foil_card_with_finishes(self, raw_csv_df: pl.DataFrame, finishes_df: pl.DataFrame):
        """Regular foil card (finishes=["nonfoil","foil"]) gets cardsphereFoilId."""
        result = CardSphereProvider._pivot_to_card_mapping(raw_csv_df, finishes_df)
        row = result.filter(pl.col("scryfallId") == "cd94f624-9b35-4468-829f-106366f61c1a")
        assert row["cardsphereId"][0] == "90538"
        assert row["cardsphereFoilId"][0] == "90589"
        assert row["cardsphereEtchedId"][0] is None

    def test_etched_card_with_finishes(self, raw_csv_df: pl.DataFrame, finishes_df: pl.DataFrame):
        """Etched card (finishes=["etched"], no "foil") gets cardsphereEtchedId."""
        result = CardSphereProvider._pivot_to_card_mapping(raw_csv_df, finishes_df)
        row = result.filter(pl.col("scryfallId") == "355dcac7-120b-4c52-b077-bf36c41c579d")
        assert row["cardsphereEtchedId"][0] == "90989"
        assert row["cardsphereFoilId"][0] is None
        assert row["cardsphereId"][0] is None

    def test_nonfoil_only_card(self, raw_csv_df: pl.DataFrame, finishes_df: pl.DataFrame):
        result = CardSphereProvider._pivot_to_card_mapping(raw_csv_df, finishes_df)
        row = result.filter(pl.col("scryfallId") == "bbb-only-nonfoil")
        assert row["cardsphereId"][0] == "2001"
        assert row["cardsphereFoilId"][0] is None
        assert row["cardsphereEtchedId"][0] is None

    def test_foil_only_card(self, raw_csv_df: pl.DataFrame, finishes_df: pl.DataFrame):
        result = CardSphereProvider._pivot_to_card_mapping(raw_csv_df, finishes_df)
        row = result.filter(pl.col("scryfallId") == "ccc-only-foil")
        assert row["cardsphereId"][0] is None
        assert row["cardsphereFoilId"][0] == "3002"
        assert row["cardsphereEtchedId"][0] is None

    def test_unique_scryfall_ids(self, raw_csv_df: pl.DataFrame, finishes_df: pl.DataFrame):
        result = CardSphereProvider._pivot_to_card_mapping(raw_csv_df, finishes_df)
        assert result["scryfallId"].n_unique() == result.height

    def test_without_finishes_defaults_to_foil(self, raw_csv_df: pl.DataFrame):
        """Without finishes data, all F rows default to cardsphereFoilId."""
        result = CardSphereProvider._pivot_to_card_mapping(raw_csv_df, None)
        # The etched card's F row goes to cardsphereFoilId since we can't distinguish
        row = result.filter(pl.col("scryfallId") == "355dcac7-120b-4c52-b077-bf36c41c579d")
        assert row["cardsphereFoilId"][0] == "90989"
        assert row["cardsphereEtchedId"][0] is None

    def test_keeps_first_on_duplicates(self, raw_csv_df_dupes: pl.DataFrame):
        result = CardSphereProvider._pivot_to_card_mapping(raw_csv_df_dupes, None)
        assert result.height == 1
        assert result["cardsphereId"][0] == "1001"
        assert result["cardsphereFoilId"][0] == "1002"

    def test_empty_input(self):
        empty = pl.DataFrame(schema={"scryfallId": pl.String, "cardsphereId": pl.String, "foil": pl.String})
        result = CardSphereProvider._pivot_to_card_mapping(empty, None)
        assert result.height == 0
        assert set(result.columns) == {"scryfallId", "cardsphereId", "cardsphereFoilId", "cardsphereEtchedId"}

    def test_etched_and_foil_finishes_routes_to_foil(self):
        """If finishes has BOTH foil and etched, F goes to cardsphereFoilId."""
        raw = pl.DataFrame(
            {
                "scryfallId": ["dual-finish", "dual-finish"],
                "cardsphereId": ["5001", "5002"],
                "foil": ["N", "F"],
            }
        )
        finishes = pl.DataFrame(
            {
                "scryfallId": ["dual-finish"],
                "finishes": [["nonfoil", "foil", "etched"]],
            }
        )
        result = CardSphereProvider._pivot_to_card_mapping(raw, finishes)
        row = result.filter(pl.col("scryfallId") == "dual-finish")
        # Has "foil" in finishes, so F -> cardsphereFoilId
        assert row["cardsphereFoilId"][0] == "5002"
        assert row["cardsphereEtchedId"][0] is None


# -- _download_csv tests --

SAMPLE_CSV = b'Tradelist Count,Name,Edition,Condition,Language,Foil,Cardsphere ID,Scryfall ID\n"0","Lightning Bolt","Alpha","NM","EN","N","1001","aaa-111"\n"0","Lightning Bolt","Alpha","NM","EN","F","1002","aaa-111"\n'


class TestDownloadCsv:
    def test_parses_csv_to_dataframe(self):
        mock_response = MagicMock()
        mock_response.content = SAMPLE_CSV

        mock_session = MagicMock()
        mock_session.get.return_value = mock_response

        result = CardSphereProvider._download_csv(mock_session, 100)
        assert isinstance(result, pl.DataFrame)
        assert result.height == 2
        assert set(result.columns) == {"scryfallId", "cardsphereId", "foil"}
        assert result["scryfallId"][0] == "aaa-111"
        assert result["cardsphereId"][0] == "1001"

    def test_filters_empty_scryfall_id(self):
        csv_data = b'Tradelist Count,Name,Edition,Condition,Language,Foil,Cardsphere ID,Scryfall ID\n"0","Bad","Set","NM","EN","N","9999",""\n"0","Good","Set","NM","EN","N","1001","valid-id"\n'
        mock_response = MagicMock()
        mock_response.content = csv_data

        mock_session = MagicMock()
        mock_session.get.return_value = mock_response

        result = CardSphereProvider._download_csv(mock_session, 100)
        assert result.height == 1
        assert result["scryfallId"][0] == "valid-id"

    def test_raises_on_http_error(self):
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("404")

        mock_session = MagicMock()
        mock_session.get.return_value = mock_response

        with pytest.raises(requests.HTTPError):
            CardSphereProvider._download_csv(mock_session, 999)


# -- fetch_and_build retry and logging tests --


class TestFetchAndBuildRetries:
    @patch.object(CardSphereProvider, "_fetch_sets_index")
    @patch.object(CardSphereProvider, "_download_csv")
    @patch("mtgjson5.providers.cardsphere.provider._build_session")
    def test_retries_failed_downloads(self, mock_session_factory, mock_download, mock_sets_index):
        """Failed set downloads are retried up to 3 times."""
        mock_session_factory.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_session_factory.return_value.__exit__ = MagicMock(return_value=False)

        mock_sets_index.return_value = [
            {"id": 1, "code": "SET", "name": "Test Set"},
        ]

        # Fail twice, succeed on third attempt
        call_count = {"n": 0}

        def side_effect(_session, _set_id):
            call_count["n"] += 1
            if call_count["n"] <= 2:
                raise requests.ReadTimeout("timed out")
            return pl.DataFrame(
                {
                    "scryfallId": ["abc-123"],
                    "cardsphereId": ["5001"],
                    "foil": ["N"],
                }
            )

        mock_download.side_effect = side_effect

        provider = CardSphereProvider()
        cards_df, _sets_df = provider.fetch_and_build()

        assert call_count["n"] == 3
        assert cards_df.height == 1
        assert cards_df["cardsphereId"][0] == "5001"

    @patch.object(CardSphereProvider, "_fetch_sets_index")
    @patch.object(CardSphereProvider, "_download_csv")
    @patch("mtgjson5.providers.cardsphere.provider._build_session")
    def test_logs_warning_after_all_retries_exhausted(
        self, mock_session_factory, mock_download, mock_sets_index, caplog
    ):
        """Logs a WARNING with failed set_ids after all retry attempts."""
        mock_session_factory.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_session_factory.return_value.__exit__ = MagicMock(return_value=False)

        mock_sets_index.return_value = [
            {"id": 42, "code": "FAI", "name": "Fail Set"},
            {"id": 99, "code": "FA2", "name": "Fail Set 2"},
        ]
        mock_download.side_effect = requests.ReadTimeout("timed out")

        provider = CardSphereProvider()
        with caplog.at_level(logging.WARNING, logger="mtgjson5.providers.cardsphere.provider"):
            cards_df, _ = provider.fetch_and_build()

        assert cards_df.height == 0
        warning_messages = [r.message for r in caplog.records if r.levelno == logging.WARNING]
        assert any("Failed to download 2/" in msg for msg in warning_messages)
        assert any("42" in msg and "99" in msg for msg in warning_messages)

    @patch.object(CardSphereProvider, "_fetch_sets_index")
    @patch.object(CardSphereProvider, "_download_csv")
    @patch("mtgjson5.providers.cardsphere.provider._build_session")
    def test_logs_retry_info(self, mock_session_factory, mock_download, mock_sets_index, caplog):
        """Logs an INFO message when retrying failed sets."""
        mock_session_factory.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_session_factory.return_value.__exit__ = MagicMock(return_value=False)

        mock_sets_index.return_value = [
            {"id": 1, "code": "SET", "name": "Test Set"},
        ]

        call_count = {"n": 0}

        def side_effect(_session, _set_id):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise requests.ReadTimeout("timed out")
            return pl.DataFrame({"scryfallId": ["x"], "cardsphereId": ["1"], "foil": ["N"]})

        mock_download.side_effect = side_effect

        provider = CardSphereProvider()
        with caplog.at_level(logging.INFO, logger="mtgjson5.providers.cardsphere.provider"):
            provider.fetch_and_build()

        info_messages = [r.message for r in caplog.records if r.levelno == logging.INFO]
        assert any("Retrying 1 failed CSVs (attempt 2/3)" in msg for msg in info_messages)

    @patch.object(CardSphereProvider, "_fetch_sets_index")
    def test_returns_empty_frames_when_sets_index_fails(self, mock_sets_index):
        """Returns empty DataFrames when set index fetch fails."""
        mock_sets_index.return_value = []

        provider = CardSphereProvider()
        cards_df, sets_df = provider.fetch_and_build()

        assert cards_df.height == 0
        assert sets_df.height == 0

    @patch.object(CardSphereProvider, "_fetch_sets_index")
    @patch.object(CardSphereProvider, "_download_csv")
    @patch("mtgjson5.providers.cardsphere.provider._build_session")
    def test_partial_failures_still_return_successful_data(self, mock_session_factory, mock_download, mock_sets_index):
        """Successful downloads are kept even when some sets fail."""
        mock_session_factory.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_session_factory.return_value.__exit__ = MagicMock(return_value=False)

        mock_sets_index.return_value = [
            {"id": 1, "code": "OK1", "name": "Good Set"},
            {"id": 2, "code": "BAD", "name": "Bad Set"},
        ]

        def side_effect(_session, set_id):
            if set_id == 2:
                raise requests.ReadTimeout("timed out")
            return pl.DataFrame({"scryfallId": ["good-id"], "cardsphereId": ["100"], "foil": ["N"]})

        mock_download.side_effect = side_effect

        provider = CardSphereProvider()
        cards_df, sets_df = provider.fetch_and_build()

        assert cards_df.height == 1
        assert cards_df["scryfallId"][0] == "good-id"
        assert sets_df.height == 2


# -- _fetch_sets_index logging tests --


class TestFetchSetsIndexLogging:
    @patch("mtgjson5.providers.cardsphere.provider.requests.get")
    def test_logs_error_on_network_failure(self, mock_get, caplog):
        """Logs ERROR when the sets page request fails."""
        mock_get.side_effect = requests.ConnectionError("DNS resolution failed")

        provider = CardSphereProvider()
        with caplog.at_level(logging.ERROR, logger="mtgjson5.providers.cardsphere.provider"):
            result = provider._fetch_sets_index()

        assert result == []
        assert any("Failed to fetch CardSphere sets page" in r.message for r in caplog.records)

    @patch("mtgjson5.providers.cardsphere.provider.requests.get")
    def test_logs_error_on_missing_next_data(self, mock_get, caplog):
        """Logs ERROR when __NEXT_DATA__ script tag is missing from response."""
        mock_response = MagicMock()
        mock_response.text = "<html><body>No data</body></html>"
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        provider = CardSphereProvider()
        with caplog.at_level(logging.ERROR, logger="mtgjson5.providers.cardsphere.provider"):
            result = provider._fetch_sets_index()

        assert result == []
        assert any("Could not find __NEXT_DATA__" in r.message for r in caplog.records)
