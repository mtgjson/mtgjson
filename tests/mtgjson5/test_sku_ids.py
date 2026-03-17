"""Tests for skuIds generation (add_sku_ids pipeline stage)."""

from __future__ import annotations

import uuid

import polars as pl

from mtgjson5.pipeline.stages.identifiers import add_sku_ids

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ALL_FINISHES = ["nonfoil", "foil", "etched", "signed", "other"]


def _expected_sku(card_uuid: str, finish: str, language: str) -> str:
    """Compute the expected skuId for a given (uuid, finish, language) triple."""
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{card_uuid}_{finish}_{language}"))


def _make_lf(
    rows: list[dict],
    *,
    with_foreign_data: bool = False,
) -> pl.LazyFrame:
    """Build a minimal LazyFrame suitable for add_sku_ids."""
    fd_struct = pl.Struct(
        {
            "faceName": pl.String,
            "flavorText": pl.String,
            "identifiers": pl.Struct({"multiverseId": pl.String, "scryfallId": pl.String}),
            "language": pl.String,
            "multiverseId": pl.Int64,
            "name": pl.String,
            "text": pl.String,
            "type": pl.String,
            "uuid": pl.String,
        }
    )
    schema: dict[str, pl.DataType] = {
        "uuid": pl.String,
        "finishes": pl.List(pl.String),
        "language": pl.String,
    }
    if with_foreign_data:
        schema["foreignData"] = pl.List(fd_struct)
    return pl.LazyFrame(rows, schema=schema)


def _fd_entry(language: str, name: str = "card") -> dict:
    """Build a minimal foreignData struct dict."""
    return {
        "faceName": None,
        "flavorText": None,
        "identifiers": {"multiverseId": None, "scryfallId": f"sf-{language.lower()[:2]}"},
        "language": language,
        "multiverseId": None,
        "name": name,
        "text": None,
        "type": None,
        "uuid": f"fd-uuid-{language.lower()[:2]}",
    }


# ---------------------------------------------------------------------------
# Top-level skuIds
# ---------------------------------------------------------------------------


class TestTopLevelSkuIds:
    """Tests for the top-level skuIds struct on each card."""

    def test_single_finish(self) -> None:
        lf = _make_lf([{"uuid": "card-1", "finishes": ["nonfoil"], "language": "English"}])
        result = add_sku_ids(lf).collect()
        sku = result["skuIds"][0]

        assert sku["nonfoil"] == _expected_sku("card-1", "nonfoil", "English")
        assert sku["foil"] is None
        assert sku["etched"] is None
        assert sku["signed"] is None
        assert sku["other"] is None

    def test_multiple_finishes(self) -> None:
        lf = _make_lf([{"uuid": "card-2", "finishes": ["nonfoil", "foil", "etched"], "language": "English"}])
        result = add_sku_ids(lf).collect()
        sku = result["skuIds"][0]

        assert sku["nonfoil"] == _expected_sku("card-2", "nonfoil", "English")
        assert sku["foil"] == _expected_sku("card-2", "foil", "English")
        assert sku["etched"] == _expected_sku("card-2", "etched", "English")
        assert sku["signed"] is None
        assert sku["other"] is None

    def test_all_finishes(self) -> None:
        lf = _make_lf([{"uuid": "card-3", "finishes": ALL_FINISHES, "language": "English"}])
        result = add_sku_ids(lf).collect()
        sku = result["skuIds"][0]

        for f in ALL_FINISHES:
            assert sku[f] == _expected_sku("card-3", f, "English")

    def test_language_affects_hash(self) -> None:
        """Same uuid + finish but different language must produce different skuIds."""
        lf = _make_lf(
            [
                {"uuid": "card-4", "finishes": ["nonfoil"], "language": "English"},
                {"uuid": "card-4", "finishes": ["nonfoil"], "language": "Japanese"},
            ]
        )
        result = add_sku_ids(lf).collect()
        en_sku = result["skuIds"][0]["nonfoil"]
        ja_sku = result["skuIds"][1]["nonfoil"]

        assert en_sku == _expected_sku("card-4", "nonfoil", "English")
        assert ja_sku == _expected_sku("card-4", "nonfoil", "Japanese")
        assert en_sku != ja_sku

    def test_deterministic(self) -> None:
        """Running add_sku_ids twice on the same data produces identical results."""
        lf = _make_lf([{"uuid": "card-5", "finishes": ["nonfoil", "foil"], "language": "English"}])
        r1 = add_sku_ids(lf).collect()["skuIds"][0]
        r2 = add_sku_ids(lf).collect()["skuIds"][0]
        assert r1 == r2

    def test_empty_finishes(self) -> None:
        """A card with no finishes gets all-null skuIds."""
        lf = _make_lf([{"uuid": "card-6", "finishes": [], "language": "English"}])
        result = add_sku_ids(lf).collect()
        sku = result["skuIds"][0]

        for f in ALL_FINISHES:
            assert sku[f] is None

    def test_different_uuids_differ(self) -> None:
        """Different card uuids produce different skuIds even with same finish and language."""
        lf = _make_lf(
            [
                {"uuid": "card-a", "finishes": ["foil"], "language": "English"},
                {"uuid": "card-b", "finishes": ["foil"], "language": "English"},
            ]
        )
        result = add_sku_ids(lf).collect()
        assert result["skuIds"][0]["foil"] != result["skuIds"][1]["foil"]

    def test_multiple_cards(self) -> None:
        """Processing multiple cards in one batch."""
        lf = _make_lf(
            [
                {"uuid": "u1", "finishes": ["nonfoil"], "language": "English"},
                {"uuid": "u2", "finishes": ["foil", "etched"], "language": "French"},
                {"uuid": "u3", "finishes": ["nonfoil", "foil"], "language": "English"},
            ]
        )
        result = add_sku_ids(lf).collect()

        assert result["skuIds"][0]["nonfoil"] == _expected_sku("u1", "nonfoil", "English")
        assert result["skuIds"][0]["foil"] is None

        assert result["skuIds"][1]["nonfoil"] is None
        assert result["skuIds"][1]["foil"] == _expected_sku("u2", "foil", "French")
        assert result["skuIds"][1]["etched"] == _expected_sku("u2", "etched", "French")

        assert result["skuIds"][2]["nonfoil"] == _expected_sku("u3", "nonfoil", "English")
        assert result["skuIds"][2]["foil"] == _expected_sku("u3", "foil", "English")

    def test_no_foreign_data_column(self) -> None:
        """Works when foreignData column is absent (e.g. tokens)."""
        lf = _make_lf(
            [{"uuid": "token-1", "finishes": ["nonfoil"], "language": "English"}],
            with_foreign_data=False,
        )
        result = add_sku_ids(lf).collect()
        assert result["skuIds"][0]["nonfoil"] == _expected_sku("token-1", "nonfoil", "English")
        assert "foreignData" not in result.columns


# ---------------------------------------------------------------------------
# foreignData skuIds
# ---------------------------------------------------------------------------


class TestForeignDataSkuIds:
    """Tests for skuIds injected into each foreignData entry."""

    def test_single_foreign_entry(self) -> None:
        lf = _make_lf(
            [
                {
                    "uuid": "card-fd1",
                    "finishes": ["nonfoil", "foil"],
                    "language": "English",
                    "foreignData": [_fd_entry("French")],
                }
            ],
            with_foreign_data=True,
        )
        result = add_sku_ids(lf).collect()
        fd = result["foreignData"][0]

        assert len(fd) == 1
        fd_sku = fd[0]["skuIds"]
        assert fd_sku["nonfoil"] == _expected_sku("card-fd1", "nonfoil", "French")
        assert fd_sku["foil"] == _expected_sku("card-fd1", "foil", "French")
        assert fd_sku["etched"] is None

    def test_multiple_foreign_entries(self) -> None:
        lf = _make_lf(
            [
                {
                    "uuid": "card-fd2",
                    "finishes": ["nonfoil"],
                    "language": "English",
                    "foreignData": [_fd_entry("French"), _fd_entry("Japanese"), _fd_entry("German")],
                }
            ],
            with_foreign_data=True,
        )
        result = add_sku_ids(lf).collect()
        fd_list = result["foreignData"][0]

        # Should be sorted by language
        languages = [entry["language"] for entry in fd_list]
        assert languages == sorted(languages)

        for entry in fd_list:
            lang = entry["language"]
            assert entry["skuIds"]["nonfoil"] == _expected_sku("card-fd2", "nonfoil", lang)
            assert entry["skuIds"]["foil"] is None  # not in finishes

    def test_foreign_uses_card_uuid_not_fd_uuid(self) -> None:
        """foreignData skuIds must be derived from the card's uuid, not the foreignData entry's uuid."""
        card_uuid = "card-fd3"
        lf = _make_lf(
            [
                {
                    "uuid": card_uuid,
                    "finishes": ["foil"],
                    "language": "English",
                    "foreignData": [_fd_entry("Spanish")],
                }
            ],
            with_foreign_data=True,
        )
        result = add_sku_ids(lf).collect()
        fd_sku = result["foreignData"][0][0]["skuIds"]

        # Must use card_uuid, not the foreignData entry's own uuid
        assert fd_sku["foil"] == _expected_sku(card_uuid, "foil", "Spanish")
        assert fd_sku["foil"] != _expected_sku("fd-uuid-sp", "foil", "Spanish")

    def test_top_level_and_foreign_differ(self) -> None:
        """Top-level and foreignData skuIds for the same finish must differ (different language)."""
        lf = _make_lf(
            [
                {
                    "uuid": "card-fd4",
                    "finishes": ["nonfoil"],
                    "language": "English",
                    "foreignData": [_fd_entry("French")],
                }
            ],
            with_foreign_data=True,
        )
        result = add_sku_ids(lf).collect()
        top_sku = result["skuIds"][0]["nonfoil"]
        fd_sku = result["foreignData"][0][0]["skuIds"]["nonfoil"]

        assert top_sku != fd_sku
        assert top_sku == _expected_sku("card-fd4", "nonfoil", "English")
        assert fd_sku == _expected_sku("card-fd4", "nonfoil", "French")

    def test_null_foreign_data(self) -> None:
        """Cards with null foreignData get an empty list and no errors."""
        lf = _make_lf(
            [
                {
                    "uuid": "card-fd5",
                    "finishes": ["nonfoil"],
                    "language": "English",
                    "foreignData": None,
                }
            ],
            with_foreign_data=True,
        )
        result = add_sku_ids(lf).collect()
        assert result["skuIds"][0]["nonfoil"] == _expected_sku("card-fd5", "nonfoil", "English")
        assert len(result["foreignData"][0]) == 0

    def test_empty_foreign_data_list(self) -> None:
        """Cards with empty foreignData list keep it empty."""
        lf = _make_lf(
            [
                {
                    "uuid": "card-fd6",
                    "finishes": ["foil"],
                    "language": "English",
                    "foreignData": [],
                }
            ],
            with_foreign_data=True,
        )
        result = add_sku_ids(lf).collect()
        assert result["skuIds"][0]["foil"] == _expected_sku("card-fd6", "foil", "English")
        assert len(result["foreignData"][0]) == 0

    def test_foreign_data_fields_preserved(self) -> None:
        """All existing foreignData fields are preserved after skuIds injection."""
        fd = _fd_entry("Korean", name="번개 화살")
        fd["flavorText"] = "맛있는 텍스트"
        fd["text"] = "카드 텍스트"
        fd["type"] = "순간마법"
        lf = _make_lf(
            [
                {
                    "uuid": "card-fd7",
                    "finishes": ["nonfoil"],
                    "language": "English",
                    "foreignData": [fd],
                }
            ],
            with_foreign_data=True,
        )
        result = add_sku_ids(lf).collect()
        entry = result["foreignData"][0][0]

        assert entry["language"] == "Korean"
        assert entry["name"] == "번개 화살"
        assert entry["flavorText"] == "맛있는 텍스트"
        assert entry["text"] == "카드 텍스트"
        assert entry["type"] == "순간마법"
        assert entry["uuid"] == "fd-uuid-ko"
        assert entry["skuIds"]["nonfoil"] is not None

    def test_mixed_cards_with_and_without_foreign_data(self) -> None:
        """Batch with a mix of cards: some with foreignData, some without."""
        lf = _make_lf(
            [
                {
                    "uuid": "card-a",
                    "finishes": ["nonfoil", "foil"],
                    "language": "English",
                    "foreignData": [_fd_entry("French"), _fd_entry("Japanese")],
                },
                {
                    "uuid": "card-b",
                    "finishes": ["foil"],
                    "language": "English",
                    "foreignData": None,
                },
                {
                    "uuid": "card-c",
                    "finishes": ["nonfoil"],
                    "language": "English",
                    "foreignData": [_fd_entry("Spanish")],
                },
            ],
            with_foreign_data=True,
        )
        result = add_sku_ids(lf).collect()

        # card-a: top-level + 2 foreignData entries
        assert result["skuIds"][0]["nonfoil"] == _expected_sku("card-a", "nonfoil", "English")
        assert len(result["foreignData"][0]) == 2

        # card-b: top-level only, empty foreignData
        assert result["skuIds"][1]["foil"] == _expected_sku("card-b", "foil", "English")
        assert len(result["foreignData"][1]) == 0

        # card-c: top-level + 1 foreignData entry
        assert result["skuIds"][2]["nonfoil"] == _expected_sku("card-c", "nonfoil", "English")
        assert len(result["foreignData"][2]) == 1
        assert result["foreignData"][2][0]["skuIds"]["nonfoil"] == _expected_sku("card-c", "nonfoil", "Spanish")
