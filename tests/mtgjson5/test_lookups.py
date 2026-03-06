"""Tests for v2 pipeline lookups: add_meld_other_face_ids, apply_meld_overrides."""

from __future__ import annotations

import polars as pl

from mtgjson5.pipeline.lookups import add_meld_other_face_ids, apply_meld_overrides

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_meld_triplet_lf(
    set_code: str = "BRO",
    lang: str = "en",
    front1_name: str = "Urza, Lord Protector",
    front2_name: str = "The Mightstone and Weakstone",
    result_name: str = "Urza, Planeswalker",
    front1_num: str = "225",
    front2_num: str = "238a",
    result_num: str = "238b",
    front1_uuid: str = "uuid-front1",
    front2_uuid: str = "uuid-front2",
    result_uuid: str = "uuid-result",
) -> pl.LazyFrame:
    """Build a 3-card LazyFrame representing a meld triplet."""
    card_parts = [front1_name, front2_name, result_name]
    rows = [
        {
            "setCode": set_code,
            "language": lang,
            "number": front1_num,
            "faceName": front1_name,
            "uuid": front1_uuid,
            "cardParts": card_parts,
            "otherFaceIds": None,
        },
        {
            "setCode": set_code,
            "language": lang,
            "number": front2_num,
            "faceName": front2_name,
            "uuid": front2_uuid,
            "cardParts": card_parts,
            "otherFaceIds": None,
        },
        {
            "setCode": set_code,
            "language": lang,
            "number": result_num,
            "faceName": result_name,
            "uuid": result_uuid,
            "cardParts": card_parts,
            "otherFaceIds": None,
        },
    ]
    return pl.LazyFrame(
        rows,
        schema={
            "setCode": pl.String,
            "language": pl.String,
            "number": pl.String,
            "faceName": pl.String,
            "uuid": pl.String,
            "cardParts": pl.List(pl.String),
            "otherFaceIds": pl.List(pl.String),
        },
    )


# ---------------------------------------------------------------------------
# TestAddMeldOtherFaceIds
# ---------------------------------------------------------------------------


class TestAddMeldOtherFaceIds:
    def test_standard_meld_triplet(self):
        lf = _make_meld_triplet_lf()
        result = add_meld_other_face_ids(lf).collect()

        # Front faces should point to result
        front1 = result.filter(pl.col("faceName") == "Urza, Lord Protector")
        front2 = result.filter(pl.col("faceName") == "The Mightstone and Weakstone")
        meld_result = result.filter(pl.col("faceName") == "Urza, Planeswalker")

        assert front1["otherFaceIds"][0].to_list() == ["uuid-result"]
        assert front2["otherFaceIds"][0].to_list() == ["uuid-result"]
        # Result should point to both fronts
        result_face_ids = sorted(meld_result["otherFaceIds"][0].to_list())
        assert result_face_ids == ["uuid-front1", "uuid-front2"]

    def test_non_meld_cards_unchanged(self):
        lf = pl.LazyFrame(
            {
                "setCode": ["TST"],
                "language": ["en"],
                "number": ["1"],
                "faceName": ["Normal Card"],
                "uuid": ["uuid-normal"],
                "cardParts": [None],
                "otherFaceIds": [["existing-id"]],
            },
            schema={
                "setCode": pl.String,
                "language": pl.String,
                "number": pl.String,
                "faceName": pl.String,
                "uuid": pl.String,
                "cardParts": pl.List(pl.String),
                "otherFaceIds": pl.List(pl.String),
            },
        )
        result = add_meld_other_face_ids(lf).collect()
        assert result["otherFaceIds"][0].to_list() == ["existing-id"]

    def test_cards_without_card_parts_passthrough(self):
        lf = pl.LazyFrame(
            {
                "setCode": ["TST"],
                "language": ["en"],
                "number": ["1"],
                "faceName": ["Test Card"],
                "uuid": ["uuid-1"],
                "cardParts": [None],
                "otherFaceIds": [None],
            },
            schema={
                "setCode": pl.String,
                "language": pl.String,
                "number": pl.String,
                "faceName": pl.String,
                "uuid": pl.String,
                "cardParts": pl.List(pl.String),
                "otherFaceIds": pl.List(pl.String),
            },
        )
        result = add_meld_other_face_ids(lf).collect()
        assert result["otherFaceIds"][0] is None


# ---------------------------------------------------------------------------
# TestApplyMeldOverrides
# ---------------------------------------------------------------------------


class TestApplyMeldOverrides:
    def _base_lf(self):
        return pl.LazyFrame(
            {
                "uuid": ["uuid-1", "uuid-2"],
                "otherFaceIds": [["old-1"], ["old-2"]],
                "cardParts": [["p1", "p2", "p3"], None],
            },
            schema={
                "uuid": pl.String,
                "otherFaceIds": pl.List(pl.String),
                "cardParts": pl.List(pl.String),
            },
        )

    def test_empty_overrides_passthrough(self):
        lf = self._base_lf()
        result = apply_meld_overrides(lf, {}).collect()
        assert result.equals(lf.collect())

    def test_override_other_face_ids(self):
        lf = self._base_lf()
        overrides = {"uuid-1": {"otherFaceIds": ["new-a", "new-b"]}}
        result = apply_meld_overrides(lf, overrides).collect()
        row = result.filter(pl.col("uuid") == "uuid-1")
        assert row["otherFaceIds"][0].to_list() == ["new-a", "new-b"]

    def test_override_card_parts(self):
        lf = self._base_lf()
        overrides = {"uuid-1": {"cardParts": ["x", "y", "z"]}}
        result = apply_meld_overrides(lf, overrides).collect()
        row = result.filter(pl.col("uuid") == "uuid-1")
        assert row["cardParts"][0].to_list() == ["x", "y", "z"]

    def test_uuid_not_in_frame_no_error(self):
        lf = self._base_lf()
        overrides = {"uuid-missing": {"otherFaceIds": ["a"]}}
        result = apply_meld_overrides(lf, overrides).collect()
        # Should not crash, original data preserved
        assert len(result) == 2
        assert result.filter(pl.col("uuid") == "uuid-1")["otherFaceIds"][0].to_list() == ["old-1"]
