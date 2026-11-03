"""Tests for the TDM reversible adventure dragon fixes in the pipeline core."""

from __future__ import annotations

from typing import Any

import polars as pl

from mtgjson5.pipeline.core import _apply_tdm_name_fix, _apply_tdm_omen_face_fix

_FACE_SCHEMA = pl.Struct(
    {
        "name": pl.String,
        "mana_cost": pl.String,
        "oracle_text": pl.String,
        "power": pl.String,
        "toughness": pl.String,
        "oracle_id": pl.String,
        "layout": pl.String,
        "cmc": pl.Float64,
    }
)

_SCHEMA = {
    "_set_upper": pl.String,
    "lang": pl.String,
    "name": pl.String,
    "layout": pl.String,
    "oracleId": pl.String,
    "collectorNumber": pl.String,
    "cardFaces": pl.List(_FACE_SCHEMA),
}

ORACLE_ID = "da1e019c-2ffb-412d-90d7-f2e5e5c44c4b"
CREATURE_TEXT = "Flying\nWhenever this creature or another Dragon you control enters, you gain 3 life."
OMEN_TEXT = "Search your library for up to two basic Forest cards."


def _face(**overrides: Any) -> dict[str, Any]:
    face: dict[str, Any] = dict.fromkeys(_FACE_SCHEMA.to_schema())
    face.update(overrides)
    return face


def _creature_face(**overrides: Any) -> dict[str, Any]:
    return _face(
        name="Bloomvine Regent",
        mana_cost="{3}{G}{G}",
        oracle_text=CREATURE_TEXT,
        power="4",
        toughness="5",
        **overrides,
    )


def _adventure_row(lang: str = "en") -> dict[str, Any]:
    return {
        "_set_upper": "TDM",
        "lang": lang,
        "name": "Bloomvine Regent // Claim Territory",
        "layout": "adventure",
        "oracleId": ORACLE_ID,
        "collectorNumber": "136",
        "cardFaces": [
            _creature_face(),
            _face(name="Claim Territory", mana_cost="{2}{G}", oracle_text=OMEN_TEXT),
        ],
    }


def _reversible_row(set_upper: str = "TDM") -> dict[str, Any]:
    reversible = {"oracle_id": ORACLE_ID, "layout": "adventure", "cmc": 5.0}
    return {
        "_set_upper": set_upper,
        "lang": "en",
        "name": "Bloomvine Regent // Claim Territory // Bloomvine Regent // Claim Territory",
        "layout": "reversible_card",
        "oracleId": None,
        "collectorNumber": "381",
        "cardFaces": [
            _creature_face(**reversible),
            # Scryfall copies the creature's text and P/T onto the omen face
            _face(
                name="Claim Territory",
                mana_cost="{2}{G}",
                oracle_text=CREATURE_TEXT,
                power="4",
                toughness="5",
                **reversible,
            ),
        ],
    }


def _faces_for(df: pl.DataFrame, number: str) -> list[dict[str, Any]]:
    return df.filter(pl.col("collectorNumber") == number)["cardFaces"][0].to_list()


class TestTdmNameFix:
    def test_triple_slash_name_gets_fourth_part(self):
        lf = pl.LazyFrame({"_set_upper": ["TDM"], "name": ["Bloomvine Regent // Claim Territory // Bloomvine Regent"]})
        result = _apply_tdm_name_fix(lf).collect()
        assert result["name"][0] == "Bloomvine Regent // Claim Territory // Bloomvine Regent // Claim Territory"

    def test_other_sets_untouched(self):
        lf = pl.LazyFrame({"_set_upper": ["SLD"], "name": ["A // B // A"]})
        result = _apply_tdm_name_fix(lf).collect()
        assert result["name"][0] == "A // B // A"


class TestTdmOmenFaceFix:
    def test_omen_face_takes_text_from_adventure_printing(self):
        lf = pl.LazyFrame([_adventure_row(), _adventure_row(lang="fr"), _reversible_row()], schema=_SCHEMA)
        result = _apply_tdm_omen_face_fix(lf).collect()

        creature, omen = _faces_for(result, "381")
        assert omen["name"] == "Claim Territory"
        assert omen["oracle_text"] == OMEN_TEXT
        assert omen["power"] is None
        assert omen["toughness"] is None
        assert omen["mana_cost"] == "{2}{G}"
        assert omen["cmc"] == 5.0
        assert creature == _creature_face(oracle_id=ORACLE_ID, layout="adventure", cmc=5.0)

    def test_preserves_rows_and_columns(self):
        rows = [_reversible_row(), _adventure_row(), _adventure_row(lang="fr")]
        lf = pl.LazyFrame(rows, schema=_SCHEMA)
        result = _apply_tdm_omen_face_fix(lf).collect()

        assert result.columns == list(_SCHEMA)
        assert result.schema == pl.Schema(_SCHEMA)
        assert result["collectorNumber"].to_list() == ["381", "136", "136"]
        assert _faces_for(result, "136") == rows[1]["cardFaces"]

    def test_no_adventure_printing_leaves_faces_alone(self):
        row = _reversible_row()
        result = _apply_tdm_omen_face_fix(pl.LazyFrame([row], schema=_SCHEMA)).collect()
        assert _faces_for(result, "381") == row["cardFaces"]

    def test_other_sets_untouched(self):
        adventure = _adventure_row()
        adventure["_set_upper"] = "SLD"
        row = _reversible_row(set_upper="SLD")
        result = _apply_tdm_omen_face_fix(pl.LazyFrame([adventure, row], schema=_SCHEMA)).collect()
        assert _faces_for(result, "381") == row["cardFaces"]
