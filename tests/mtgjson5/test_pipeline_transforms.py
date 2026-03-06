"""Tests for v2 pipeline core transformation functions."""

from __future__ import annotations

from typing import Any

import polars as pl
import pytest

from mtgjson5.data.context import PipelineContext
from mtgjson5.models.scryfall.models import CardFace
from mtgjson5.pipeline.stages.basic_fields import (
    add_mana_info,
    format_planeswalker_text,
    parse_type_line_expr,
)
from mtgjson5.pipeline.stages.derived import add_is_timeshifted
from mtgjson5.pipeline.stages.explode import (
    assign_meld_sides,
    detect_aftermath_layout,
    explode_card_faces,
)



# ---------------------------------------------------------------------------
# Helpers (duplicated from conftest to avoid relative imports)
# ---------------------------------------------------------------------------


def make_face_struct(**overrides: Any) -> dict[str, Any]:
    """Build a dict matching CardFace.polars_schema() struct fields."""
    defaults: dict[str, Any] = {
        "object": "card_face",
        "name": "Test Face",
        "mana_cost": "{1}{W}",
        "type_line": "Creature — Human",
        "oracle_text": "Test text",
        "colors": ["W"],
        "color_indicator": None,
        "power": "2",
        "toughness": "2",
        "defense": None,
        "loyalty": None,
        "flavor_text": None,
        "flavor_name": None,
        "illustration_id": None,
        "image_uris": None,
        "artist": "Test Artist",
        "artist_id": None,
        "watermark": None,
        "printed_name": None,
        "printed_text": None,
        "printed_type_line": None,
        "cmc": None,
        "oracle_id": None,
        "layout": None,
    }
    defaults.update(overrides)
    return defaults


def _make_card_row(**overrides: Any) -> dict[str, Any]:
    defaults: dict[str, Any] = {
        "name": "Test Card",
        "setCode": "TST",
        "number": "1",
        "layout": "normal",
        "uuid": "test-uuid-001",
        "scryfallId": "sf-001",
        "side": None,
        "faceId": 0,
        "manaCost": "{1}{W}",
        "type": "Creature — Human Warrior",
        "text": "Test card text.",
        "colors": ["W"],
        "colorIdentity": ["W"],
        "finishes": ["nonfoil"],
        "rarity": "common",
        "lang": "en",
        "language": "en",
        "manaValue": 2.0,
        "promoTypes": None,
        "frameEffects": None,
        "keywords": None,
        "power": None,
        "toughness": None,
        "boosterTypes": None,
        "cardParts": None,
        "faceName": None,
        "otherFaceIds": None,
        "_face_data": None,
        "_row_id": 0,
    }
    defaults.update(overrides)
    return defaults


def make_card_lf(rows: list[dict[str, Any]] | None = None) -> pl.LazyFrame:
    """Wraps make_card_row defaults + explicit schema -> pl.LazyFrame."""
    if rows is None:
        rows = [_make_card_row()]
    face_struct_schema = CardFace.polars_schema()
    full_rows = [_make_card_row(**r) for r in rows]
    return pl.LazyFrame(
        full_rows,
        schema={
            "name": pl.String,
            "setCode": pl.String,
            "number": pl.String,
            "layout": pl.String,
            "uuid": pl.String,
            "scryfallId": pl.String,
            "side": pl.String,
            "faceId": pl.Int64,
            "manaCost": pl.String,
            "type": pl.String,
            "text": pl.String,
            "colors": pl.List(pl.String),
            "colorIdentity": pl.List(pl.String),
            "finishes": pl.List(pl.String),
            "rarity": pl.String,
            "lang": pl.String,
            "manaValue": pl.Float64,
            "_face_data": face_struct_schema,
            "_row_id": pl.UInt32,
        },
    )


# ---------------------------------------------------------------------------
# Helper to build a multi-face LazyFrame with cardFaces column
# ---------------------------------------------------------------------------


def _make_multiface_lf(card_faces_list: list[list[dict]], **base_overrides: Any) -> pl.LazyFrame:
    """Build a LazyFrame with cardFaces as list of structs."""
    face_struct = CardFace.polars_schema()
    rows = []
    for i, faces in enumerate(card_faces_list):
        row = {
            "name": base_overrides.get("name", f"Card {i}"),
            "setCode": "TST",
            "number": str(i + 1),
            "layout": base_overrides.get("layout", "transform"),
            "uuid": f"uuid-{i}",
            "scryfallId": f"sf-{i}",
            "manaCost": "{1}{W}",
            "type": "Creature",
            "text": "text",
            "colors": ["W"],
            "colorIdentity": ["W"],
            "finishes": ["nonfoil"],
            "rarity": "rare",
            "lang": "en",
            "manaValue": 2.0,
            "cardFaces": faces,
        }
        row.update({k: v for k, v in base_overrides.items() if k not in ("name", "layout")})
        rows.append(row)

    schema: dict[str, Any] = {
        "name": pl.String,
        "setCode": pl.String,
        "number": pl.String,
        "layout": pl.String,
        "uuid": pl.String,
        "scryfallId": pl.String,
        "manaCost": pl.String,
        "type": pl.String,
        "text": pl.String,
        "colors": pl.List(pl.String),
        "colorIdentity": pl.List(pl.String),
        "finishes": pl.List(pl.String),
        "rarity": pl.String,
        "lang": pl.String,
        "manaValue": pl.Float64,
        "cardFaces": pl.List(face_struct),
    }
    return pl.LazyFrame(rows, schema=schema)


# ---------------------------------------------------------------------------
# TestExplodeCardFaces
# ---------------------------------------------------------------------------


class TestExplodeCardFaces:
    def test_single_face_no_cardfaces_column(self):
        """No cardFaces column -> faceId=0, side=None, _face_data=None."""
        lf = make_card_lf([{"name": "Lightning Bolt"}])
        # drop _face_data and _row_id so we test the "no cardFaces" fallback
        lf = lf.drop(["_face_data", "_row_id"])
        result = explode_card_faces(lf).collect()
        assert len(result) == 1
        assert result["faceId"][0] == 0
        assert result["side"][0] is None
        assert result["_face_data"][0] is None

    def test_dual_face_card(self):
        """cardFaces with 2 structs -> 2 rows."""
        face_a = make_face_struct(name="Front")
        face_b = make_face_struct(name="Back", mana_cost="", oracle_text="Back text")
        lf = _make_multiface_lf([[face_a, face_b]])
        result = explode_card_faces(lf).collect()
        assert len(result) == 2
        assert result["faceId"].to_list() == [0, 1]
        assert result["side"].to_list() == ["a", "b"]
        # _face_data should be populated
        assert result["_face_data"][0] is not None
        assert result["_face_data"][1] is not None

    def test_triple_face_card(self):
        """3 faces -> sides a/b/c."""
        faces = [
            make_face_struct(name="A"),
            make_face_struct(name="B"),
            make_face_struct(name="C"),
        ]
        lf = _make_multiface_lf([faces])
        result = explode_card_faces(lf).collect()
        assert len(result) == 3
        assert result["side"].to_list() == ["a", "b", "c"]

    def test_row_id_preserved(self):
        """_row_id is set as sequential index."""
        faces = [make_face_struct(name="Front"), make_face_struct(name="Back")]
        lf = _make_multiface_lf([faces, faces])
        result = explode_card_faces(lf).collect()
        # Each original card gets a unique _row_id
        row_ids = result["_row_id"].to_list()
        # Card 0 has _row_id 0 for both faces, Card 1 has _row_id 1
        assert row_ids == [0, 0, 1, 1]


# ---------------------------------------------------------------------------
# TestAssignMeldSides
# ---------------------------------------------------------------------------


class TestAssignMeldSides:
    def test_meld_parts_get_side_a(self, meld_ctx: PipelineContext):
        lf = make_card_lf(
            [
                {"name": "Urza, Lord Protector", "layout": "meld", "side": None},
                {"name": "The Mightstone and Weakstone", "layout": "meld", "side": None, "_row_id": 1},
            ]
        )
        result = assign_meld_sides(lf, meld_ctx).collect()
        sides = result["side"].to_list()
        assert sides == ["a", "a"]

    def test_meld_result_gets_side_b(self, meld_ctx: PipelineContext):
        lf = make_card_lf(
            [
                {"name": "Urza, Planeswalker", "layout": "meld", "side": None},
            ]
        )
        result = assign_meld_sides(lf, meld_ctx).collect()
        assert result["side"][0] == "b"

    def test_non_meld_cards_unchanged(self, meld_ctx: PipelineContext):
        lf = make_card_lf(
            [
                {"name": "Lightning Bolt", "layout": "normal", "side": None},
            ]
        )
        result = assign_meld_sides(lf, meld_ctx).collect()
        assert result["side"][0] is None

    def test_empty_meld_triplets_passthrough(self, simple_ctx: PipelineContext):
        lf = make_card_lf(
            [
                {"name": "Urza, Planeswalker", "layout": "meld", "side": None},
            ]
        )
        result = assign_meld_sides(lf, simple_ctx).collect()
        # With no meld_triplets, side stays None
        assert result["side"][0] is None


# ---------------------------------------------------------------------------
# TestParseTypeLineExpr
# ---------------------------------------------------------------------------


class TestParseTypeLineExpr:
    @pytest.mark.parametrize(
        ("type_line", "expected_super", "expected_types", "expected_sub"),
        [
            ("Creature — Human Warrior", [], ["Creature"], ["Human", "Warrior"]),
            ("Legendary Creature — Elf", ["Legendary"], ["Creature"], ["Elf"]),
            ("Instant", [], ["Instant"], []),
            ("Creature — Time Lord", [], ["Creature"], ["Time Lord"]),
            ("Plane — Ravnica", [], ["Plane"], ["Ravnica"]),
            (None, [], ["Card"], []),
        ],
        ids=[
            "creature_subtypes",
            "legendary_supertype",
            "instant_no_subtypes",
            "multi_word_subtype",
            "plane_subtype",
            "null_type",
        ],
    )
    def test_parse_type_line(
        self,
        type_line: str | None,
        expected_super: list[str],
        expected_types: list[str],
        expected_sub: list[str],
    ):
        lf = make_card_lf([{"type": type_line}])
        result = parse_type_line_expr(lf).collect()
        assert result["supertypes"].to_list()[0] == expected_super
        assert result["types"].to_list()[0] == expected_types
        assert result["subtypes"].to_list()[0] == expected_sub


# ---------------------------------------------------------------------------
# TestAddManaInfo
# ---------------------------------------------------------------------------


class TestAddManaInfo:
    def test_mana_value_from_cmc(self):
        lf = make_card_lf([{"manaValue": 3.0}])
        result = add_mana_info(lf).collect()
        assert result["manaValue"][0] == pytest.approx(3.0)
        assert result["convertedManaCost"][0] == pytest.approx(3.0)

    def test_face_mana_value_from_face_data(self):
        face = make_face_struct(mana_cost="{2}{R}")
        lf = make_card_lf([{"manaValue": 5.0, "_face_data": face}])
        result = add_mana_info(lf).collect()
        assert result["faceManaValue"][0] == pytest.approx(3.0)

    def test_null_mana_cost_zero(self):
        lf = make_card_lf([{"manaValue": None}])
        result = add_mana_info(lf).collect()
        assert result["manaValue"][0] == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# TestFormatPlaneswalkerText
# ---------------------------------------------------------------------------


class TestFormatPlaneswalkerText:
    @pytest.mark.parametrize(
        ("input_text", "expected"),
        [
            ("+1: Draw a card.", "[+1]: Draw a card."),
            ("−2: Deal 3 damage.", "[−2]: Deal 3 damage."),
            ("0: Create a token.", "[0]: Create a token."),
        ],
        ids=["plus_ability", "minus_ability", "zero_ability"],
    )
    def test_format_text(self, input_text: str, expected: str):
        lf = make_card_lf([{"text": input_text}])
        result = format_planeswalker_text(lf).collect()
        assert result["text"][0] == expected


# ---------------------------------------------------------------------------
# TestAddIsTimeshifted
# ---------------------------------------------------------------------------


class TestAddIsTimeshifted:
    def test_future_frame_is_timeshifted(self):
        lf = pl.LazyFrame(
            {"frameVersion": ["future"], "setCode": ["FUT"]},
        )
        result = add_is_timeshifted(lf).collect()
        assert result["isTimeshifted"][0] is True

    def test_normal_frame_not_timeshifted(self):
        lf = pl.LazyFrame(
            {"frameVersion": ["2015"], "setCode": ["MH3"]},
        )
        result = add_is_timeshifted(lf).collect()
        assert result["isTimeshifted"][0] is None

    def test_tsb_set_always_timeshifted(self):
        lf = pl.LazyFrame(
            {"frameVersion": ["2003"], "setCode": ["TSB"]},
        )
        result = add_is_timeshifted(lf).collect()
        assert result["isTimeshifted"][0] is True


# ---------------------------------------------------------------------------
# TestDetectAftermathLayout
# ---------------------------------------------------------------------------


class TestDetectAftermathLayout:
    def test_aftermath_detected(self):
        """Split card with 'Aftermath' in back face text -> layout='aftermath'."""
        face_a = make_face_struct(name="Front", oracle_text="Deal 3 damage.")
        face_b = make_face_struct(name="Back", oracle_text="Aftermath\nReturn target creature.")
        lf = _make_multiface_lf([[face_a, face_b]], layout="split")
        lf = explode_card_faces(lf)
        result = detect_aftermath_layout(lf).collect()
        layouts = result["layout"].to_list()
        assert all(l == "aftermath" for l in layouts)

    def test_normal_split_unchanged(self):
        """Split card without 'Aftermath' stays split."""
        face_a = make_face_struct(name="Front", oracle_text="Draw a card.")
        face_b = make_face_struct(name="Back", oracle_text="Gain 3 life.")
        lf = _make_multiface_lf([[face_a, face_b]], layout="split")
        lf = explode_card_faces(lf)
        result = detect_aftermath_layout(lf).collect()
        layouts = result["layout"].to_list()
        assert all(l == "split" for l in layouts)
