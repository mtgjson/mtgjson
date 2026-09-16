"""Tests for PipelineContext._build_foreign_data_df's name fallback.

Scryfall doesn't always transcribe a localized `printed_name` for a given
printing -- confirmed against the real Scryfall bulk data for e.g. all four
cards in "Secret Lair Drop Special Guest Junji Ito" (SLD 1114-1117): their
Japanese printings genuinely exist (their own distinct Scryfall IDs), but
`printed_name`/`printed_text`/`printed_type_line` are all null. Before this
fix, `_build_foreign_data_df` dropped any row whose computed `_foreign_name`
was null, which silently excluded these otherwise-legitimate printings from
foreignData[] entirely -- even though the foreign UUID it computes
(uuid5(defaultScryfallId + side + "_" + language, NAMESPACE_DNS)) never
depends on the name at all, so there was no reason those rows couldn't
resolve.
"""

from __future__ import annotations

import polars as pl
import polars_hash as plh

from mtgjson5.data.context import _DNS_NAMESPACE, PipelineContext

_CARD_FACES_SCHEMA = pl.List(
    pl.Struct(
        {
            "printed_name": pl.String,
            "name": pl.String,
            "flavor_text": pl.String,
            "printed_text": pl.String,
            "printed_type_line": pl.String,
        }
    )
)

_CARDS_SCHEMA = {
    "id": pl.String,
    "set": pl.String,
    "collectorNumber": pl.String,
    "lang": pl.String,
    "name": pl.String,
    "printedName": pl.String,
    "printedText": pl.String,
    "printedTypeLine": pl.String,
    "flavorText": pl.String,
    "multiverseIds": pl.List(pl.String),
    "cardFaces": _CARD_FACES_SCHEMA,
}


def _row(scryfall_id: str, lang: str, name: str, printed_name: str | None) -> dict:
    return {
        "id": scryfall_id,
        "set": "sld",
        "collectorNumber": "1114",
        "lang": lang,
        "name": name,
        "printedName": printed_name,
        "printedText": None,
        "printedTypeLine": None,
        "flavorText": None,
        "multiverseIds": [],
        "cardFaces": [],
    }


def _default_lookup(scryfall_id: str, number: str = "1114") -> pl.DataFrame:
    return pl.DataFrame(
        {
            "setCode": ["SLD"],
            "number": [number],
            "_default_scryfall_id": [scryfall_id],
            "_default_side": ["a"],
        }
    )


def _expected_uuid(default_scryfall_id: str, language: str) -> str:
    source = default_scryfall_id + "a" + "_" + language
    return pl.DataFrame({"s": [source]}).with_columns(plh.col("s").uuidhash.uuid5(_DNS_NAMESPACE).alias("u"))["u"][0]


def _build(rows: list[dict], default_lookup: pl.DataFrame) -> list[dict]:
    cards = pl.DataFrame(rows, schema=_CARDS_SCHEMA)
    ctx = PipelineContext.for_testing()
    result = ctx._build_foreign_data_df(cards, default_lookup, set(), set())
    if result.height == 0:
        return []
    return result.to_dicts()[0]["foreignData"]


DEFAULT_EN_ID = "c0e175cb-ffea-473f-8422-53273f263016"
JA_ID = "793322ec-e854-4457-a8a3-832825f02a87"


class TestForeignNameFallback:
    def test_null_printed_name_falls_back_to_the_card_name(self):
        """The Carrion Feeder / SLD:1114 case: Japanese printing exists, but
        Scryfall never transcribed a printed_name for it."""
        rows = [
            _row(DEFAULT_EN_ID, "en", "Carrion Feeder", None),
            _row(JA_ID, "ja", "Carrion Feeder", None),
        ]
        foreign_data = _build(rows, _default_lookup(DEFAULT_EN_ID))
        assert len(foreign_data) == 1
        assert foreign_data[0]["language"] == "Japanese"
        assert foreign_data[0]["name"] == "Carrion Feeder"

    def test_fallback_does_not_change_the_computed_uuid(self):
        """The UUID formula never depended on the name -- the fallback exists
        purely to stop the row being dropped before it, so the resulting UUID
        must match uuid5(defaultScryfallId + side + '_' + language) exactly,
        the same formula card: entries resolve against."""
        rows = [
            _row(DEFAULT_EN_ID, "en", "Carrion Feeder", None),
            _row(JA_ID, "ja", "Carrion Feeder", None),
        ]
        foreign_data = _build(rows, _default_lookup(DEFAULT_EN_ID))
        assert foreign_data[0]["uuid"] == _expected_uuid(DEFAULT_EN_ID, "Japanese")

    def test_genuine_printed_name_is_still_preferred_over_the_fallback(self):
        """Regression guard: when Scryfall *does* have a transcribed name,
        that real translation must still win, not the English fallback."""
        rows = [
            _row(DEFAULT_EN_ID, "en", "Plains", None),
            _row(JA_ID, "ja", "Plains", "平地"),
        ]
        foreign_data = _build(rows, _default_lookup(DEFAULT_EN_ID))
        assert foreign_data[0]["name"] == "平地"

    def test_text_and_type_are_not_fabricated(self):
        """Only the name gets an English fallback; text/type stay null when
        Scryfall has no translation for them, rather than silently showing
        the English rules text under a foreign-language entry."""
        rows = [
            _row(DEFAULT_EN_ID, "en", "Carrion Feeder", None),
            _row(JA_ID, "ja", "Carrion Feeder", None),
        ]
        foreign_data = _build(rows, _default_lookup(DEFAULT_EN_ID))
        assert foreign_data[0]["text"] is None
        assert foreign_data[0]["type"] is None
