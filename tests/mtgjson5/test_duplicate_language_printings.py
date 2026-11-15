"""One published card per collector number, whatever Scryfall's default_cards says.

On 2026-11-12 Scryfall added Japanese records for the Japan Showcase fracture
foils of DFT, ECL and EOE. For a day default_cards carried both the English
and the Japanese record of each number, and the build published both as
separate cards (DFT #411 Chandra, Spark Hunter twice, and so on). The IDs
below are the real Scryfall IDs of those printings.
"""

from __future__ import annotations

import logging

import polars as pl
import pytest

from mtgjson5.data.cache import select_default_languages
from mtgjson5.pipeline.stages.output import drop_duplicate_card_numbers

CHANDRA_DFT_411_EN = "d6a40fa6-4378-413b-bed8-7eb0d61b70ad"
CHANDRA_DFT_411_JA = "b1d34432-4665-4121-9f7a-f72ccdef5498"
RADIANT_LOTUS_DFT_406_EN = "8eb85966-66a2-4fb7-b89d-05db7f4e2cb8"
ALABASTER_POTION_4BB_1_ES = "d79196ec-038e-4a59-bd27-77a132c6596c"


def _default_cards(*rows: tuple[str, str, str, str]) -> pl.LazyFrame:
    return pl.DataFrame(
        [{"id": i, "lang": lang, "set": s, "collector_number": n} for i, lang, s, n in rows],
    ).lazy()


def _pairs(df: pl.DataFrame) -> set[tuple[str, str]]:
    return set(zip(df["scryfallId"].to_list(), df["language"].to_list(), strict=True))


def test_english_wins_a_number_default_cards_lists_in_two_languages() -> None:
    languages, _ = select_default_languages(
        _default_cards(
            (CHANDRA_DFT_411_JA, "ja", "dft", "411"),
            (CHANDRA_DFT_411_EN, "en", "dft", "411"),
            (RADIANT_LOTUS_DFT_406_EN, "en", "dft", "406"),
        )
    )

    assert _pairs(languages) == {
        (CHANDRA_DFT_411_EN, "English"),
        (RADIANT_LOTUS_DFT_406_EN, "English"),
    }


def test_the_dropped_printing_is_reported() -> None:
    _, dropped = select_default_languages(
        _default_cards(
            (CHANDRA_DFT_411_JA, "ja", "dft", "411"),
            (CHANDRA_DFT_411_EN, "en", "dft", "411"),
        )
    )

    assert dropped.to_dicts() == [
        {"setCode": "DFT", "number": "411", "language": "Japanese", "scryfallId": CHANDRA_DFT_411_JA}
    ]


def test_a_number_never_printed_in_english_keeps_its_one_language() -> None:
    languages, dropped = select_default_languages(_default_cards((ALABASTER_POTION_4BB_1_ES, "es", "4bb", "1")))

    assert _pairs(languages) == {(ALABASTER_POTION_4BB_1_ES, "Spanish")}
    assert dropped.is_empty()


def test_two_non_english_languages_resolve_to_the_same_one_every_time() -> None:
    rows = [("id-ja", "ja", "xxx", "1"), ("id-de", "de", "xxx", "1")]

    forward, _ = select_default_languages(_default_cards(*rows))
    backward, _ = select_default_languages(_default_cards(*reversed(rows)))

    assert _pairs(forward) == _pairs(backward) == {("id-de", "German")}


def _cards(*rows: tuple[str, str, str | None, str, str]) -> pl.DataFrame:
    return pl.DataFrame(
        [{"setCode": s, "number": n, "side": side, "name": name, "uuid": uuid} for s, n, side, name, uuid in rows],
        schema={"setCode": pl.String, "number": pl.String, "side": pl.String, "name": pl.String, "uuid": pl.String},
    )


def test_a_number_published_twice_keeps_one_card() -> None:
    cards = _cards(
        ("DFT", "411", None, "Chandra, Spark Hunter", "uuid-b"),
        ("DFT", "411", None, "Chandra, Spark Hunter", "uuid-a"),
        ("DFT", "406", None, "Radiant Lotus", "uuid-c"),
    )

    kept = drop_duplicate_card_numbers(cards, "cards")

    assert sorted(kept["uuid"].to_list()) == ["uuid-a", "uuid-c"]


def test_the_kept_card_does_not_depend_on_row_order() -> None:
    rows = [
        ("DFT", "411", None, "Chandra, Spark Hunter", "uuid-b"),
        ("DFT", "411", None, "Chandra, Spark Hunter", "uuid-a"),
    ]

    forward = drop_duplicate_card_numbers(_cards(*rows), "cards")
    backward = drop_duplicate_card_numbers(_cards(*reversed(rows)), "cards")

    assert forward["uuid"].to_list() == backward["uuid"].to_list() == ["uuid-a"]


def test_the_faces_of_one_card_share_a_number() -> None:
    cards = _cards(
        ("TDM", "1", "a", "Front // Back", "uuid-a"),
        ("TDM", "1", "b", "Front // Back", "uuid-b"),
        ("DFT", "1", None, "Other Set, Same Number", "uuid-c"),
    )

    kept = drop_duplicate_card_numbers(cards, "cards")

    assert kept.equals(cards)


def test_every_dropped_card_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    cards = _cards(
        ("DFT", "411", None, "Chandra, Spark Hunter", "uuid-a"),
        ("DFT", "411", None, "Chandra, Spark Hunter", "uuid-b"),
        ("EOE", "392", None, "Secluded Starforge", "uuid-c"),
        ("EOE", "392", None, "Secluded Starforge", "uuid-d"),
    )

    with caplog.at_level(logging.ERROR):
        drop_duplicate_card_numbers(cards, "cards")

    assert "DFT #411 Chandra, Spark Hunter: dropped uuid-b, kept uuid-a" in caplog.text
    assert "EOE #392 Secluded Starforge: dropped uuid-d, kept uuid-c" in caplog.text


def test_nothing_is_logged_when_every_number_is_unique(caplog: pytest.LogCaptureFixture) -> None:
    cards = _cards(("DFT", "406", None, "Radiant Lotus", "uuid-c"))

    with caplog.at_level(logging.ERROR):
        drop_duplicate_card_numbers(cards, "cards")

    assert caplog.text == ""
