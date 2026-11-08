"""Regression tests for picking one UUID per Scryfall ID (issue #1703).

Faces of a multi-face card share a Scryfall ID. Card Kingdom and Manapool
prices are keyed by Scryfall ID, so the scryfall_to_uuid mapping keeps one
face's UUID per Scryfall ID, and it has to be the same one on every build.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl
import pytest

from mtgjson5 import constants
from mtgjson5.build.prices.price_builder import PolarsPriceBuilder
from mtgjson5.data import GLOBAL_CACHE
from mtgjson5.data.context import PipelineContext
from mtgjson5.pipeline.stages.output import _build_id_mappings, build_id_mappings_from_parquet

DFC_SCRYFALL_ID = "dfc00000-0000-0000-0000-000000000000"
SINGLE_SCRYFALL_ID = "single00-0000-0000-0000-000000000000"
# The back face sorts before the front face by UUID, so a plain UUID sort
# would pick the wrong one.
FRONT_UUID = "ffff0000-0000-0000-0000-00000000000a"
BACK_UUID = "0000ffff-0000-0000-0000-00000000000b"
SINGLE_UUID = "5151e000-0000-0000-0000-000000000000"

ROWS: list[dict[str, Any]] = [
    {"uuid": BACK_UUID, "scryfallId": DFC_SCRYFALL_ID, "side": "b"},
    {"uuid": FRONT_UUID, "scryfallId": DFC_SCRYFALL_ID, "side": "a"},
    {"uuid": SINGLE_UUID, "scryfallId": SINGLE_SCRYFALL_ID, "side": None},
]
ROW_ORDERS = [ROWS, list(reversed(ROWS))]
NO_IDS: dict[str, str | None] = dict.fromkeys(
    [
        "tcgplayerProductId",
        "tcgplayerEtchedProductId",
        "tcgplayerAlternativeFoilProductId",
        "mtgoId",
        "scryfallId",
        "mcmId",
    ]
)


def _cards(rows: list[dict[str, Any]]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "uuid": [row["uuid"] for row in rows],
            "side": pl.Series([row["side"] for row in rows], dtype=pl.String),
            "identifiers": [{**NO_IDS, "scryfallId": row["scryfallId"]} for row in rows],
        }
    )


@pytest.mark.parametrize("rows", ROW_ORDERS)
def test_scryfall_mapping_keeps_front_face(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, rows: list[dict[str, Any]]
) -> None:
    monkeypatch.setattr(constants, "CACHE_PATH", tmp_path)
    monkeypatch.setattr(GLOBAL_CACHE, "scryfall_to_uuid_lf", None)

    _build_id_mappings(PipelineContext(_cache=GLOBAL_CACHE), _cards(rows).lazy())

    persisted = pl.read_parquet(tmp_path / "scryfall_to_uuid.parquet")
    assert persisted.columns == ["uuid", "scryfallId"]
    assert sorted(persisted.rows()) == sorted([(FRONT_UUID, DFC_SCRYFALL_ID), (SINGLE_UUID, SINGLE_SCRYFALL_ID)])


@pytest.mark.parametrize("sides", [("a", None), (None, "a")])
def test_scryfall_mapping_breaks_front_face_ties_by_uuid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, sides: tuple[str | None, str | None]
) -> None:
    # A row with no side counts as a front face, so it neither beats nor loses to side "a".
    monkeypatch.setattr(constants, "CACHE_PATH", tmp_path)
    monkeypatch.setattr(GLOBAL_CACHE, "scryfall_to_uuid_lf", None)
    rows = [
        {"uuid": FRONT_UUID, "scryfallId": DFC_SCRYFALL_ID, "side": sides[0]},
        {"uuid": SINGLE_UUID, "scryfallId": DFC_SCRYFALL_ID, "side": sides[1]},
    ]

    _build_id_mappings(PipelineContext(_cache=GLOBAL_CACHE), _cards(rows).lazy())

    persisted = pl.read_parquet(tmp_path / "scryfall_to_uuid.parquet")
    assert persisted.rows() == [(SINGLE_UUID, DFC_SCRYFALL_ID)]


def test_scryfall_mapping_without_side_column(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(constants, "CACHE_PATH", tmp_path)
    monkeypatch.setattr(GLOBAL_CACHE, "scryfall_to_uuid_lf", None)

    _build_id_mappings(PipelineContext(_cache=GLOBAL_CACHE), _cards(ROWS).drop("side").lazy())

    persisted = pl.read_parquet(tmp_path / "scryfall_to_uuid.parquet")
    assert sorted(persisted.rows()) == sorted([(BACK_UUID, DFC_SCRYFALL_ID), (SINGLE_UUID, SINGLE_SCRYFALL_ID)])


@pytest.mark.parametrize("rows", ROW_ORDERS)
def test_manapool_prices_land_on_front_face(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, rows: list[dict[str, Any]]
) -> None:
    monkeypatch.setattr(constants, "CACHE_PATH", tmp_path)
    monkeypatch.setattr(GLOBAL_CACHE, "scryfall_to_uuid_lf", None)
    _build_id_mappings(PipelineContext(_cache=GLOBAL_CACHE), _cards(rows).lazy())
    raw_path = tmp_path / "manapool_raw.parquet"
    pl.DataFrame(
        {
            "scryfallId": [DFC_SCRYFALL_ID, SINGLE_SCRYFALL_ID],
            "price_cents": [150, 250],
            "price_cents_foil": [0, 0],
            "price_cents_etched": [0, 0],
        }
    ).write_parquet(raw_path)

    result = PolarsPriceBuilder()._map_manapool_raw(raw_path, tmp_path / "scryfall_to_uuid.parquet")

    assert sorted(zip(result["uuid"].to_list(), result["price"].to_list(), strict=True)) == sorted(
        [(FRONT_UUID, 1.5), (SINGLE_UUID, 2.5)]
    )


def test_other_mappings_keep_all_rows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(constants, "CACHE_PATH", tmp_path)
    monkeypatch.setattr(GLOBAL_CACHE, "scryfall_to_uuid_lf", None)
    monkeypatch.setattr(GLOBAL_CACHE, "mtgo_to_uuid_lf", None)
    cards = pl.DataFrame(
        {
            "uuid": [FRONT_UUID, BACK_UUID],
            "side": ["a", "b"],
            "identifiers": [{**NO_IDS, "scryfallId": DFC_SCRYFALL_ID, "mtgoId": mtgo} for mtgo in ("1", "2")],
        }
    )

    _build_id_mappings(PipelineContext(_cache=GLOBAL_CACHE), cards.lazy())

    mtgo = pl.read_parquet(tmp_path / "mtgo_to_uuid.parquet")
    assert mtgo.columns == ["uuid", "mtgoId"]
    assert sorted(mtgo.rows()) == sorted([(FRONT_UUID, "1"), (BACK_UUID, "2")])


def test_build_id_mappings_from_parquet_uses_side(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(constants, "CACHE_PATH", tmp_path)
    monkeypatch.setattr(GLOBAL_CACHE, "scryfall_to_uuid_lf", None)

    def write_partition(subdir: str, set_code: str, uuid: str, side: str | None) -> None:
        part = tmp_path / subdir / f"setCode={set_code}"
        part.mkdir(parents=True)
        data: dict[str, Any] = {
            "uuid": [uuid],
            "setCode": [set_code],
            "name": ["Card"],
            "number": ["1"],
            "identifiers": [{**NO_IDS, "scryfallId": DFC_SCRYFALL_ID if side else SINGLE_SCRYFALL_ID}],
            "finishes": [["nonfoil"]],
        }
        if side:
            data["side"] = [side]
        pl.DataFrame(data).write_parquet(part / "0.parquet")

    write_partition("_parquet", "TS1", BACK_UUID, "b")
    write_partition("_parquet", "TS2", FRONT_UUID, "a")
    # A partition without a side column (e.g. tokens) still maps.
    write_partition("_parquet_tokens", "TST", SINGLE_UUID, None)

    build_id_mappings_from_parquet(PipelineContext(_cache=GLOBAL_CACHE))

    persisted = pl.read_parquet(tmp_path / "scryfall_to_uuid.parquet")
    assert sorted(persisted.rows()) == sorted([(FRONT_UUID, DFC_SCRYFALL_ID), (SINGLE_UUID, SINGLE_SCRYFALL_ID)])
