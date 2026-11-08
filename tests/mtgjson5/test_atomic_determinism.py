"""AtomicCards must pick the same representative printing on every build."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from mtgjson5.build.context import AssemblyContext
from mtgjson5.models.cards import CardSet


def _printing(set_code: str, number: str, uuid: str) -> dict:
    """A printing of the same oracle card that ties on every sort preference."""
    return {
        "name": "Llanowar Elves",
        "setCode": set_code,
        "number": number,
        "uuid": uuid,
        "layout": "normal",
        "type": "Creature — Elf Druid",
        "types": ["Creature"],
        "subtypes": ["Elf", "Druid"],
        "supertypes": [],
        "text": "{T}: Add {G}.",
        "manaCost": "{G}",
        "manaValue": 1.0,
        "convertedManaCost": 1.0,
        "colors": ["G"],
        "colorIdentity": ["G"],
        "power": "1",
        "toughness": "1",
        "foreignData": [],
        "purchaseUrls": {"cardKingdom": f"https://mtgjson.com/links/{uuid}"},
    }


def _write_ctx(base: Path, rows: list[dict]) -> AssemblyContext:
    parquet_dir = base / "_parquet"
    tokens_dir = base / "_parquet_tokens"
    output_dir = base / "output"
    output_dir.mkdir(parents=True)
    tokens_dir.mkdir(parents=True)
    schema = CardSet.polars_schema()
    for set_code in sorted({r["setCode"] for r in rows}):
        path = parquet_dir / f"setCode={set_code}"
        path.mkdir(parents=True)
        set_rows = [r for r in rows if r["setCode"] == set_code]
        pl.DataFrame(set_rows, schema=schema).write_parquet(path / "0.parquet")
    return AssemblyContext(
        parquet_dir=parquet_dir,
        tokens_dir=tokens_dir,
        set_meta={},
        meta={"date": "2025-01-01", "version": "5.3.0+test"},
        output_path=output_dir,
    )


PRINTINGS = [
    _printing("AAA", "10", "uuid-aaa-10"),
    _printing("AAA", "2", "uuid-aaa-2"),
    _printing("BBB", "1", "uuid-bbb-1"),
    _printing("CCC", "1", "uuid-ccc-1"),
]


@pytest.mark.parametrize("order", [[0, 1, 2, 3], [3, 2, 1, 0], [2, 0, 3, 1], [1, 3, 0, 2]])
def test_atomic_representative_printing_is_deterministic(tmp_path: Path, order: list[int]) -> None:
    ctx = _write_ctx(tmp_path, [PRINTINGS[i] for i in order])

    for _ in range(3):
        atomic = dict(ctx.atomic_cards.iter_atomic())
        cards = atomic["Llanowar Elves"]
        assert len(cards) == 1
        # Ties on every preference flag are broken by (setCode, number, uuid).
        assert cards[0]["purchaseUrls"]["cardKingdom"] == "https://mtgjson.com/links/uuid-aaa-10"
