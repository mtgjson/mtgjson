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


def _write_ctx(base: Path, rows: list[dict], set_meta: dict | None = None) -> AssemblyContext:
    files = {
        f"setCode={set_code}/0.parquet": [r for r in rows if r["setCode"] == set_code]
        for set_code in sorted({r["setCode"] for r in rows})
    }
    return _write_files_ctx(base, files, set_meta)


def _write_files_ctx(base: Path, files: dict[str, list[dict]], set_meta: dict | None = None) -> AssemblyContext:
    parquet_dir = base / "_parquet"
    tokens_dir = base / "_parquet_tokens"
    output_dir = base / "output"
    output_dir.mkdir(parents=True)
    tokens_dir.mkdir(parents=True)
    schema = CardSet.polars_schema()
    for rel_path, file_rows in files.items():
        path = parquet_dir / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(file_rows, schema=schema).write_parquet(path)
    return AssemblyContext(
        parquet_dir=parquet_dir,
        tokens_dir=tokens_dir,
        set_meta=set_meta or {},
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


def test_null_foreign_data_ranks_as_empty(tmp_path: Path) -> None:
    # Null foreignData counts as zero entries, so a printing with foreign
    # entries is preferred even when it sorts later on the tiebreak columns.
    null_foreign = _printing("AAA", "1", "uuid-aaa-1")
    null_foreign["foreignData"] = None
    with_foreign = _printing("BBB", "1", "uuid-bbb-1")
    with_foreign["foreignData"] = [{"language": "German", "name": "Llanowarelfen"}]
    ctx = _write_ctx(tmp_path, [null_foreign, with_foreign])

    cards = dict(ctx.atomic_cards.iter_atomic())["Llanowar Elves"]
    assert cards[0]["purchaseUrls"]["cardKingdom"] == "https://mtgjson.com/links/uuid-bbb-1"


def test_duplicate_uuid_resolves_to_first_file(tmp_path: Path) -> None:
    # The same uuid in two files ties on every sort key, including the tiebreak,
    # so load_all_cards must pick the same row every time.
    first = _printing("AAA", "1", "uuid-aaa-1")
    first["purchaseUrls"] = {"cardKingdom": "https://mtgjson.com/links/first"}
    second = _printing("AAA", "1", "uuid-aaa-1")
    second["purchaseUrls"] = {"cardKingdom": "https://mtgjson.com/links/second"}
    ctx = _write_files_ctx(
        tmp_path,
        {"setCode=AAA/0.parquet": [first], "setCode=AAA/1.parquet": [second]},
    )

    for _ in range(5):
        cards = dict(ctx.atomic_cards.iter_atomic())["Llanowar Elves"]
        assert cards[0]["purchaseUrls"]["cardKingdom"] == "https://mtgjson.com/links/first"


def test_first_printing_is_earliest_non_reprint(tmp_path: Path) -> None:
    original = _printing("BBB", "1", "uuid-bbb-1")
    original["isReprint"] = False
    later_original = _printing("AAA", "1", "uuid-aaa-1")
    later_original["isReprint"] = False
    reprint = _printing("CCC", "1", "uuid-ccc-1")
    reprint["isReprint"] = True
    set_meta = {
        "AAA": {"releaseDate": "1994-01-01"},
        "BBB": {"releaseDate": "1993-08-05"},
        "CCC": {"releaseDate": "1990-01-01"},
    }
    ctx = _write_ctx(tmp_path, [reprint, later_original, original], set_meta)

    cards = dict(ctx.atomic_cards.iter_atomic())["Llanowar Elves"]
    assert cards[0]["firstPrinting"] == "BBB"


def test_first_printing_omitted_without_non_reprint(tmp_path: Path) -> None:
    reprint = _printing("AAA", "1", "uuid-aaa-1")
    reprint["isReprint"] = True
    ctx = _write_ctx(tmp_path, [reprint])

    cards = dict(ctx.atomic_cards.iter_atomic())["Llanowar Elves"]
    assert "firstPrinting" not in cards[0]
