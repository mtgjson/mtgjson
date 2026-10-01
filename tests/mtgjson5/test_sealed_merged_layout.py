"""Reading mtg-sealed-content in either on-disk layout.

mtg-sealed-content keeps each sealed product in two files today: its
definition in data/products/SET.yaml and what is inside it in
data/contents/SET.yaml. It is folding the second into the first as a nested
``contents`` key and dropping data/contents/ altogether. Until that lands
MTGJSON reads both layouts, deciding once per tarball (merged exactly when it
has no data/contents/), and the same data has to compile to exactly the same
products, contents and deck map either way.
"""

from __future__ import annotations

import asyncio
import io
import logging
import tarfile
import tempfile
from pathlib import Path

import pytest
import yaml

from mtgjson5.pipeline.stages.sealed import compile_contents, compile_products
from mtgjson5.providers.github.provider import SealedDataProvider, _build_sealed_products_records, _to_lazyframe

PRODUCTS = {
    "tst": {
        "Test Anniversary Bundle": {"category": "BUNDLE", "subtype": "DEFAULT", "identifiers": {"mcmId": "3"}},
        "Test Booster Box": {"category": "BOOSTER_BOX", "subtype": "DRAFT", "identifiers": {"tcgplayerProductId": "1"}},
        "Test Booster Pack": {"category": "BOOSTER_PACK", "subtype": "DRAFT", "identifiers": {}},
        "Test Bundle": {
            "category": "BUNDLE",
            "subtype": "DEFAULT",
            "release_date": "2024-08-02",
            "identifiers": {"tcgplayerProductId": "2"},
        },
        "Test Mystery Box": {"category": "BOX_SET", "subtype": "DEFAULT", "identifiers": {}},
        "Test Showdown Booster": {
            "category": "BOOSTER_PACK",
            "subtype": "PREMIER",
            "language": "Japanese",
            "identifiers": {"mcmId": "5"},
        },
    },
    "ts2": {
        "Second Set Booster Pack": {"category": "BOOSTER_PACK", "subtype": "DEFAULT", "identifiers": {}},
        "Second Set Bundle": {"category": "BUNDLE", "subtype": "DEFAULT", "identifiers": {}},
    },
}

# What data/contents/ holds for the products above, placeholders included.
CONTENTS = {
    "tst": {
        # Sorted product names often put a copy ahead of the product it copies.
        "Test Anniversary Bundle": {"copy": "Test Bundle"},
        "Test Booster Box": {
            "sealed": [{"set": "tst", "count": 36, "name": "Test Booster Pack"}],
            # YAML reads one number as a string and the other as an int.
            "card": [
                {"set": "tst", "number": "2a", "name": "Second Card"},
                {"set": "tst", "number": 1, "name": "Test Card"},
            ],
        },
        "Test Booster Pack": {"pack": [{"set": "tst", "code": "draft"}], "card_count": 15},
        "Test Bundle": {
            "sealed": [{"set": "tst", "count": 9, "name": "Test Booster Pack"}],
            "deck": [{"set": "tst", "name": "Test Deck"}],
            "card": [{"set": "tst", "number": 1, "name": "Test Card", "foil": True}],
            "other": [{"name": "Spindown life counter"}],
            "card_count": 1,
        },
        "Test Mystery Box": {
            "variable_mode": {"count": 1},
            "variable": [
                {"deck": [{"set": "tst", "name": "Test Deck"}]},
                {"card": [{"set": "tst", "number": 3, "name": "Third Card"}]},
            ],
        },
        # Not researched yet
        "Test Showdown Booster": [],
    },
    "ts2": {"Second Set Booster Pack": {}, "Second Set Bundle": None},
}


def _dump(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data, allow_unicode=True), encoding="utf-8")


def _write_split(root: Path) -> tuple[Path, Path]:
    for code, products in PRODUCTS.items():
        _dump(root / "data" / "products" / f"{code.upper()}.yaml", {"code": code, "products": products})
        _dump(root / "data" / "contents" / f"{code.upper()}.yaml", {"code": code, "products": CONTENTS[code]})
    return root / "data" / "products", root / "data" / "contents"


def _write_merged(root: Path) -> Path:
    """The same data after the migration: a placeholder becomes no contents key."""
    for code, products in PRODUCTS.items():
        merged = {
            name: {**info, "contents": CONTENTS[code][name]} if CONTENTS[code][name] else dict(info)
            for name, info in products.items()
        }
        _dump(root / "data" / "products" / f"{code.upper()}.yaml", {"code": code, "products": merged})
    return root / "data" / "products"


def _uuid_map() -> dict:
    return {
        "tst": {
            "cards": {"1": ("card-1", "Test Card"), "2a": ("card-2a", "Second Card"), "3": ("card-3", "Third Card")},
            "tokens": {},
            "booster": {"draft"},
            "decks": {"Test Deck"},
            "sealedProduct": {name: f"uuid-{name}" for name in PRODUCTS["tst"]},
        }
    }


def test_compile_products_drops_nested_contents(tmp_path):
    split = compile_products(_write_split(tmp_path / "split")[0])
    merged = compile_products(_write_merged(tmp_path / "merged"))

    assert merged == split == PRODUCTS


def test_sealed_products_frame_builds_from_merged_products(tmp_path):
    """Nested contents would reach the frame, and polars cannot build one list of mixed card numbers."""
    split = compile_products(_write_split(tmp_path / "split")[0])
    merged = compile_products(_write_merged(tmp_path / "merged"))

    frame = _to_lazyframe(_build_sealed_products_records(merged), "sealed_products", "sealed_products").collect()
    expected = _to_lazyframe(_build_sealed_products_records(split), "sealed_products", "sealed_products").collect()

    assert "contents" not in frame.columns
    assert frame.equals(expected)


def test_compile_contents_matches_across_layouts(tmp_path, caplog):
    products_dir, contents_dir = _write_split(tmp_path / "split")
    merged_dir = _write_merged(tmp_path / "merged")

    with caplog.at_level(logging.WARNING, logger="mtgjson5.pipeline.stages.sealed"):
        split = compile_contents(products_dir, contents_dir, _uuid_map())
        split_warnings = [r.getMessage() for r in caplog.records]
        caplog.clear()
        merged = compile_contents(merged_dir, None, _uuid_map())
        merged_warnings = [r.getMessage() for r in caplog.records]

    assert merged == split
    assert merged_warnings == split_warnings
    assert split_warnings == [
        "Product ts2 - Second Set Booster Pack missing contents",
        "Product ts2 - Second Set Bundle missing contents",
        "Product tst - Test Showdown Booster missing contents",
    ]

    contents, deck_map = merged
    # Nothing researched in ts2, and the placeholder product is left out.
    assert list(contents) == ["tst"]
    assert list(contents["tst"]) == [
        "Test Anniversary Bundle",
        "Test Booster Box",
        "Test Booster Pack",
        "Test Bundle",
        "Test Mystery Box",
    ]
    assert contents["tst"]["Test Anniversary Bundle"] == contents["tst"]["Test Bundle"]
    assert contents["tst"]["Test Bundle"]["card"] == [
        {"name": "Test Card", "set": "tst", "number": "1", "uuid": "card-1", "foil": True}
    ]
    configs = contents["tst"]["Test Mystery Box"]["variable"][0]["configs"]
    assert [sorted(config) for config in configs] == [["deck", "variable_config"], ["card", "variable_config"]]
    assert all(config["variable_config"] == [{"chance": 1, "weight": 2}] for config in configs)
    # A copy holds its target's decks too, so both link back from the deck.
    assert deck_map == {"tst": {"Test Deck": ["uuid-Test Anniversary Bundle", "uuid-Test Bundle"]}}


# ---------------------------------------------------------------------------
# Tarball extraction
# ---------------------------------------------------------------------------

TARBALL_ROOT = "mtgjson-mtg-sealed-content-0123abc/"
SET_FILE = "code: tst\nproducts: {}\n"
SPLIT_TARBALL = {
    "README.md": "# mtg-sealed-content\n",
    "data/products/TST.yaml": SET_FILE,
    "data/contents/TST.yaml": SET_FILE,
    "outputs/deck_map.json": "{}",
}
MERGED_TARBALL = {
    "README.md": "# mtg-sealed-content\n",
    "data/products/TST.yaml": SET_FILE,
    "outputs/deck_map.json": "{}",
}


def _tarball(files: dict[str, str]) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        for name, text in files.items():
            payload = text.encode()
            member = tarfile.TarInfo(TARBALL_ROOT + name)
            member.size = len(payload)
            tar.addfile(member, io.BytesIO(payload))
    return buffer.getvalue()


class FakeResponse:
    def __init__(self, payload: bytes):
        self._payload = payload

    async def __aenter__(self) -> FakeResponse:
        return self

    async def __aexit__(self, *args: object) -> None:
        return None

    def raise_for_status(self) -> None:
        return None

    async def read(self) -> bytes:
        return self._payload


class FakeSession:
    def __init__(self, tarball: bytes):
        self._tarball = tarball
        self.calls = 0

    def get(self, url: str) -> FakeResponse:
        self.calls += 1
        return FakeResponse(self._tarball)


def _extract(cache_path: Path | None, session: FakeSession) -> tuple[Path, Path | None]:
    provider = SealedDataProvider(cache_path=cache_path)
    return asyncio.run(provider._fetch_and_extract_yaml(session))


def _yaml_names(directory: Path) -> list[str]:
    return sorted(path.name for path in directory.glob("*.yaml"))


@pytest.mark.parametrize("cached", [True, False], ids=["cache_path", "tempdir"])
@pytest.mark.parametrize(("files", "split"), [(SPLIT_TARBALL, True), (MERGED_TARBALL, False)], ids=["split", "merged"])
def test_layout_follows_the_tarball(tmp_path, monkeypatch, files, split, cached):
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))

    products_dir, contents_dir = _extract(tmp_path if cached else None, FakeSession(_tarball(files)))

    assert _yaml_names(products_dir) == ["TST.yaml"]
    if split:
        assert contents_dir == products_dir.parent / "contents"
        assert _yaml_names(contents_dir) == ["TST.yaml"]
    else:
        assert contents_dir is None
        assert not (products_dir.parent / "contents").exists()


def test_extraction_starts_from_an_empty_directory(tmp_path):
    """A data/contents/ tree left from an earlier run must not make a merged tarball look split."""
    stale = tmp_path / "sealed_yaml" / "data" / "contents" / "OLD.yaml"
    stale.parent.mkdir(parents=True)
    stale.write_text(SET_FILE)
    # ... nor may anything a crashed extraction left in the staging directory.
    leftover = tmp_path / "sealed_yaml.partial" / "data" / "products" / "GONE.yaml"
    leftover.parent.mkdir(parents=True)
    leftover.write_text(SET_FILE)

    products_dir, contents_dir = _extract(tmp_path, FakeSession(_tarball(MERGED_TARBALL)))

    assert contents_dir is None
    assert _yaml_names(products_dir) == ["TST.yaml"]
    assert not (tmp_path / "sealed_yaml" / "data" / "contents").exists()
    assert not (tmp_path / "sealed_yaml.partial").exists()


@pytest.mark.parametrize(("files", "split"), [(SPLIT_TARBALL, True), (MERGED_TARBALL, False)], ids=["split", "merged"])
def test_cached_extraction_keeps_its_layout(tmp_path, files, split):
    session = FakeSession(_tarball(files))

    first = _extract(tmp_path, session)
    products_dir, contents_dir = _extract(tmp_path, session)

    assert (products_dir, contents_dir) == first
    assert (contents_dir is not None) == split
    assert session.calls == 1
