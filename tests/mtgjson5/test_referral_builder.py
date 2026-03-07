"""Tests for mtgjson5.build.referral_builder.

Covers all three card providers (Card Kingdom, TCGPlayer, Cardmarket),
sealed-product referrals, the Nginx map writer, and the top-level
build_and_write_referral_map orchestrator.

Every provider builder is tested against on-disk parquet files so
these tests also serve as a regression guard
"""

from __future__ import annotations

from typing import Any

import polars as pl
import polars_hash as plh  # noqa: F401  # pylint: disable=unused-import  # registers .chash namespace

from mtgjson5 import constants
from mtgjson5.build.referral_builder import (
    CK_BASE,
    CK_REFERRAL,
    TCG_REFERRAL_PREFIX,
    TCG_REFERRAL_SUFFIX,
    _build_cardmarket_entries_from_parquet,
    _build_ck_entries_from_parquet,
    _build_tcg_entries_from_parquet,
    build_and_write_referral_map,
    build_referral_map_from_sealed,
    write_referral_map,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sha256_16(val: str) -> str:
    """Compute the same sha256[:16] hash the referral builder uses."""
    df = pl.DataFrame({"v": [val]}).with_columns(pl.col("v").chash.sha2_256().str.slice(0, 16).alias("h"))
    return df["h"][0]


def _write_cards_parquet(
    parquet_dir: Any,
    rows: list[dict[str, Any]],
    *,
    set_code: str = "TST",
    extra_schema: dict[str, Any] | None = None,
) -> None:
    """Write a minimal cards parquet partitioned by setCode."""
    schema: dict[str, Any] = {
        "uuid": pl.String,
        "identifiers": pl.Struct(
            {
                "scryfallId": pl.String,
                "tcgplayerProductId": pl.String,
                "tcgplayerEtchedProductId": pl.String,
                "tcgplayerAlternativeFoilProductId": pl.String,
                "mcmId": pl.String,
                "mcmMetaId": pl.String,
                "cardKingdomId": pl.String,
            }
        ),
        "finishes": pl.List(pl.String),
    }
    if extra_schema:
        schema.update(extra_schema)

    out = parquet_dir / f"setCode={set_code}"
    out.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows, schema=schema).write_parquet(out / "cards.parquet")


def _write_ck_cache(cache_dir: Any, rows: list[dict[str, Any]]) -> None:
    """Write a ck_pivoted.parquet cache file."""
    schema = {
        "id": pl.String,
        "cardKingdomId": pl.String,
        "cardKingdomUrl": pl.String,
        "cardKingdomFoilId": pl.String,
        "cardKingdomFoilUrl": pl.String,
        "cardKingdomEtchedId": pl.String,
        "cardKingdomEtchedUrl": pl.String,
    }
    pl.DataFrame(rows, schema=schema).write_parquet(cache_dir / "ck_pivoted.parquet")


# ---------------------------------------------------------------------------
# Card Kingdom
# ---------------------------------------------------------------------------


class TestBuildCkEntries:
    """Tests for _build_ck_entries_from_parquet."""

    def test_produces_entries_for_nonfoil_url(self, tmp_path, monkeypatch):

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        monkeypatch.setattr(constants, "CACHE_PATH", cache_dir)

        _write_ck_cache(
            cache_dir,
            [
                {
                    "id": "sf-001",
                    "cardKingdomId": "1234",
                    "cardKingdomUrl": "catalog/product/1234",
                    "cardKingdomFoilId": None,
                    "cardKingdomFoilUrl": None,
                    "cardKingdomEtchedId": None,
                    "cardKingdomEtchedUrl": None,
                },
            ],
        )

        parquet_dir = tmp_path / "parquet"
        _write_cards_parquet(
            parquet_dir,
            [
                {
                    "uuid": "uuid-001",
                    "identifiers": {
                        "scryfallId": "sf-001",
                        "tcgplayerProductId": None,
                        "tcgplayerEtchedProductId": None,
                        "tcgplayerAlternativeFoilProductId": None,
                        "mcmId": None,
                        "mcmMetaId": None,
                        "cardKingdomId": "1234",
                    },
                    "finishes": ["nonfoil"],
                },
            ],
        )

        result = _build_ck_entries_from_parquet(parquet_dir)

        assert result is not None
        assert len(result) == 1
        assert set(result.columns) == {"hash", "referral_url"}

        expected_hash = _sha256_16(CK_BASE + "catalog/product/1234" + "uuid-001")
        expected_url = CK_BASE + "catalog/product/1234" + CK_REFERRAL
        assert result["hash"][0] == expected_hash
        assert result["referral_url"][0] == expected_url

    def test_produces_entries_for_foil_and_etched(self, tmp_path, monkeypatch):

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        monkeypatch.setattr(constants, "CACHE_PATH", cache_dir)

        _write_ck_cache(
            cache_dir,
            [
                {
                    "id": "sf-002",
                    "cardKingdomId": "2000",
                    "cardKingdomUrl": "catalog/regular/2000",
                    "cardKingdomFoilId": "2001",
                    "cardKingdomFoilUrl": "catalog/foil/2001",
                    "cardKingdomEtchedId": "2002",
                    "cardKingdomEtchedUrl": "catalog/etched/2002",
                },
            ],
        )

        parquet_dir = tmp_path / "parquet"
        _write_cards_parquet(
            parquet_dir,
            [
                {
                    "uuid": "uuid-002",
                    "identifiers": {
                        "scryfallId": "sf-002",
                        "tcgplayerProductId": None,
                        "tcgplayerEtchedProductId": None,
                        "tcgplayerAlternativeFoilProductId": None,
                        "mcmId": None,
                        "mcmMetaId": None,
                        "cardKingdomId": "2000",
                    },
                    "finishes": ["nonfoil", "foil"],
                },
            ],
        )

        result = _build_ck_entries_from_parquet(parquet_dir)

        assert result is not None
        # 3 entries: regular, foil, etched
        assert len(result) == 3

        urls = set(result["referral_url"].to_list())
        assert CK_BASE + "catalog/regular/2000" + CK_REFERRAL in urls
        assert CK_BASE + "catalog/foil/2001" + CK_REFERRAL in urls
        assert CK_BASE + "catalog/etched/2002" + CK_REFERRAL in urls

    def test_returns_none_when_no_ck_cache(self, tmp_path, monkeypatch):

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        monkeypatch.setattr(constants, "CACHE_PATH", cache_dir)
        # No ck_pivoted.parquet written

        parquet_dir = tmp_path / "parquet"
        parquet_dir.mkdir()

        result = _build_ck_entries_from_parquet(parquet_dir)
        assert result is None

    def test_returns_none_when_no_parquet_dir(self, tmp_path, monkeypatch):

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        monkeypatch.setattr(constants, "CACHE_PATH", cache_dir)
        _write_ck_cache(
            cache_dir,
            [
                {
                    "id": "sf-001",
                    "cardKingdomId": "1",
                    "cardKingdomUrl": "url",
                    "cardKingdomFoilId": None,
                    "cardKingdomFoilUrl": None,
                    "cardKingdomEtchedId": None,
                    "cardKingdomEtchedUrl": None,
                },
            ],
        )

        result = _build_ck_entries_from_parquet(None)
        assert result is None

    def test_skips_null_urls(self, tmp_path, monkeypatch):
        """Cards that exist in CK cache but have null URL should be excluded."""

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        monkeypatch.setattr(constants, "CACHE_PATH", cache_dir)

        _write_ck_cache(
            cache_dir,
            [
                {
                    "id": "sf-001",
                    "cardKingdomId": "1234",
                    "cardKingdomUrl": None,
                    "cardKingdomFoilId": None,
                    "cardKingdomFoilUrl": None,
                    "cardKingdomEtchedId": None,
                    "cardKingdomEtchedUrl": None,
                },
            ],
        )

        parquet_dir = tmp_path / "parquet"
        _write_cards_parquet(
            parquet_dir,
            [
                {
                    "uuid": "uuid-001",
                    "identifiers": {
                        "scryfallId": "sf-001",
                        "tcgplayerProductId": None,
                        "tcgplayerEtchedProductId": None,
                        "tcgplayerAlternativeFoilProductId": None,
                        "mcmId": None,
                        "mcmMetaId": None,
                        "cardKingdomId": "1234",
                    },
                    "finishes": ["nonfoil"],
                },
            ],
        )

        result = _build_ck_entries_from_parquet(parquet_dir)
        assert result is None

    def test_multiple_cards(self, tmp_path, monkeypatch):
        """Multiple cards from different sets produce distinct entries."""

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        monkeypatch.setattr(constants, "CACHE_PATH", cache_dir)

        _write_ck_cache(
            cache_dir,
            [
                {
                    "id": "sf-a",
                    "cardKingdomId": "100",
                    "cardKingdomUrl": "catalog/a",
                    "cardKingdomFoilId": None,
                    "cardKingdomFoilUrl": None,
                    "cardKingdomEtchedId": None,
                    "cardKingdomEtchedUrl": None,
                },
                {
                    "id": "sf-b",
                    "cardKingdomId": "200",
                    "cardKingdomUrl": "catalog/b",
                    "cardKingdomFoilId": None,
                    "cardKingdomFoilUrl": None,
                    "cardKingdomEtchedId": None,
                    "cardKingdomEtchedUrl": None,
                },
            ],
        )

        parquet_dir = tmp_path / "parquet"
        _write_cards_parquet(
            parquet_dir,
            [
                {
                    "uuid": "uuid-a",
                    "identifiers": {
                        "scryfallId": "sf-a",
                        "tcgplayerProductId": None,
                        "tcgplayerEtchedProductId": None,
                        "tcgplayerAlternativeFoilProductId": None,
                        "mcmId": None,
                        "mcmMetaId": None,
                        "cardKingdomId": "100",
                    },
                    "finishes": ["nonfoil"],
                },
            ],
            set_code="SET1",
        )
        _write_cards_parquet(
            parquet_dir,
            [
                {
                    "uuid": "uuid-b",
                    "identifiers": {
                        "scryfallId": "sf-b",
                        "tcgplayerProductId": None,
                        "tcgplayerEtchedProductId": None,
                        "tcgplayerAlternativeFoilProductId": None,
                        "mcmId": None,
                        "mcmMetaId": None,
                        "cardKingdomId": "200",
                    },
                    "finishes": ["nonfoil"],
                },
            ],
            set_code="SET2",
        )

        result = _build_ck_entries_from_parquet(parquet_dir)

        assert result is not None
        assert len(result) == 2
        assert len(result["hash"].unique()) == 2


# ---------------------------------------------------------------------------
# TCGPlayer
# ---------------------------------------------------------------------------


class TestBuildTcgEntries:
    """Tests for _build_tcg_entries_from_parquet."""

    def test_produces_entries_for_tcgplayer_product(self, tmp_path):
        parquet_dir = tmp_path / "parquet"
        _write_cards_parquet(
            parquet_dir,
            [
                {
                    "uuid": "uuid-001",
                    "identifiers": {
                        "scryfallId": "sf-001",
                        "tcgplayerProductId": "55555",
                        "tcgplayerEtchedProductId": None,
                        "tcgplayerAlternativeFoilProductId": None,
                        "mcmId": None,
                        "mcmMetaId": None,
                        "cardKingdomId": None,
                    },
                    "finishes": ["nonfoil"],
                },
            ],
        )

        result = _build_tcg_entries_from_parquet(parquet_dir)

        assert result is not None
        assert len(result) == 1

        expected_hash = _sha256_16("55555" + "uuid-001")
        expected_url = TCG_REFERRAL_PREFIX + "55555" + TCG_REFERRAL_SUFFIX
        assert result["hash"][0] == expected_hash
        assert result["referral_url"][0] == expected_url

    def test_produces_etched_and_alt_foil_entries(self, tmp_path):
        parquet_dir = tmp_path / "parquet"
        _write_cards_parquet(
            parquet_dir,
            [
                {
                    "uuid": "uuid-001",
                    "identifiers": {
                        "scryfallId": "sf-001",
                        "tcgplayerProductId": "100",
                        "tcgplayerEtchedProductId": "101",
                        "tcgplayerAlternativeFoilProductId": "102",
                        "mcmId": None,
                        "mcmMetaId": None,
                        "cardKingdomId": None,
                    },
                    "finishes": ["nonfoil", "foil"],
                },
            ],
        )

        result = _build_tcg_entries_from_parquet(parquet_dir)

        assert result is not None
        assert len(result) == 3
        assert len(result["hash"].unique()) == 3

    def test_returns_none_when_no_parquet(self, tmp_path):
        result = _build_tcg_entries_from_parquet(None)
        assert result is None

    def test_returns_none_when_no_tcg_ids(self, tmp_path):
        parquet_dir = tmp_path / "parquet"
        _write_cards_parquet(
            parquet_dir,
            [
                {
                    "uuid": "uuid-001",
                    "identifiers": {
                        "scryfallId": "sf-001",
                        "tcgplayerProductId": None,
                        "tcgplayerEtchedProductId": None,
                        "tcgplayerAlternativeFoilProductId": None,
                        "mcmId": None,
                        "mcmMetaId": None,
                        "cardKingdomId": None,
                    },
                    "finishes": ["nonfoil"],
                },
            ],
        )

        result = _build_tcg_entries_from_parquet(parquet_dir)
        assert result is None


# ---------------------------------------------------------------------------
# Cardmarket
# ---------------------------------------------------------------------------


class TestBuildCardmarketEntries:
    """Tests for _build_cardmarket_entries_from_parquet."""

    def _make_ctx(self, scryfall_rows: list[dict[str, Any]]):
        from mtgjson5.data.context import PipelineContext

        cards_lf = pl.LazyFrame(
            scryfall_rows,
            schema={
                "id": pl.String,
                "purchaseUris": pl.Struct(
                    {
                        "cardmarket": pl.String,
                    }
                ),
            },
        )
        return PipelineContext.for_testing(
            cards_lf=cards_lf,
            meld_triplets={},
            manual_overrides={},
        )

    def test_produces_nonfoil_entries(self, tmp_path):
        ctx = self._make_ctx(
            [
                {
                    "id": "sf-001",
                    "purchaseUris": {
                        "cardmarket": "https://www.cardmarket.com/en/Magic/Products/Singles/Set/Card?referrer=scryfall&utm_source=scryfall",
                    },
                },
            ]
        )

        parquet_dir = tmp_path / "parquet"
        _write_cards_parquet(
            parquet_dir,
            [
                {
                    "uuid": "uuid-001",
                    "identifiers": {
                        "scryfallId": "sf-001",
                        "tcgplayerProductId": None,
                        "tcgplayerEtchedProductId": None,
                        "tcgplayerAlternativeFoilProductId": None,
                        "mcmId": "99",
                        "mcmMetaId": "88",
                        "cardKingdomId": None,
                    },
                    "finishes": ["nonfoil"],
                },
            ],
        )

        result = _build_cardmarket_entries_from_parquet(ctx, parquet_dir)

        assert result is not None
        assert len(result) >= 1

        # URL should have "scryfall" replaced with "mtgjson"
        for url in result["referral_url"].to_list():
            assert "scryfall" not in url
            assert "mtgjson" in url

    def test_produces_foil_entries(self, tmp_path):
        ctx = self._make_ctx(
            [
                {
                    "id": "sf-001",
                    "purchaseUris": {
                        "cardmarket": "https://www.cardmarket.com/en/Magic/Card?referrer=scryfall",
                    },
                },
            ]
        )

        parquet_dir = tmp_path / "parquet"
        _write_cards_parquet(
            parquet_dir,
            [
                {
                    "uuid": "uuid-001",
                    "identifiers": {
                        "scryfallId": "sf-001",
                        "tcgplayerProductId": None,
                        "tcgplayerEtchedProductId": None,
                        "tcgplayerAlternativeFoilProductId": None,
                        "mcmId": "99",
                        "mcmMetaId": "88",
                        "cardKingdomId": None,
                    },
                    "finishes": ["nonfoil", "foil"],
                },
            ],
        )

        result = _build_cardmarket_entries_from_parquet(ctx, parquet_dir)

        assert result is not None
        # Should have both nonfoil and foil entries
        assert len(result) == 2

        foil_urls = [u for u in result["referral_url"].to_list() if "&isFoil=Y" in u]
        assert len(foil_urls) == 1

    def test_returns_none_when_no_cards_lf(self, tmp_path):
        from mtgjson5.data.context import PipelineContext

        ctx = PipelineContext.for_testing(meld_triplets={}, manual_overrides={})
        parquet_dir = tmp_path / "parquet"
        parquet_dir.mkdir()

        result = _build_cardmarket_entries_from_parquet(ctx, parquet_dir)
        assert result is None

    def test_returns_none_when_no_parquet_dir(self):
        ctx = self._make_ctx(
            [
                {"id": "sf-001", "purchaseUris": {"cardmarket": "https://example.com"}},
            ]
        )
        result = _build_cardmarket_entries_from_parquet(ctx, None)
        assert result is None


# ---------------------------------------------------------------------------
# Sealed products
# ---------------------------------------------------------------------------


class TestBuildSealedEntries:
    """Tests for build_referral_map_from_sealed."""

    def test_produces_ck_sealed_entries(self):
        sealed_df = pl.DataFrame(
            [
                {
                    "uuid": "sealed-001",
                    "_ck_url": "catalog/sealed/product",
                    "identifiers": {"tcgplayerProductId": None},
                },
            ],
            schema={
                "uuid": pl.String,
                "_ck_url": pl.String,
                "identifiers": pl.Struct({"tcgplayerProductId": pl.String}),
            },
        )

        result = build_referral_map_from_sealed(sealed_df)

        assert len(result) == 1
        assert CK_REFERRAL in result["referral_url"][0]

    def test_produces_tcg_sealed_entries(self):
        sealed_df = pl.DataFrame(
            [
                {
                    "uuid": "sealed-001",
                    "_ck_url": None,
                    "identifiers": {"tcgplayerProductId": "77777"},
                },
            ],
            schema={
                "uuid": pl.String,
                "_ck_url": pl.String,
                "identifiers": pl.Struct({"tcgplayerProductId": pl.String}),
            },
        )

        result = build_referral_map_from_sealed(sealed_df)

        assert len(result) == 1
        assert "77777" in result["referral_url"][0]

    def test_produces_both_ck_and_tcg_entries(self):
        sealed_df = pl.DataFrame(
            [
                {
                    "uuid": "sealed-001",
                    "_ck_url": "catalog/sealed/box",
                    "identifiers": {"tcgplayerProductId": "88888"},
                },
            ],
            schema={
                "uuid": pl.String,
                "_ck_url": pl.String,
                "identifiers": pl.Struct({"tcgplayerProductId": pl.String}),
            },
        )

        result = build_referral_map_from_sealed(sealed_df)

        assert len(result) == 2

    def test_returns_empty_for_none_input(self):
        result = build_referral_map_from_sealed(None)
        assert len(result) == 0
        assert set(result.columns) == {"hash", "referral_url"}

    def test_returns_empty_for_empty_df(self):
        sealed_df = pl.DataFrame(
            schema={"uuid": pl.String, "_ck_url": pl.String},
        )
        result = build_referral_map_from_sealed(sealed_df)
        assert len(result) == 0


# ---------------------------------------------------------------------------
# Write referral map
# ---------------------------------------------------------------------------


class TestWriteReferralMap:
    """Tests for write_referral_map (Nginx map format)."""

    def test_writes_nginx_map_format(self, tmp_path):
        df = pl.DataFrame(
            {
                "hash": ["abc123", "def456"],
                "referral_url": ["https://example.com/a", "https://example.com/b"],
            }
        )

        write_referral_map(df, tmp_path)

        map_path = tmp_path / "ReferralMap.json"
        assert map_path.exists()

        lines = map_path.read_text().strip().split("\n")
        assert len(lines) == 2

        # Sorted by hash
        assert lines[0] == "/links/abc123\thttps://example.com/a;"
        assert lines[1] == "/links/def456\thttps://example.com/b;"

    def test_creates_output_directory(self, tmp_path):
        nested = tmp_path / "a" / "b" / "c"
        df = pl.DataFrame({"hash": ["h1"], "referral_url": ["url1"]})

        write_referral_map(df, nested)

        assert (nested / "ReferralMap.json").exists()

    def test_empty_dataframe_writes_empty_file(self, tmp_path):
        df = pl.DataFrame({"hash": [], "referral_url": []})
        write_referral_map(df, tmp_path)

        content = (tmp_path / "ReferralMap.json").read_text()
        assert content == ""


# ---------------------------------------------------------------------------
# Hash determinism
# ---------------------------------------------------------------------------


class TestHashDeterminism:
    """Hashes must be stable across runs — they become part of published URLs."""

    def test_ck_hash_is_deterministic(self, tmp_path, monkeypatch):

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        monkeypatch.setattr(constants, "CACHE_PATH", cache_dir)

        ck_url = "catalog/magic-the-gathering-singles/test-set/lightning-bolt-702"
        uuid = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"

        _write_ck_cache(
            cache_dir,
            [
                {
                    "id": "sf-det",
                    "cardKingdomId": "702",
                    "cardKingdomUrl": ck_url,
                    "cardKingdomFoilId": None,
                    "cardKingdomFoilUrl": None,
                    "cardKingdomEtchedId": None,
                    "cardKingdomEtchedUrl": None,
                },
            ],
        )

        parquet_dir = tmp_path / "parquet"
        _write_cards_parquet(
            parquet_dir,
            [
                {
                    "uuid": uuid,
                    "identifiers": {
                        "scryfallId": "sf-det",
                        "tcgplayerProductId": None,
                        "tcgplayerEtchedProductId": None,
                        "tcgplayerAlternativeFoilProductId": None,
                        "mcmId": None,
                        "mcmMetaId": None,
                        "cardKingdomId": "702",
                    },
                    "finishes": ["nonfoil"],
                },
            ],
        )

        result = _build_ck_entries_from_parquet(parquet_dir)
        assert result is not None

        expected = _sha256_16(CK_BASE + ck_url + uuid)
        assert result["hash"][0] == expected

    def test_tcg_hash_is_deterministic(self, tmp_path):
        tcg_id = "12345"
        uuid = "uuid-det-tcg"

        parquet_dir = tmp_path / "parquet"
        _write_cards_parquet(
            parquet_dir,
            [
                {
                    "uuid": uuid,
                    "identifiers": {
                        "scryfallId": "sf-001",
                        "tcgplayerProductId": tcg_id,
                        "tcgplayerEtchedProductId": None,
                        "tcgplayerAlternativeFoilProductId": None,
                        "mcmId": None,
                        "mcmMetaId": None,
                        "cardKingdomId": None,
                    },
                    "finishes": ["nonfoil"],
                },
            ],
        )

        result = _build_tcg_entries_from_parquet(parquet_dir)
        assert result is not None

        expected = _sha256_16(tcg_id + uuid)
        assert result["hash"][0] == expected

    def test_cm_hash_is_deterministic(self, tmp_path):
        from mtgjson5.data.context import PipelineContext

        mcm_id = "999"
        mcm_meta = "888"
        uuid = "uuid-det-cm"

        ctx = PipelineContext.for_testing(
            cards_lf=pl.LazyFrame(
                [{"id": "sf-cm", "purchaseUris": {"cardmarket": "https://example.com?referrer=scryfall"}}],
                schema={"id": pl.String, "purchaseUris": pl.Struct({"cardmarket": pl.String})},
            ),
            meld_triplets={},
            manual_overrides={},
        )

        parquet_dir = tmp_path / "parquet"
        _write_cards_parquet(
            parquet_dir,
            [
                {
                    "uuid": uuid,
                    "identifiers": {
                        "scryfallId": "sf-cm",
                        "tcgplayerProductId": None,
                        "tcgplayerEtchedProductId": None,
                        "tcgplayerAlternativeFoilProductId": None,
                        "mcmId": mcm_id,
                        "mcmMetaId": mcm_meta,
                        "cardKingdomId": None,
                    },
                    "finishes": ["nonfoil"],
                },
            ],
        )

        result = _build_cardmarket_entries_from_parquet(ctx, parquet_dir)
        assert result is not None

        expected = _sha256_16(mcm_id + uuid + constants.CARD_MARKET_BUFFER + mcm_meta)
        nonfoil_row = result.filter(~pl.col("referral_url").str.contains("isFoil"))
        assert len(nonfoil_row) == 1
        assert nonfoil_row["hash"][0] == expected


# ---------------------------------------------------------------------------
# build_and_write_referral_map (orchestrator)
# ---------------------------------------------------------------------------


class TestBuildAndWriteReferralMap:
    """Integration tests for the top-level orchestrator."""

    def test_writes_file_and_returns_count(self, tmp_path, monkeypatch):

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        monkeypatch.setattr(constants, "CACHE_PATH", cache_dir)

        _write_ck_cache(
            cache_dir,
            [
                {
                    "id": "sf-int",
                    "cardKingdomId": "500",
                    "cardKingdomUrl": "catalog/test",
                    "cardKingdomFoilId": None,
                    "cardKingdomFoilUrl": None,
                    "cardKingdomEtchedId": None,
                    "cardKingdomEtchedUrl": None,
                },
            ],
        )

        parquet_dir = tmp_path / "parquet"
        _write_cards_parquet(
            parquet_dir,
            [
                {
                    "uuid": "uuid-int",
                    "identifiers": {
                        "scryfallId": "sf-int",
                        "tcgplayerProductId": "42",
                        "tcgplayerEtchedProductId": None,
                        "tcgplayerAlternativeFoilProductId": None,
                        "mcmId": None,
                        "mcmMetaId": None,
                        "cardKingdomId": "500",
                    },
                    "finishes": ["nonfoil"],
                },
            ],
        )

        from mtgjson5.data.context import PipelineContext

        ctx = PipelineContext.for_testing(meld_triplets={}, manual_overrides={})
        output_dir = tmp_path / "output"

        count = build_and_write_referral_map(
            ctx=ctx,
            parquet_dir=parquet_dir,
            sealed_df=None,
            output_path=output_dir,
        )

        # CK (1) + TCG (1) = 2
        assert count == 2
        assert (output_dir / "ReferralMap.json").exists()

        lines = (output_dir / "ReferralMap.json").read_text().strip().split("\n")
        assert len(lines) == 2

    def test_returns_zero_with_no_data(self, tmp_path, monkeypatch):

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        monkeypatch.setattr(constants, "CACHE_PATH", cache_dir)

        from mtgjson5.data.context import PipelineContext

        ctx = PipelineContext.for_testing(meld_triplets={}, manual_overrides={})
        output_dir = tmp_path / "output"

        count = build_and_write_referral_map(
            ctx=ctx,
            parquet_dir=None,
            sealed_df=None,
            output_path=output_dir,
        )

        assert count == 0

    def test_includes_sealed_entries(self, tmp_path, monkeypatch):

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        monkeypatch.setattr(constants, "CACHE_PATH", cache_dir)

        from mtgjson5.data.context import PipelineContext

        ctx = PipelineContext.for_testing(meld_triplets={}, manual_overrides={})
        output_dir = tmp_path / "output"

        sealed_df = pl.DataFrame(
            [
                {
                    "uuid": "sealed-001",
                    "_ck_url": "catalog/sealed/box",
                    "identifiers": {"tcgplayerProductId": "99999"},
                },
            ],
            schema={
                "uuid": pl.String,
                "_ck_url": pl.String,
                "identifiers": pl.Struct({"tcgplayerProductId": pl.String}),
            },
        )

        count = build_and_write_referral_map(
            ctx=ctx,
            parquet_dir=None,
            sealed_df=sealed_df,
            output_path=output_dir,
        )

        # CK sealed (1) + TCG sealed (1) = 2
        assert count == 2

    def test_deduplicates_hashes(self, tmp_path, monkeypatch):
        """Duplicate hashes across sources should be deduplicated."""

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        monkeypatch.setattr(constants, "CACHE_PATH", cache_dir)

        from mtgjson5.data.context import PipelineContext

        ctx = PipelineContext.for_testing(meld_triplets={}, manual_overrides={})
        output_dir = tmp_path / "output"

        # Two sealed entries with different data → different hashes
        sealed_df = pl.DataFrame(
            [
                {"uuid": "s-001", "_ck_url": "cat/a", "identifiers": {"tcgplayerProductId": None}},
                {"uuid": "s-002", "_ck_url": "cat/b", "identifiers": {"tcgplayerProductId": None}},
            ],
            schema={
                "uuid": pl.String,
                "_ck_url": pl.String,
                "identifiers": pl.Struct({"tcgplayerProductId": pl.String}),
            },
        )

        count = build_and_write_referral_map(
            ctx=ctx,
            parquet_dir=None,
            sealed_df=sealed_df,
            output_path=output_dir,
        )

        assert count == 2

        lines = (output_dir / "ReferralMap.json").read_text().strip().split("\n")
        hashes = [line.split("\t")[0] for line in lines]
        assert len(hashes) == len(set(hashes)), "Duplicate hashes in output"


# ---------------------------------------------------------------------------
# Regression: CK works after release_pipeline_data()
# ---------------------------------------------------------------------------


class TestCkWorksAfterRelease:
    """Regression test for 636fbed: CK referrals must work even when
    ctx.identifiers_lf is None (after release_pipeline_data)."""

    def test_ck_entries_produced_without_identifiers_lf(self, tmp_path, monkeypatch):
        """Simulates the exact bug: identifiers_lf=None, but CK cache + parquet exist."""
        from mtgjson5.data.context import PipelineContext

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        monkeypatch.setattr(constants, "CACHE_PATH", cache_dir)

        _write_ck_cache(
            cache_dir,
            [
                {
                    "id": "sf-reg",
                    "cardKingdomId": "9999",
                    "cardKingdomUrl": "catalog/singles/test/card-9999",
                    "cardKingdomFoilId": None,
                    "cardKingdomFoilUrl": None,
                    "cardKingdomEtchedId": None,
                    "cardKingdomEtchedUrl": None,
                },
            ],
        )

        parquet_dir = tmp_path / "parquet"
        _write_cards_parquet(
            parquet_dir,
            [
                {
                    "uuid": "uuid-reg",
                    "identifiers": {
                        "scryfallId": "sf-reg",
                        "tcgplayerProductId": None,
                        "tcgplayerEtchedProductId": None,
                        "tcgplayerAlternativeFoilProductId": None,
                        "mcmId": None,
                        "mcmMetaId": None,
                        "cardKingdomId": "9999",
                    },
                    "finishes": ["nonfoil"],
                },
            ],
        )

        # Create ctx with identifiers_lf=None (simulating post-release state)
        ctx = PipelineContext.for_testing(meld_triplets={}, manual_overrides={})
        assert ctx.identifiers_lf is None

        output_dir = tmp_path / "output"
        count = build_and_write_referral_map(
            ctx=ctx,
            parquet_dir=parquet_dir,
            sealed_df=None,
            output_path=output_dir,
        )

        assert count > 0
        content = (output_dir / "ReferralMap.json").read_text()
        assert "cardkingdom.com" in content
