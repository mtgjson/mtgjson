"""Tests for apply_scryfall_overrides pipeline stage."""

from __future__ import annotations

import polars as pl
import pytest

from mtgjson5.data.context import PipelineContext
from mtgjson5.pipeline.stages.derived import apply_scryfall_overrides


def _make_lf(rows: list[dict] | None = None) -> pl.LazyFrame:
    """Build a minimal LazyFrame with scryfallId and common override targets."""
    if rows is None:
        rows = [
            {
                "scryfallId": "aaa-111",
                "name": "Card A",
                "tcgplayerAlternativeFoilProductId": None,
            },
            {
                "scryfallId": "bbb-222",
                "name": "Card B",
                "tcgplayerAlternativeFoilProductId": None,
            },
            {
                "scryfallId": "ccc-333",
                "name": "Card C",
                "tcgplayerAlternativeFoilProductId": 999,
            },
        ]
    return pl.LazyFrame(rows)


class TestApplyScryfallOverrides:
    def test_no_overrides_returns_unchanged(self) -> None:
        lf = _make_lf()
        ctx = PipelineContext.for_testing(scryfall_overrides={})
        result = apply_scryfall_overrides(lf, ctx).collect()
        expected = lf.collect()
        assert result.equals(expected)

    def test_none_overrides_returns_unchanged(self) -> None:
        lf = _make_lf()
        ctx = PipelineContext.for_testing(scryfall_overrides=None)
        result = apply_scryfall_overrides(lf, ctx).collect()
        expected = lf.collect()
        assert result.equals(expected)

    def test_override_sets_null_field(self) -> None:
        """Override should fill a null field for matching scryfallId."""
        lf = _make_lf()
        ctx = PipelineContext.for_testing(
            scryfall_overrides={
                "aaa-111": {"tcgplayerAlternativeFoilProductId": 12345},
            }
        )
        result = apply_scryfall_overrides(lf, ctx).collect()

        row_a = result.filter(pl.col("scryfallId") == "aaa-111")
        assert row_a["tcgplayerAlternativeFoilProductId"].item() == 12345

        # Other rows unaffected
        row_b = result.filter(pl.col("scryfallId") == "bbb-222")
        assert row_b["tcgplayerAlternativeFoilProductId"].item() is None

    def test_override_replaces_existing_value(self) -> None:
        """Override should replace an existing non-null value."""
        lf = _make_lf()
        ctx = PipelineContext.for_testing(
            scryfall_overrides={
                "ccc-333": {"tcgplayerAlternativeFoilProductId": 77777},
            }
        )
        result = apply_scryfall_overrides(lf, ctx).collect()

        row_c = result.filter(pl.col("scryfallId") == "ccc-333")
        assert row_c["tcgplayerAlternativeFoilProductId"].item() == 77777

    def test_multiple_cards_overridden(self) -> None:
        """Multiple scryfallIds can be overridden for the same field."""
        lf = _make_lf()
        ctx = PipelineContext.for_testing(
            scryfall_overrides={
                "aaa-111": {"tcgplayerAlternativeFoilProductId": 100},
                "bbb-222": {"tcgplayerAlternativeFoilProductId": 200},
            }
        )
        result = apply_scryfall_overrides(lf, ctx).collect()

        row_a = result.filter(pl.col("scryfallId") == "aaa-111")
        assert row_a["tcgplayerAlternativeFoilProductId"].item() == 100

        row_b = result.filter(pl.col("scryfallId") == "bbb-222")
        assert row_b["tcgplayerAlternativeFoilProductId"].item() == 200

    def test_override_string_field(self) -> None:
        """Overrides should work for string fields."""
        lf = _make_lf()
        ctx = PipelineContext.for_testing(
            scryfall_overrides={
                "aaa-111": {"name": "Corrected Name"},
            }
        )
        result = apply_scryfall_overrides(lf, ctx).collect()

        row_a = result.filter(pl.col("scryfallId") == "aaa-111")
        assert row_a["name"].item() == "Corrected Name"

        row_b = result.filter(pl.col("scryfallId") == "bbb-222")
        assert row_b["name"].item() == "Card B"

    def test_nonexistent_scryfall_id_ignored(self) -> None:
        """Overrides for scryfallIds not in the data are silently skipped."""
        lf = _make_lf()
        ctx = PipelineContext.for_testing(
            scryfall_overrides={
                "zzz-999": {"tcgplayerAlternativeFoilProductId": 55555},
            }
        )
        result = apply_scryfall_overrides(lf, ctx).collect()
        expected = lf.collect()
        assert result.equals(expected)

    def test_nonexistent_field_skipped_with_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        """Fields not in the schema should be skipped and logged."""
        lf = _make_lf()
        ctx = PipelineContext.for_testing(
            scryfall_overrides={
                "aaa-111": {"nonExistentField": "value"},
            }
        )
        result = apply_scryfall_overrides(lf, ctx).collect()

        # Data unchanged
        expected = lf.collect()
        assert result.equals(expected)

        assert "nonExistentField" in caplog.text
        assert "not in schema" in caplog.text

    def test_dunder_keys_skipped(self) -> None:
        """Fields starting with __ (metadata comments) are ignored."""
        lf = _make_lf()
        ctx = PipelineContext.for_testing(
            scryfall_overrides={
                "aaa-111": {
                    "__comment": "This is a metadata note",
                    "tcgplayerAlternativeFoilProductId": 42,
                },
            }
        )
        result = apply_scryfall_overrides(lf, ctx).collect()

        row_a = result.filter(pl.col("scryfallId") == "aaa-111")
        assert row_a["tcgplayerAlternativeFoilProductId"].item() == 42
        assert "__comment" not in result.columns

    def test_multiple_fields_per_card(self) -> None:
        """A single scryfallId can have multiple fields overridden."""
        lf = _make_lf()
        ctx = PipelineContext.for_testing(
            scryfall_overrides={
                "aaa-111": {
                    "name": "New Name",
                    "tcgplayerAlternativeFoilProductId": 999,
                },
            }
        )
        result = apply_scryfall_overrides(lf, ctx).collect()

        row_a = result.filter(pl.col("scryfallId") == "aaa-111")
        assert row_a["name"].item() == "New Name"
        assert row_a["tcgplayerAlternativeFoilProductId"].item() == 999
