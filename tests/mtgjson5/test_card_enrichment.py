"""Tests for apply_card_enrichment and the shipped card_enrichment.json data."""

from __future__ import annotations

import polars as pl

from mtgjson5.data.cache import load_resource_json
from mtgjson5.data.context import PipelineContext
from mtgjson5.models.cards import CardSet
from mtgjson5.pipeline.stages.basic_fields import apply_card_enrichment


def _make_lf() -> pl.LazyFrame:
    return pl.LazyFrame(
        [
            {"setCode": "LTC", "number": "408z", "name": "Sol Ring", "promoTypes": ["serialized"]},
            {"setCode": "LTC", "number": "409z", "name": "Sol Ring", "promoTypes": ["serialized"]},
            {"setCode": "LTC", "number": "408", "name": "Sol Ring", "promoTypes": None},
            {"setCode": "WHO", "number": "408z", "name": "Other Card", "promoTypes": None},
            {"setCode": "FIN", "number": "551a", "name": "Traveling Chocobo", "promoTypes": ["serialized"]},
        ],
        schema={
            "setCode": pl.String,
            "number": pl.String,
            "name": pl.String,
            "promoTypes": pl.List(pl.String),
        },
    )


def _enrichment_ctx() -> PipelineContext:
    return PipelineContext.for_testing(
        card_enrichment={
            "ltc": {
                "408z": {"name": "Sol Ring", "enrichment": {"print_run_size": 300}},
                "409z": {"name": "Sol Ring", "enrichment": {"print_run_size": 700}},
            },
            "FIN": {
                "551a": {
                    "name": "Traveling Chocobo",
                    "enrichment": {"promo_types": ["neoninkyellow"], "print_run_size": 77},
                },
            },
        }
    )


def _print_run_sizes(df: pl.DataFrame) -> dict[tuple[str, str], int | None]:
    return {(r["setCode"], r["number"]): r["printRunSize"] for r in df.iter_rows(named=True)}


class TestPrintRunSize:
    def test_serialized_card_gets_its_own_size(self) -> None:
        result = apply_card_enrichment(_make_lf(), _enrichment_ctx()).collect()
        sizes = _print_run_sizes(result)

        assert sizes[("LTC", "408z")] == 300
        assert sizes[("LTC", "409z")] == 700

    def test_other_cards_get_no_size(self) -> None:
        result = apply_card_enrichment(_make_lf(), _enrichment_ctx()).collect()
        sizes = _print_run_sizes(result)

        # Same set with a different number, and the same number in a different set
        assert sizes[("LTC", "408")] is None
        assert sizes[("WHO", "408z")] is None

    def test_row_count_and_column_type_unchanged_by_join(self) -> None:
        lf = _make_lf()
        result = apply_card_enrichment(lf, _enrichment_ctx()).collect()

        assert result.height == lf.collect().height
        assert result.schema["printRunSize"] == pl.Int64
        assert not any(c.startswith("enrichment") for c in result.columns)

    def test_size_and_promo_types_on_the_same_entry(self) -> None:
        result = apply_card_enrichment(_make_lf(), _enrichment_ctx()).collect()
        row = result.filter(pl.col("setCode") == "FIN").row(0, named=True)

        assert row["printRunSize"] == 77
        assert sorted(row["promoTypes"]) == ["neoninkyellow", "serialized"]

    def test_entry_without_size_leaves_size_null(self) -> None:
        ctx = PipelineContext.for_testing(
            card_enrichment={"FIN": {"551a": {"name": "X", "enrichment": {"promo_types": ["neoninkyellow"]}}}}
        )
        result = apply_card_enrichment(_make_lf(), ctx).collect()

        assert result["printRunSize"].null_count() == result.height
        fin = result.filter(pl.col("setCode") == "FIN").row(0, named=True)
        assert sorted(fin["promoTypes"]) == ["neoninkyellow", "serialized"]

    def test_no_enrichment_data_adds_null_column(self) -> None:
        ctx = PipelineContext.for_testing(card_enrichment={})
        result = apply_card_enrichment(_make_lf(), ctx).collect()

        assert result.schema["printRunSize"] == pl.Int64
        assert result["printRunSize"].null_count() == result.height

    def test_model_field_uses_camel_case_alias(self) -> None:
        assert CardSet.model_fields["print_run_size"].alias == "printRunSize"


class TestShippedEnrichmentData:
    """Sanity checks on the real card_enrichment.json."""

    def test_print_run_sizes_are_positive_integers(self) -> None:
        data = load_resource_json("card_enrichment.json")
        assert isinstance(data, dict)

        sizes = {
            (set_code, number): entry["enrichment"]["print_run_size"]
            for set_code, cards in data.items()
            for number, entry in cards.items()
            if "print_run_size" in entry["enrichment"]
        }

        assert sizes
        for key, size in sizes.items():
            assert isinstance(size, int)
            assert not isinstance(size, bool), key
            assert size > 0, key

    def test_printings_with_different_run_sizes_are_keyed_separately(self) -> None:
        data = load_resource_json("card_enrichment.json")
        assert isinstance(data, dict)

        # Three serialized Sol Rings in LTC, each with its own run size
        ltc = {n: data["LTC"][n]["enrichment"]["print_run_size"] for n in ("408z", "409z", "410z")}
        assert ltc == {"408z": 300, "409z": 700, "410z": 900}
        # The unique One Ring (LTR 0) is a run of one
        assert data["LTR"]["0"]["enrichment"]["print_run_size"] == 1
        # Doctor Who counts up with the Doctor number
        assert data["WHO"]["552z"]["enrichment"]["print_run_size"] == 501
        assert data["WHO"]["564z"]["enrichment"]["print_run_size"] == 513
