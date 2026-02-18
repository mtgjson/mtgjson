"""
Legalities and availability.

Builds structured legalities and availability fields.
"""

from __future__ import annotations

import polars as pl

from mtgjson5.data import PipelineContext


def add_legalities_struct(
    lf: pl.LazyFrame,
    ctx: PipelineContext,
) -> pl.LazyFrame:
    """
    Builds legalities struct from Scryfall's legalities column.
    """
    lf = lf.unnest("legalities")

    formats = (
        ctx.categoricals.legalities if ctx.categoricals is not None and ctx.categoricals.legalities is not None else []
    )

    if not formats:
        return lf.with_columns(pl.lit(None).alias("legalities"))

    struct_fields = []
    for fmt in formats:
        expr = (
            pl.when(pl.col(fmt).is_not_null() & (pl.col(fmt) != "not_legal") & (pl.col("setType") != "memorabilia"))
            .then(pl.col(fmt).str.to_titlecase())
            .otherwise(pl.lit(None))
            .alias(fmt)
        )
        struct_fields.append(expr)

    return lf.with_columns(pl.struct(struct_fields).alias("legalities")).drop(formats, strict=False)


def add_availability_struct(
    lf: pl.LazyFrame,
    ctx: PipelineContext,
) -> pl.LazyFrame:
    """
    Build availability list from games column.
    """
    schema = lf.collect_schema()

    if "games" not in schema.names():
        return lf.with_columns(pl.lit([]).cast(pl.List(pl.String)).alias("availability"))

    categoricals = ctx.categoricals
    platforms = categoricals.games if categoricals else []

    if not platforms:
        return lf.with_columns(pl.col("games").alias("availability"))

    games_dtype = schema["games"]

    if isinstance(games_dtype, pl.Struct):
        return lf.with_columns(
            pl.concat_list(
                [
                    pl.when(pl.col("games").struct.field(p).fill_null(False)).then(pl.lit(p)).otherwise(pl.lit(None))
                    for p in platforms
                ]
            )
            .list.drop_nulls()
            .list.sort()
            .alias("availability")
        )
    return lf.with_columns(pl.col("games").list.sort().alias("availability"))


def remap_availability_values(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Remap Scryfall game names to MTGJSON availability names.
    - astral -> shandalar (Microprose Shandalar game)
    - sega -> dreamcast (Sega Dreamcast game)
    """
    schema = lf.collect_schema()

    if "availability" not in schema.names():
        return lf

    return lf.with_columns(
        pl.col("availability").list.eval(pl.element().replace({"astral": "shandalar", "sega": "dreamcast"})).list.sort()
    )


def fix_availability_from_ids(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Add platforms to availability if their respective ID fields are present.
    """
    schema = lf.collect_schema()

    if "availability" not in schema.names():
        return lf

    exprs = []

    if "mtgoId" in schema.names():
        exprs.append(
            pl.when(pl.col("mtgoId").is_not_null() & ~pl.col("availability").list.contains("mtgo"))
            .then(pl.col("availability").list.concat(pl.lit(["mtgo"])))
            .otherwise(pl.col("availability"))
            .alias("availability")
        )

    if "arenaId" in schema.names():
        exprs.append(
            pl.when(pl.col("arenaId").is_not_null() & ~pl.col("availability").list.contains("arena"))
            .then(pl.col("availability").list.concat(pl.lit(["arena"])))
            .otherwise(pl.col("availability"))
            .alias("availability")
        )

    for expr in exprs:
        lf = lf.with_columns(expr)

    if exprs:
        lf = lf.with_columns(pl.col("availability").list.sort())

    return lf
