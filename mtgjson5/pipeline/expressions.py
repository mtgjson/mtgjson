"""
Vectorized Polars expressions for card data transformations.

Replaces map_elements UDFs with pure Polars expressions for
significant performance improvements.
"""

from __future__ import annotations

import polars as pl

# =============================================================================
# Finish Ordering
# =============================================================================

# Canonical finish order: nonfoil, foil, etched, signed, then alphabetical
_FINISH_ORDER_MAP: dict[str, int] = {
    "nonfoil": 0,
    "foil": 1,
    "etched": 2,
    "signed": 3,
}


def order_finishes_expr(col: str = "finishes") -> pl.Expr:
    """
    Order finishes in canonical order: nonfoil, foil, etched, signed, others.

    Vectorized replacement for _order_finishes map_elements.

    Args:
        col: Name of finishes list column

    Returns:
        Expression producing sorted finishes list
    """
    return (
        pl.col(col)
        .list.eval(
            # Create (finish, sort_key) pairs
            pl.struct(
                finish=pl.element(),
                order=pl.element().replace_strict(
                    _FINISH_ORDER_MAP,
                    default=99,
                    return_dtype=pl.Int8,
                ),
            )
            # Sort by order, then alphabetically for ties
            .sort_by(["order", "finish"])
            # Extract just the finish strings
            .struct.field("finish")
        )
    )


# =============================================================================
# Mana Cost Parsing
# =============================================================================


def extract_mana_symbols_expr(col: str | pl.Expr = "manaCost") -> pl.Expr:
    """
    Extract mana symbols from cost string like "{2}{W}{U}".

    Returns list of symbol contents: ["2", "W", "U"]

    Args:
        col: Column name or expression to extract symbols from
    """
    expr = pl.col(col) if isinstance(col, str) else col
    return expr.fill_null("").str.extract_all(r"\{([^}]+)\}")


def calculate_cmc_expr(col: str | pl.Expr = "manaCost") -> pl.Expr:
    """
    Calculate converted mana cost from mana cost string.

    Vectorized replacement for _calculate_cmc_from_mana_cost.

    Rules:
    - Numbers: their value ({2} = 2)
    - X/Y/Z: 0 (variable)
    - H prefix: 0.5 (half mana)
    - Hybrid {2/W}: first part (2)
    - Colors: 1 each

    Args:
        col: Name of mana cost column or expression

    Returns:
        Expression producing CMC as Float64
    """
    symbols = extract_mana_symbols_expr(col)

    return (
        symbols.list.eval(
            # For each symbol, calculate its CMC contribution
            pl.when(pl.element().str.contains(r"^[XYZ]$"))
            .then(pl.lit(0.0))
            # Numeric: parse as float
            .when(pl.element().str.contains(r"^\d+$"))
            .then(pl.element().cast(pl.Float64))
            # Half mana (HW, HR, etc.)
            .when(pl.element().str.starts_with("H"))
            .then(pl.lit(0.5))
            # Hybrid: take first part before /
            .when(pl.element().str.contains("/"))
            .then(
                pl.element()
                .str.split("/")
                .list.first()
                .pipe(
                    lambda e: pl.when(e.str.contains(r"^\d+$"))
                    .then(e.cast(pl.Float64))
                    .otherwise(pl.lit(1.0))
                )
            )
            # Single color symbol
            .otherwise(pl.lit(1.0))
        )
        .list.sum()
        .fill_null(0.0)
    )


def extract_colors_from_mana_expr(col: str | pl.Expr = "manaCost") -> pl.Expr:
    """
    Extract color letters from mana cost.

    Vectorized replacement for color extraction from mana cost.

    "{2}{W}{U}" -> ["U", "W"]
    "{2/W}{G}" -> ["G", "W"]

    Args:
        col: Name of mana cost column or expression

    Returns:
        Expression producing sorted list of color letters
    """
    expr = pl.col(col) if isinstance(col, str) else col
    return (
        expr.fill_null("")
        # Extract all WUBRG characters from the string
        .str.extract_all(r"[WUBRG]")
        .list.unique()
        .list.sort()
    )


# =============================================================================
# Keyword Filtering
# =============================================================================


def _filter_keywords_batch(struct_series: pl.Series) -> pl.Series:
    """
    Batch filter keywords for a series of structs.

    Each struct contains {"text": str, "_all_keywords": list[str]}.
    Returns list of keywords that appear in the text.

    This is more efficient than map_elements as it processes in batches.
    """
    results = []
    for row in struct_series:
        if row is None:
            results.append([])
            continue

        text = (row.get("text") or "").lower()
        keywords = row.get("_all_keywords") or []

        if not keywords or not text:
            results.append([])
            continue

        # Filter keywords that appear in text, sort case-insensitively
        filtered = sorted(
            (kw for kw in keywords if kw.lower() in text),
            key=str.lower,
        )
        results.append(filtered)

    return pl.Series(results, dtype=pl.List(pl.String))


def filter_keywords_expr(
    text_col: str = "text",
    keywords_col: str = "_all_keywords",
) -> pl.Expr:
    """
    Filter keywords to only those appearing in card text.

    Uses map_batches for efficient batch processing instead of per-row map_elements.

    Args:
        text_col: Name of oracle text column
        keywords_col: Name of keywords list column

    Returns:
        Expression producing filtered, sorted keywords list
    """
    return pl.struct([text_col, keywords_col]).map_batches(
        _filter_keywords_batch,
        return_dtype=pl.List(pl.String),
    )


def filter_keywords_join(
    lf: pl.LazyFrame,
    text_col: str = "text",
    keywords_col: str = "_all_keywords",
    output_col: str = "keywords",
) -> pl.LazyFrame:
    """
    Filter keywords using explode-join pattern (faster for large datasets).

    This avoids per-row processing by:
    1. Exploding keywords to one row per keyword
    2. Filtering rows where keyword appears in text
    3. Re-aggregating back to original row structure

    Args:
        lf: Input LazyFrame
        text_col: Name of oracle text column
        keywords_col: Name of keywords list column
        output_col: Name for output filtered keywords column

    Returns:
        LazyFrame with filtered keywords column added
    """
    # Add row ID for re-aggregation
    lf = lf.with_row_index("_kw_row_id")

    # Explode keywords and filter to those present in text
    keywords_filtered = (
        lf.select(["_kw_row_id", text_col, keywords_col])
        .explode(keywords_col)
        .filter(pl.col(keywords_col).is_not_null())
        .with_columns(
            pl.col(text_col).fill_null("").str.to_lowercase().alias("_text_lower"),
            pl.col(keywords_col).str.to_lowercase().alias("_kw_lower"),
        )
        # Filter to keywords present in text
        .filter(pl.col("_text_lower").str.contains(pl.col("_kw_lower")))
        # Re-aggregate by row, sorting keywords
        .group_by("_kw_row_id")
        .agg(pl.col(keywords_col).sort().alias(output_col))
    )

    # Join back and clean up
    return (
        lf.join(keywords_filtered, on="_kw_row_id", how="left")
        .with_columns(pl.col(output_col).fill_null([]))
        .drop("_kw_row_id")
    )


# =============================================================================
# ASCII Name Normalization
# =============================================================================

# Character replacement map for ASCII normalization
_ASCII_REPLACEMENTS: dict[str, str] = {
    "AE": "AE",
    "ae": "ae",
    "OE": "OE",
    "oe": "oe",
    "ss": "ss",
    "E": "E",
    "E": "E",
    "E": "E",
    "E": "E",
    "A": "A",
    "A": "A",
    "A": "A",
    "A": "A",
    "A": "A",
    "I": "I",
    "I": "I",
    "I": "I",
    "I": "I",
    "O": "O",
    "O": "O",
    "O": "O",
    "O": "O",
    "O": "O",
    "U": "U",
    "U": "U",
    "U": "U",
    "U": "U",
    "Y": "Y",
    "N": "N",
    "C": "C",
    "e": "e",
    "e": "e",
    "e": "e",
    "e": "e",
    "a": "a",
    "a": "a",
    "a": "a",
    "a": "a",
    "a": "a",
    "i": "i",
    "i": "i",
    "i": "i",
    "i": "i",
    "o": "o",
    "o": "o",
    "o": "o",
    "o": "o",
    "o": "o",
    "u": "u",
    "u": "u",
    "u": "u",
    "u": "u",
    "y": "y",
    "y": "y",
    "n": "n",
    "c": "c",
}


def ascii_name_expr(col: str | pl.Expr) -> pl.Expr:
    """
    Normalize card name to ASCII.

    Uses str.replace_many for efficient batch replacement.

    Args:
        col: Column name or expression

    Returns:
        Expression producing ASCII-normalized string
    """
    expr = pl.col(col) if isinstance(col, str) else col
    old_chars = list(_ASCII_REPLACEMENTS.keys())
    new_chars = list(_ASCII_REPLACEMENTS.values())
    return expr.str.replace_many(old_chars, new_chars)


__all__ = [
    "order_finishes_expr",
    "extract_mana_symbols_expr",
    "calculate_cmc_expr",
    "extract_colors_from_mana_expr",
    "filter_keywords_expr",
    "filter_keywords_join",
    "ascii_name_expr",
]
